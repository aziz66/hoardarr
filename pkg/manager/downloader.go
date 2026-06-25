package manager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	grab "github.com/cavaliergopher/grab/v3"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/pool"
)

type Downloader struct {
	manager      *Manager
	strmURL      string
	mountPath    string
	dest         string
	logger       zerolog.Logger
	maxDownloads int
	grabClient   *grab.Client
}

const (
	// maxDownloadAttempts bounds how many times a single file download is retried.
	// Debrid links expire (~48h); a large file on a slow HDD can outlive one, so on
	// failure we re-resolve the link (linkService repairs/re-resolves) and retry.
	maxDownloadAttempts = 3
	// globalMaxDownloads caps the TOTAL concurrent file downloads across ALL entries.
	// max_downloads bounds concurrency per entry, but bulk imports spawn one pool per
	// entry, so without a global cap N entries × max_downloads writers can flood the
	// disk. Tuned for a single spinning HDD: too many concurrent sequential writers
	// seek-thrash the head and collapse throughput, so keep this low (~3).
	globalMaxDownloads = 3
	// progressPersistInterval throttles how often an in-flight download's progress is
	// marshalled + appended to the queue store (the in-memory entry stays current for
	// the API; only disk persistence is rate-limited).
	progressPersistInterval = 3 * time.Second
	// downloadStallTimeout aborts a download attempt whose byte count has not advanced
	// for this long. The pooled HTTP client has no overall timeout (large files), so a
	// half-open debrid/CDN connection would otherwise hang a goroutine forever while
	// holding a globalDownloadSem slot — 8 such stalls would freeze ALL downloads. On
	// stall we cancel the attempt so the retry loop can re-resolve and try again.
	downloadStallTimeout = 90 * time.Second
)

// globalDownloadSem bounds total concurrent file downloads process-wide.
var globalDownloadSem = make(chan struct{}, globalMaxDownloads)

// downloadRetryBackoff returns an exponential backoff capped at 15s.
func downloadRetryBackoff(attempt int) time.Duration {
	d := time.Duration(1<<uint(attempt)) * time.Second
	if d > 15*time.Second {
		d = 15 * time.Second
	}
	return d
}

const (
	symlinkMountWaitTimeout     = 30 * time.Minute
	symlinkScanInitialInterval  = 100 * time.Millisecond
	symlinkScanMaxInterval      = 2 * time.Second
	symlinkReadyTimeout         = 2 * time.Minute
	symlinkReadyInitialInterval = 200 * time.Millisecond
	symlinkReadyMaxInterval     = 2 * time.Second
	symlinkLogEveryAttempts     = 10
	symlinkLogSampleSize        = 8
)

type downloadLogMeta struct {
	requestHost     string
	finalHost       string
	requestRange    string
	contentRange    string
	responseProto   string
	contentEncoding string
	statusCode      int
	transferMode    string
	parts           int
}

// NewDownloadManager creates a new strm manager
func NewDownloadManager(manager *Manager) *Downloader {
	cfg := config.Get()
	strmURL := cfg.AppURL
	if strmURL == "" {
		bindAddress := cfg.BindAddress
		if bindAddress == "" {
			bindAddress = "localhost"
		}

		strmURL = fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	}
	return &Downloader{
		manager:      manager,
		strmURL:      strmURL,
		mountPath:    cfg.Mount.MountPath,
		logger:       manager.logger.With().Str("component", "downloader").Logger(),
		dest:         cfg.DownloadFolder,
		maxDownloads: cfg.MaxDownloads,
		// One grab.Client reused across all downloads (it's safe for concurrent use);
		// avoids allocating a throwaway http.Client+Transport per file. The actual
		// transport/keepalive pooling comes from the shared streamClient.
		grabClient: &grab.Client{HTTPClient: manager.streamClient, BufferSize: 1 << 20},
	}
}

func (d *Downloader) download(torrent *storage.Entry) error {
	// Mark as in-flight up front so the queue scheduler skips this entry while
	// we're iterating seasons / creating symlinks (processSymlink only flips
	// this flag after its own directory scan, which is too late for the parent
	// of a multi-season torrent).
	torrent.IsDownloading = true
	_ = d.manager.queue.Update(torrent)

	var (
		isMultiSeason bool
		seasons       []SeasonInfo
	)
	// Multi-season fan-out splits a pack into per-season entries under SYNTHETIC hashes.
	// That only works for the symlink/mount model; in download-to-disk mode the *arr polls
	// the ORIGINAL grabbed hash, so splitting leaves that hash pointing at an empty folder
	// and import fails. Download the whole pack under the grabbed hash instead.
	if !torrent.SkipMultiSeason && torrent.Action != config.DownloadActionDownload {
		isMultiSeason, seasons = d.detectMultiSeason(torrent)
	}
	torrentMountPath := d.manager.GetTorrentMountPath(torrent)
	if isMultiSeason {
		seasonResults := convertToMultiSeason(torrent, seasons)
		for _, result := range seasonResults {
			if err := d.manager.queue.Add(result); err != nil {
				d.logger.Error().Err(err).Msgf("Failed to save season torrent")
				continue
			}
			if err := d.process(result, torrentMountPath); err != nil {
				d.markAsError(result, err)
			}
		}
		// Parent has been fanned out into season entries; mark it complete so
		// it leaves the downloading queue instead of getting re-processed.
		d.completeEntry(torrent)
		return nil
	}
	return d.process(torrent, torrentMountPath)
}

func (d *Downloader) process(entry *storage.Entry, mountPath string) error {
	switch entry.Action {
	case config.DownloadActionDownload:
		return d.processDownload(entry)
	case config.DownloadActionSymlink:
		return d.processSymlink(entry, mountPath)
	case config.DownloadActionStrm:
		return d.processStrm(entry)
	case config.DownloadActionNone:
		d.completeEntry(entry)
		// Remove entry from queue
		_ = d.manager.queue.Delete(entry.InfoHash, nil)
		return nil
	default:
		// This is a download-to-disk client; the FUSE mount that processSymlink/
		// processStrm depend on no longer exists. Any unknown/empty action (e.g. a
		// queued entry persisted from a prior symlink-mode run, or config drift)
		// must download to disk, not block 30 min waiting for mount files.
		return d.processDownload(entry)
	}
}

func (d *Downloader) completeEntry(entry *storage.Entry) {
	d.markAsCompleted(entry)
	d.notifyCompleted(entry)
	d.triggerArrRefresh(entry)
}

func (d *Downloader) markAsCompleted(entry *storage.Entry) {
	// Mark as completed
	entry.MarkAsCompleted(entry.DownloadPath())
	_ = d.manager.queue.Update(entry)
}

func (d *Downloader) notifyCompleted(entry *storage.Entry) {
	// Send notification
	msg := fmt.Sprintf("Download completed: %s [%s] -> %s", entry.Name, entry.Category, entry.DownloadPath())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadComplete,
		Status:  "success",
		Entry:   entry,
		Message: msg,
	})
}

func (d *Downloader) triggerArrRefresh(entry *storage.Entry) {
	go func() {
		a := d.manager.arr.GetOrCreate(entry.Category)
		if a == nil || a.Host == "" || a.Token == "" {
			return
		}
		if err := a.Refresh(); err != nil {
			d.logger.Debug().
				Err(err).
				Str("arr", a.Name).
				Str("entry", entry.Name).
				Msg("Failed to trigger Arr refresh")
		}
	}()
}

func (d *Downloader) markAsError(entry *storage.Entry, err error) {
	d.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to process action")
	entry.MarkAsError(err)
	_ = d.manager.queue.Update(entry)

	// Send error notification
	msg := fmt.Sprintf("Download failed: %s [%s] - %s", entry.Name, entry.Category, err.Error())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadFailed,
		Status:  "error",
		Entry:   entry,
		Message: msg,
		Error:   err,
	})
}

// processSymlink creates symlinks for torrent files
func (d *Downloader) processSymlink(entry *storage.Entry, mountPath string) error {
	files := entry.GetActiveFiles()
	torrentSymlinkPath := entry.DownloadPath()
	d.logger.Info().Str("mount_path", mountPath).Msgf("Creating symlinks for %d files in %s", len(files), torrentSymlinkPath)

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	filePaths, err := d.createSymlinksWhenMountFilesAppear(entry, files, mountPath, torrentSymlinkPath)
	if err != nil {
		return err
	}

	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	if err := d.waitForSymlinkFilesReady(filePaths, symlinkReadyTimeout); err != nil {
		return err
	}

	// Run ffprobe on files to warm cache and trigger imports. Skipped for usenet
	// entries: their files don't probe cleanly over the mount (ffprobe seeks the
	// header/footer, which resolves awkwardly via usenet/requestdl) and it's only
	// for Decypharr's own media-info — the *arr does its own analysis.
	if !d.manager.config.SkipPreCache && !entry.IsNZB() && len(filePaths) > 0 {
		probeFiles := filePaths
		if len(probeFiles) > MaxNZBPreCacheFiles {
			probeFiles = probeFiles[:MaxNZBPreCacheFiles]
		}
		d.logger.Debug().Int("files", len(probeFiles)).Msgf("Running ffprobe on %s", entry.Name)
		if err := d.manager.RunFFprobe(probeFiles); err != nil {
			d.logger.Error().Msgf("Failed to run ffprobe: %s", err)
		} else {
			d.logger.Debug().Str("entry", entry.Name).Msgf("Ran ffprobe on %d/%d files", len(probeFiles), len(filePaths))
		}
	}

	d.completeEntry(entry)

	return nil
}

func (d *Downloader) createSymlinksWhenMountFilesAppear(entry *storage.Entry, files []*storage.File, mountPath string, symlinkDir string) ([]string, error) {
	remainingFiles := make(map[string]*storage.File, len(files))
	for _, file := range files {
		remainingFiles[file.Name] = file
	}

	filePaths := make([]string, 0, len(remainingFiles))
	deadline := time.Now().Add(symlinkMountWaitTimeout)
	delay := symlinkScanInitialInterval
	attempt := 0
	var lastScanErr error
	var scanErr error

	var checkDirectory func(string) error
	checkDirectory = func(dirPath string) error {
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			if scanErr == nil {
				scanErr = err
			}
			return nil
		}

		for _, item := range entries {
			entryName := item.Name()
			fullPath := filepath.Join(dirPath, entryName)

			if file, exists := remainingFiles[entryName]; exists {
				fileSymlinkPath := filepath.Join(symlinkDir, file.Name)
				if err := os.Symlink(fullPath, fileSymlinkPath); err != nil && !os.IsExist(err) {
					return fmt.Errorf("failed to create symlink %s -> %s: %w", fileSymlinkPath, fullPath, err)
				}
				filePaths = append(filePaths, fileSymlinkPath)
				delete(remainingFiles, entryName)
				d.logger.Info().Msgf("File is ready: %s/%s", entry.GetFolder(), file.Name)
				continue
			}

			if item.IsDir() {
				if err := checkDirectory(fullPath); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for len(remainingFiles) > 0 {
		attempt++
		scanErr = nil
		if err := checkDirectory(mountPath); err != nil {
			return nil, err
		}
		lastScanErr = scanErr
		if len(remainingFiles) == 0 {
			break
		}

		if time.Now().After(deadline) {
			pending := pendingMountFileNames(remainingFiles, symlinkLogSampleSize)
			if lastScanErr != nil {
				return nil, fmt.Errorf("timeout waiting for mount files: %d files still pending (%s): last scan error: %w", len(remainingFiles), strings.Join(pending, ", "), lastScanErr)
			}
			return nil, fmt.Errorf("timeout waiting for mount files: %d files still pending (%s)", len(remainingFiles), strings.Join(pending, ", "))
		}

		if shouldLogSymlinkWaitAttempt(attempt) {
			d.logger.Debug().
				Err(lastScanErr).
				Str("entry", entry.Name).
				Str("mount_path", mountPath).
				Int("pending", len(remainingFiles)).
				Strs("sample", pendingMountFileNames(remainingFiles, symlinkLogSampleSize)).
				Msg("Waiting for mount files before creating symlinks")
		}

		if err := d.sleepUntilNextSymlinkAttempt(delay, deadline); err != nil {
			return nil, err
		}
		delay = nextSymlinkBackoff(delay, symlinkScanMaxInterval)
	}

	return filePaths, nil
}

func (d *Downloader) waitForSymlinkFilesReady(filePaths []string, timeout time.Duration) error {
	if len(filePaths) == 0 {
		return nil
	}

	pending := make(map[string]error, len(filePaths))
	for _, path := range filePaths {
		pending[path] = nil
	}

	deadline := time.Now().Add(timeout)
	delay := symlinkReadyInitialInterval
	attempt := 0

	for len(pending) > 0 {
		attempt++
		for path := range pending {
			if err := verifySymlinkFileReady(path); err != nil {
				pending[path] = err
				continue
			}
			delete(pending, path)
		}
		if len(pending) == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for symlink files to be ready: %d files still pending (%s)", len(pending), strings.Join(pendingSymlinkFileStatuses(pending, symlinkLogSampleSize), ", "))
		}

		if shouldLogSymlinkWaitAttempt(attempt) {
			d.logger.Debug().
				Int("pending", len(pending)).
				Strs("sample", pendingSymlinkFileStatuses(pending, symlinkLogSampleSize)).
				Msg("Waiting for symlink files to resolve")
		}

		if err := d.sleepUntilNextSymlinkAttempt(delay, deadline); err != nil {
			return err
		}
		delay = nextSymlinkBackoff(delay, symlinkReadyMaxInterval)
	}

	return nil
}

func verifySymlinkFileReady(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("symlink not available: %w", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("path is not a symlink")
	}

	targetInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("symlink target not available: %w", err)
	}
	if targetInfo.IsDir() {
		return fmt.Errorf("symlink target is a directory")
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("symlink target cannot be opened: %w", err)
	}
	return f.Close()
}

func (d *Downloader) sleepUntilNextSymlinkAttempt(delay time.Duration, deadline time.Time) error {
	if remaining := time.Until(deadline); remaining < delay {
		delay = remaining
	}
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	ctx := d.operationContext()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (d *Downloader) operationContext() context.Context {
	if d.manager != nil && d.manager.ctx != nil {
		return d.manager.ctx
	}
	return context.Background()
}

func nextSymlinkBackoff(current time.Duration, maxDelay time.Duration) time.Duration {
	current *= 2
	if current > maxDelay {
		return maxDelay
	}
	return current
}

func shouldLogSymlinkWaitAttempt(attempt int) bool {
	return attempt == 1 || attempt%symlinkLogEveryAttempts == 0
}

func pendingMountFileNames(files map[string]*storage.File, limit int) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return limitedStringSample(names, limit)
}

func pendingSymlinkFileStatuses(files map[string]error, limit int) []string {
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	statuses := make([]string, 0, len(paths))
	for _, path := range paths {
		err := files[path]
		status := path
		if err != nil {
			status = fmt.Sprintf("%s: %s", path, err.Error())
		}
		statuses = append(statuses, status)
	}
	return limitedStringSample(statuses, limit)
}

func limitedStringSample(values []string, limit int) []string {
	if limit <= 0 || len(values) <= limit {
		return values
	}

	sample := append([]string(nil), values[:limit]...)
	sample = append(sample, fmt.Sprintf("... %d more", len(values)-limit))
	return sample
}

// processDownload downloads all files for an entry with progress tracking
// For torrents: uses HTTP download from debrid
// For NZBs: uses parallel NNTP segment download
func (d *Downloader) processDownload(entry *storage.Entry) error {
	// Native NNTP entries (ActiveProvider == "usenet") need the NNTP engine.
	// Debrid-usenet (e.g. TorBox: ActiveProvider == "torbox") resolves to an
	// HTTPS link via linkService just like torrents, so download it over HTTP.
	if entry.IsNZB() && entry.ActiveProvider == usenetNativeProvider {
		return d.processUsenetDownload(entry)
	}
	return d.processTorrentDownload(entry)
}

// processTorrentDownload downloads files from debrid via HTTP
func (d *Downloader) processTorrentDownload(entry *storage.Entry) error {
	files := entry.GetActiveFiles()
	d.logger.Info().Msgf("Downloading %d files...", len(files))

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}
	downloadedFolder := entry.DownloadPath()
	if err := os.MkdirAll(downloadedFolder, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}
	entry.SizeDownloaded = 0
	entry.IsDownloading = true
	entry.Progress = 0

	var progressMu sync.Mutex
	// Track per-file absolute bytes-on-disk so progress stays correct across retries
	// and grab resumes: localDownloader reports the CUMULATIVE bytes for a file, and we
	// add only the delta vs that file's previous value. Without this, a resumed retry
	// re-adds the already-counted prefix and Progress can exceed 1.0.
	fileProgress := make(map[string]int64)
	var lastPersist time.Time
	progressFor := func(name string) func(int64, int64) {
		return func(absolute int64, speed int64) {
			progressMu.Lock()
			defer progressMu.Unlock()

			prev := fileProgress[name]
			fileProgress[name] = absolute
			entry.SizeDownloaded += absolute - prev
			entry.Speed = speed
			if totalSize > 0 {
				entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
			}
			entry.UpdatedAt = time.Now()
			// Throttle persistence: the in-memory entry stays live, but we only marshal
			// + append the whole entry to the queue store at most every few seconds. At
			// 500ms ticks × N concurrent files this otherwise re-serializes the entire
			// entry ~6×/s and inflates the append-log (forcing frequent compaction).
			if time.Since(lastPersist) >= progressPersistInterval {
				lastPersist = time.Now()
				_ = d.manager.queue.Update(entry)
			}
		}
	}

	ctx := d.operationContext()
	// Resolve download links before spawning goroutines
	type downloadTask struct {
		file *storage.File
		link string
	}
	var tasks []downloadTask
	for _, file := range files {
		downloadLink, err := d.manager.linkService.GetLink(ctx, entry, file.Key())
		if err != nil {
			d.logger.Error().Msgf("Failed to get download link for %s: %v", file.Key(), err)
			continue
		}
		tasks = append(tasks, downloadTask{file: file, link: downloadLink.DownloadLink})
	}

	// If no valid download links were obtained, return error instead of panic
	if len(tasks) == 0 {
		return fmt.Errorf("no valid download links available for %s", entry.Name)
	}

	p := pool.New().WithErrors().WithFirstError()
	if d.maxDownloads > 0 {
		p = p.WithMaxGoroutines(d.maxDownloads)
	}
	for _, task := range tasks {
		p.Go(func() error {
			// Preserve subfolders (e.g. CD1/01.flac) while preventing traversal, and
			// create the per-file parent dirs (MkdirAll above only made the root folder).
			rel := task.file.Path
			if rel == "" {
				rel = task.file.Name
			}
			destPath := utils.SafeJoin(downloadedFolder, rel)
			if err := os.MkdirAll(filepath.Dir(destPath), os.ModePerm); err != nil {
				return fmt.Errorf("failed to create directory for %s: %w", task.file.Key(), err)
			}
			if err := d.downloadFileWithRetry(
				entry,
				task.file,
				task.link,
				destPath,
				progressFor(task.file.Key()),
			); err != nil {
				d.logger.Error().Msgf("Failed to download %s: %v", task.file.Key(), err)
				return err
			}
			d.logger.Info().Msgf("Downloaded %s", task.file.Key())
			return nil
		})
	}

	if err := p.Wait(); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	d.completeEntry(entry)
	d.logger.Info().Msgf("Downloaded all files for %s", entry.Name)
	return nil
}

// downloadFileWithRetry downloads one file to destPath, bounding global concurrency
// via globalDownloadSem and re-resolving the debrid link on failure (links expire,
// and a large file on a slow HDD can outlive one). grab resumes full-file downloads
// from the partial; byte-range members re-fetch (small, acceptable).
func (d *Downloader) downloadFileWithRetry(entry *storage.Entry, file *storage.File, initialLink, destPath string, progressCallback func(int64, int64)) error {
	ctx := d.operationContext()

	// Acquire a global download slot, honoring cancellation so shutdown/restart can't
	// block on a full+stalled semaphore.
	select {
	case globalDownloadSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-globalDownloadSem }()

	link := initialLink
	var lastErr error
	for attempt := 0; attempt < maxDownloadAttempts; attempt++ {
		if attempt > 0 {
			// Re-resolve the link before retrying. linkService + the account cache now
			// treat past-ExpiresAt links as invalid and regenerate them, so this picks
			// up a fresh link for the expiry case (and repairs/re-validates otherwise).
			//
			// Resolve against a snapshot copy, not the shared live entry: the link
			// service's repair path (markEntryBad -> AddOrUpdate -> EntryToProto) marshals
			// the entry's fields, which would race the sibling files' progress callbacks
			// mutating SizeDownloaded/Progress/Speed/Bad on the same pointer.
			entrySnapshot := *entry
			dl, err := d.manager.linkService.GetLink(ctx, &entrySnapshot, file.Key())
			if err != nil {
				lastErr = err
				d.logger.Warn().Msgf("re-resolve link failed for %s (attempt %d/%d): %v", file.Name, attempt+1, maxDownloadAttempts, err)
			} else {
				link = dl.DownloadLink
			}
		}

		err := d.localDownloader(ctx, link, destPath, file.ByteRange, progressCallback)
		if err == nil {
			return nil
		}
		lastErr = err
		d.logger.Warn().Msgf("download attempt %d/%d failed for %s: %v", attempt+1, maxDownloadAttempts, file.Name, err)

		// No point retrying into a full disk — fail fast.
		if errors.Is(err, syscall.ENOSPC) {
			break
		}

		if attempt < maxDownloadAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(downloadRetryBackoff(attempt)):
			}
		}
	}
	// Permanent failure: remove the partial so it doesn't waste space on a full volume
	// and can't be mistaken for a complete file (size-only resume) on a later re-queue.
	if rmErr := os.Remove(destPath); rmErr != nil && !os.IsNotExist(rmErr) {
		d.logger.Debug().Msgf("could not remove partial %s: %v", destPath, rmErr)
	}
	return lastErr
}

// processUsenetDownload downloads NZB files via parallel NNTP segment fetching
func (d *Downloader) processUsenetDownload(entry *storage.Entry) error {
	if d.manager.usenet == nil {
		return fmt.Errorf("usenet client not configured")
	}

	files := entry.GetActiveFiles()
	d.logger.Info().Msgf("Downloading %d NZB files via usenet...", len(files))

	downloadedFolder := entry.DownloadPath()
	if err := os.MkdirAll(downloadedFolder, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}

	entry.SizeDownloaded = 0
	entry.Progress = 0
	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	var progressMu sync.Mutex
	// Track per-file progress so we can compute the global total across all files
	fileProgress := make(map[string]int64)
	var lastPersist time.Time

	ctx := d.operationContext()
	p := pool.New().WithErrors().WithFirstError()
	if d.maxDownloads > 0 {
		p = p.WithMaxGoroutines(d.maxDownloads)
	}
	for _, file := range files {
		p.Go(func() error {
			// Bound total concurrent downloads process-wide (shared with HTTP path),
			// honoring cancellation so shutdown can't block on a full semaphore.
			select {
			case globalDownloadSem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			defer func() { <-globalDownloadSem }()

			// filepath.Base neutralizes any "../" in an untrusted file name so a member
			// can't escape the entry's download folder.
			destPath := filepath.Join(downloadedFolder, filepath.Base(file.Name))
			destFile, err := os.Create(destPath)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", file.Name, err)
			}
			defer destFile.Close()

			progressCallback := func(downloaded int64, speed int64) {
				progressMu.Lock()
				defer progressMu.Unlock()

				prev := fileProgress[file.Name]
				fileProgress[file.Name] = downloaded
				entry.SizeDownloaded += downloaded - prev
				entry.Speed = speed
				if totalSize > 0 {
					entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
				}
				entry.UpdatedAt = time.Now()
				// Throttle persistence (see processTorrentDownload).
				if time.Since(lastPersist) >= progressPersistInterval {
					lastPersist = time.Now()
					_ = d.manager.queue.Update(entry)
				}
			}

			if err := d.manager.usenet.Download(ctx, entry.InfoHash, file.Name, destFile, progressCallback); err != nil {
				_ = os.Remove(destPath)
				return fmt.Errorf("failed to download %s: %w", file.Name, err)
			}

			d.logger.Info().Msgf("Downloaded NZB file: %s", file.Name)
			return nil
		})
	}

	err := p.Wait()

	if err != nil {
		// markAsError (with state reset + notification) is applied uniformly by the
		// caller (processAction) for both torrent and usenet download failures, so we
		// only surface the error here.
		return fmt.Errorf("NZB download failed: %w", err)
	}

	d.completeEntry(entry)
	d.logger.Info().Msgf("Downloaded all NZB files for %s", entry.Name)
	return nil
}

// processStrm creates symlinks for torrent files
func (d *Downloader) processStrm(torrent *storage.Entry) error {
	files := torrent.GetActiveFiles()
	d.logger.Info().Msgf("Creating .strm for %d files ...", len(files))

	torrentSymlinkPath := torrent.DownloadPath()

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	for _, file := range files {
		strmFilePath := filepath.Join(torrentSymlinkPath, file.Name+".strm")
		streamURL, err := url.JoinPath(
			d.strmURL,
			"webdav",
			"stream",
			EntryAllFolder,
			url.PathEscape(torrent.GetFolder()),
			url.PathEscape(file.Name),
		)
		if err != nil {
			continue
		}
		if err := os.WriteFile(strmFilePath, []byte(streamURL), 0644); err != nil {
			return fmt.Errorf("failed to create .strm file: %s: %v", strmFilePath, err)
		}
	}
	d.completeEntry(torrent)
	d.logger.Info().Str("destination", torrentSymlinkPath).Msgf("Created .strm files for %s", torrent.Name)
	return nil
}

func (d *Downloader) detectMultiSeason(torrent *storage.Entry) (bool, []SeasonInfo) {
	torrentName := torrent.Name
	files := torrent.GetActiveFiles()

	// Find all seasons present in the files
	seasonsFound := findAllSeasons(files)

	// Check if this is actually a multi-season torrent
	isMultiSeason := len(seasonsFound) > 1 || hasMultiSeasonIndicators(torrentName)

	if !isMultiSeason {
		return false, nil
	}

	d.logger.Info().Msgf("Multi-season torrent detected with seasons: %v", getSortedSeasons(seasonsFound))

	// Group files by season
	seasonGroups := groupFilesBySeason(files, seasonsFound)

	// Create SeasonInfo objects with proper naming
	var seasons []SeasonInfo
	for seasonNum, seasonFiles := range seasonGroups {
		if len(seasonFiles) == 0 {
			continue
		}

		// Generate season-specific name preserving all metadata
		seasonName := replaceMultiSeasonPattern(torrentName, seasonNum)

		seasons = append(seasons, SeasonInfo{
			SeasonNumber: seasonNum,
			Files:        seasonFiles,
			InfoHash:     generateSeasonHash(torrent.InfoHash, seasonNum),
			Name:         seasonName,
		})
	}

	return true, seasons
}

// redactURL masks the debrid API token in a download URL so it never leaks into error
// messages, logs, or the *arr's blocklist UI (the requestdl link carries ?token=<key>).
func redactURL(rawurl string) string {
	u, err := url.Parse(rawurl)
	if err != nil {
		return rawurl
	}
	q := u.Query()
	if q.Get("token") == "" {
		return rawurl
	}
	q.Set("token", "REDACTED")
	u.RawQuery = q.Encode()
	return u.String()
}

// localDownloader downloads a file with grab so interrupted local downloads can resume
// cleanly. progressCallback receives the CUMULATIVE bytes-on-disk for this file (so
// callers stay correct across retries/resumes). A per-attempt context aborts the
// transfer if it stalls (no byte progress for downloadStallTimeout), since the shared
// HTTP client has no overall timeout.
func (d *Downloader) localDownloader(ctx context.Context, downloadURL, filename string, byterange *[2]int64, progressCallback func(int64, int64)) error {
	startTime := time.Now()
	requestedRange := "full"

	// Per-attempt context so a stall watchdog can cancel just this transfer without
	// tearing down the whole manager.
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req, err := grab.NewRequest(filename, downloadURL)
	if err != nil {
		return err
	}
	req = req.WithContext(attemptCtx)
	req.BufferSize = 1 << 20
	req.HTTPRequest.Header.Set("User-Agent", "Decypharr[QBitTorrent]")
	req.HTTPRequest.Header.Set("Accept", "*/*")
	req.HTTPRequest.Header.Set("Accept-Encoding", "identity")

	if byterange != nil {
		requestedRange = fmt.Sprintf("bytes=%d-%d", byterange[0], byterange[1])
		req.NoResume = true
		req.HTTPRequest.Header.Set("Range", requestedRange)
	}

	resp := d.grabClient.Do(req)
	if resp == nil {
		return fmt.Errorf("grab returned nil response for %s", redactURL(downloadURL))
	}

	var lastReported int64
	lastAdvance := time.Now()
	stalled := false
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	defer func() {
		var downloaded atomic.Int64
		downloaded.Store(resp.BytesComplete())
		meta := d.buildDownloadLogMeta(req.HTTPRequest, resp.HTTPResponse, requestedRange, "grab", 1)
		d.logDownloadCompletion(filename, startTime, &downloaded, meta)
	}()

	for {
		select {
		case <-t.C:
			current := resp.BytesComplete()
			speed := int64(resp.BytesPerSecond())
			if current != lastReported {
				lastReported = current
				lastAdvance = time.Now()
				if progressCallback != nil {
					progressCallback(current, speed)
				}
			} else if time.Since(lastAdvance) > downloadStallTimeout {
				// No progress for too long — likely a half-open connection. Cancel the
				// attempt so the retry loop can re-resolve the link and try again.
				stalled = true
				cancel()
			}
		case <-resp.Done:
			if progressCallback != nil {
				final := resp.BytesComplete()
				if final != lastReported {
					progressCallback(final, int64(resp.BytesPerSecond()))
				}
			}
			if err := resp.Err(); err != nil {
				if stalled {
					return fmt.Errorf("download stalled (no progress for %s) for %s", downloadStallTimeout, redactURL(downloadURL))
				}
				if grab.IsStatusCodeError(err) && resp.HTTPResponse != nil {
					return fmt.Errorf("unexpected status %d for %s", resp.HTTPResponse.StatusCode, redactURL(downloadURL))
				}
				return err
			}
			return nil
		}
	}
}

func (d *Downloader) buildDownloadLogMeta(req *http.Request, resp *http.Response, requestedRange, transferMode string, parts int) downloadLogMeta {
	meta := downloadLogMeta{
		requestHost:     req.URL.Host,
		requestRange:    requestedRange,
		contentRange:    "none",
		contentEncoding: "identity",
		responseProto:   "unknown",
		statusCode:      0,
		transferMode:    transferMode,
		parts:           parts,
	}

	if resp == nil {
		return meta
	}

	if resp.Request != nil && resp.Request.URL != nil {
		meta.finalHost = resp.Request.URL.Host
	}
	meta.responseProto = resp.Proto
	if resp.TLS != nil && resp.TLS.NegotiatedProtocol != "" {
		meta.responseProto = fmt.Sprintf("%s (alpn=%s)", resp.Proto, resp.TLS.NegotiatedProtocol)
	}
	if contentRange := resp.Header.Get("Content-Range"); contentRange != "" {
		meta.contentRange = contentRange
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" {
		meta.contentEncoding = encoding
	}
	meta.statusCode = resp.StatusCode
	return meta
}

func (d *Downloader) logDownloadCompletion(filename string, startTime time.Time, downloaded *atomic.Int64, meta downloadLogMeta) {
	bytesDownloaded := downloaded.Load()
	elapsed := time.Since(startTime)
	speedMBps := float64(0)
	if elapsed > 0 {
		speedMBps = float64(bytesDownloaded) / elapsed.Seconds() / (1024 * 1024)
	}

	d.logger.Info().
		Str("file", filepath.Base(filename)).
		Str("request_host", meta.requestHost).
		Str("final_host", meta.finalHost).
		Str("request_range", meta.requestRange).
		Str("content_range", meta.contentRange).
		Str("response_proto", meta.responseProto).
		Str("content_encoding", meta.contentEncoding).
		Str("transfer_mode", meta.transferMode).
		Int("parts", meta.parts).
		Int64("status", int64(meta.statusCode)).
		Int64("bytes", bytesDownloaded).
		Dur("duration", elapsed).
		Float64("speed_mbps", speedMBps).
		Msg("download transfer completed")
}
