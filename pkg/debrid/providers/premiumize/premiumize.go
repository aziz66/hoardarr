package premiumize

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/version"
	"go.uber.org/ratelimit"
)

// Premiumize implements debrid/common.Client for premiumize.me.
//
// Premiumize's model differs from Real-Debrid/TorBox: a torrent is added as an
// async "transfer" (POST /transfer/create), polled via /transfer/list, and once
// finished its files live in a cloud folder (folder_id) or as a single item
// (file_id). Each file already carries a direct CDN "link", so GetDownloadLink
// mostly returns that link rather than performing a separate unrestrict step.
type Premiumize struct {
	Host                  string
	APIKey                string
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	client                *request.Client
	logger                zerolog.Logger
	Profile               *types.Profile
	config                config.Debrid
}

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*Premiumize, error) {
	cfg := config.Get()
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	} else {
		headers["User-Agent"] = fmt.Sprintf("Decypharr/%s (%s; %s)", version.GetInfo(), runtime.GOOS, runtime.GOARCH)
	}
	_log := logger.New(dc.Name)

	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithRateLimiter(ratelimits["main"]),
		request.WithMaxRetries(cfg.Retries),
		request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway),
	}
	if dc.Proxy != "" {
		opts = append(opts, request.WithProxy(dc.Proxy))
	}

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}

	p := &Premiumize{
		Host:                  "https://www.premiumize.me/api",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		config:                dc,
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                request.New(opts...),
		logger:                _log,
	}
	return p, nil
}

func (p *Premiumize) Config() config.Debrid            { return p.config }
func (p *Premiumize) Logger() zerolog.Logger           { return p.logger }
func (p *Premiumize) SupportsCheck() bool              { return true }
func (p *Premiumize) AccountManager() *account.Manager { return p.accountsManager }

// ── HTTP helpers ─────────────────────────────────────────────────────────────

func (p *Premiumize) doGet(endpoint string, query map[string]string, result interface{}) (*http.Response, error) {
	values := url.Values{}
	for k, v := range query {
		values.Set(k, v)
	}
	return p.doGetValues(endpoint, values, result)
}

func (p *Premiumize) doGetValues(endpoint string, values url.Values, result interface{}) (*http.Response, error) {
	u, err := url.Parse(p.Host + endpoint)
	if err != nil {
		return nil, err
	}
	if len(values) > 0 {
		u.RawQuery = values.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(result); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func (p *Premiumize) doPostForm(endpoint string, form url.Values, result interface{}) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, p.Host+endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(result); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

// ── Status mapping ───────────────────────────────────────────────────────────

func premiumizeStatus(status string) types.TorrentStatus {
	switch status {
	case "finished", "seeding":
		return types.TorrentStatusDownloaded
	case "waiting", "queued", "running":
		return types.TorrentStatusDownloading
	default: // deleted, banned, error, timeout
		return types.TorrentStatusError
	}
}

// ── Availability ─────────────────────────────────────────────────────────────

func (p *Premiumize) IsAvailable(hashes []string) map[string]bool {
	result := make(map[string]bool)

	for i := 0; i < len(hashes); i += 100 {
		end := i + 100
		if end > len(hashes) {
			end = len(hashes)
		}

		batch := make([]string, 0, end-i)
		for _, h := range hashes[i:end] {
			if h != "" {
				batch = append(batch, h)
			}
		}
		if len(batch) == 0 {
			continue
		}

		values := url.Values{}
		for _, h := range batch {
			values.Add("items[]", h)
		}

		var data cacheCheckResponse
		resp, err := p.doGetValues("/cache/check", values, &data)
		if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.ok() {
			continue
		}
		for idx, h := range batch {
			if idx < len(data.Response) && data.Response[idx] {
				result[strings.ToUpper(h)] = true
			}
		}
	}
	return result
}

// ── Add / status / delete ────────────────────────────────────────────────────

func (p *Premiumize) SubmitMagnet(t *types.Torrent) (*types.Torrent, error) {
	src := ""
	if t.Magnet != nil {
		src = t.Magnet.Link
		if src == "" && t.Magnet.InfoHash != "" {
			src = "magnet:?xt=urn:btih:" + t.Magnet.InfoHash
		}
	}
	if src == "" {
		return nil, fmt.Errorf("premiumize: no magnet link or infohash to submit")
	}

	// Premiumize has no "add only if cached" flag — emulate it with a cache check.
	if !t.DownloadUncached && t.Magnet != nil && t.Magnet.InfoHash != "" {
		avail := p.IsAvailable([]string{t.Magnet.InfoHash})
		if !avail[strings.ToUpper(t.Magnet.InfoHash)] {
			return nil, fmt.Errorf("torrent: %s not cached", t.Magnet.Name)
		}
	}

	form := url.Values{}
	form.Set("src", src)

	var data createResponse
	resp, err := p.doPostForm("/transfer/create", form, &data)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.ok() {
		return nil, fmt.Errorf("premiumize API error: %s (status %d)", data.Message, resp.StatusCode)
	}
	if data.Id == "" {
		return nil, fmt.Errorf("premiumize: empty transfer id on create")
	}

	t.Id = data.Id
	t.Debrid = p.config.Name
	t.Added = time.Now()
	return t, nil
}

func (p *Premiumize) getTransfer(id string) (*transfer, error) {
	var data transferListResponse
	resp, err := p.doGet("/transfer/list", nil, &data)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.ok() {
		return nil, fmt.Errorf("premiumize API error: %s (status %d)", data.Message, resp.StatusCode)
	}
	for i := range data.Transfers {
		if data.Transfers[i].Id == id {
			return &data.Transfers[i], nil
		}
	}
	return nil, customerror.TorrentNotFoundError
}

// walkFolder recursively collects file items under a Premiumize cloud folder.
func (p *Premiumize) walkFolder(folderId, prefix string, files map[string]types.File, torrentId string, cfg *config.Config) error {
	var data folderListResponse
	resp, err := p.doGet("/folder/list", map[string]string{"id": folderId}, &data)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.ok() {
		return fmt.Errorf("premiumize API error: %s (status %d)", data.Message, resp.StatusCode)
	}
	for _, it := range data.Content {
		if it.Type == "folder" {
			if err := p.walkFolder(it.Id, path.Join(prefix, it.Name), files, torrentId, cfg); err != nil {
				return err
			}
			continue
		}
		relPath := path.Join(prefix, it.Name)
		if cfg.IsFileAllowed(relPath, it.Size) != nil {
			continue
		}
		fileName := filepath.Base(it.Name)
		link := it.Link
		if link == "" {
			link = "premiumize://" + it.Id
		}
		files[fileName] = types.File{
			TorrentId: torrentId,
			Id:        it.Id,
			Name:      fileName,
			Size:      it.Size,
			Path:      relPath,
			Link:      link,
		}
	}
	return nil
}

// collectFiles enumerates a finished transfer's files. Returns the file map plus
// the top-level original filename (folder or single-file name).
func (p *Premiumize) collectFiles(torrentId, folderId, fileId string) (map[string]types.File, string, error) {
	files := make(map[string]types.File)
	cfg := config.Get()

	switch {
	case folderId != "":
		if err := p.walkFolder(folderId, "", files, torrentId, cfg); err != nil {
			return nil, "", err
		}
	case fileId != "":
		var data itemDetailsResponse
		resp, err := p.doGet("/item/details", map[string]string{"id": fileId}, &data)
		if err != nil {
			return nil, "", err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.ok() {
			return nil, "", fmt.Errorf("premiumize API error: %s (status %d)", data.Message, resp.StatusCode)
		}
		if cfg.IsFileAllowed(data.Name, data.Size) == nil {
			fileName := filepath.Base(data.Name)
			link := data.Link
			if link == "" {
				link = "premiumize://" + data.Id
			}
			files[fileName] = types.File{
				TorrentId: torrentId,
				Id:        data.Id,
				Name:      fileName,
				Size:      data.Size,
				Path:      data.Name,
				Link:      link,
			}
		}
	}

	var original string
	for _, f := range files {
		original = strings.Split(path.Clean(f.Path), "/")[0]
		break
	}
	return files, original, nil
}

func (p *Premiumize) UpdateTorrent(t *types.Torrent) error {
	tr, err := p.getTransfer(t.Id)
	if err != nil {
		return err
	}

	t.Name = tr.Name
	t.Filename = tr.Name
	t.OriginalFilename = tr.Name
	t.Progress = tr.Progress * 100
	t.Status = premiumizeStatus(tr.Status)
	t.Debrid = p.config.Name
	t.Files = make(map[string]types.File)

	if t.Status == types.TorrentStatusDownloaded {
		files, original, err := p.collectFiles(t.Id, tr.FolderId, tr.FileId)
		if err != nil {
			return err
		}
		t.Files = files
		if original != "" {
			t.OriginalFilename = original
		}
	}
	return nil
}

func (p *Premiumize) GetTorrent(torrentId string) (*types.Torrent, error) {
	t := &types.Torrent{
		Id:     torrentId,
		Debrid: p.config.Name,
		Files:  make(map[string]types.File),
	}
	if err := p.UpdateTorrent(t); err != nil {
		return nil, err
	}
	return t, nil
}

func (p *Premiumize) CheckStatus(t *types.Torrent) (*types.Torrent, error) {
	if err := p.UpdateTorrent(t); err != nil {
		return t, err
	}
	switch t.Status {
	case types.TorrentStatusDownloaded:
		p.logger.Info().Msgf("Torrent: %s downloaded", t.Name)
		return t, nil
	case types.TorrentStatusDownloading:
		if !t.DownloadUncached {
			return t, fmt.Errorf("torrent: %s not cached", t.Name)
		}
		return t, nil
	default:
		return t, fmt.Errorf("torrent: %s has error", t.Name)
	}
}

func (p *Premiumize) DeleteTorrent(torrentId string) error {
	form := url.Values{}
	form.Set("id", torrentId)

	var data baseResponse
	resp, err := p.doPostForm("/transfer/delete", form, &data)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("premiumize API error: status %d", resp.StatusCode)
	}
	p.logger.Info().Msgf("Transfer %s deleted from Premiumize", torrentId)
	return nil
}

func (p *Premiumize) GetTorrents() ([]*types.Torrent, error) {
	var data transferListResponse
	resp, err := p.doGet("/transfer/list", nil, &data)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.ok() {
		return nil, fmt.Errorf("premiumize API error: %s (status %d)", data.Message, resp.StatusCode)
	}

	torrents := make([]*types.Torrent, 0, len(data.Transfers))
	for _, tr := range data.Transfers {
		t := &types.Torrent{
			Id:               tr.Id,
			Name:             tr.Name,
			Filename:         tr.Name,
			OriginalFilename: tr.Name,
			Progress:         tr.Progress * 100,
			Status:           premiumizeStatus(tr.Status),
			Debrid:           p.config.Name,
			Files:            make(map[string]types.File),
			Added:            time.Now(),
		}
		if t.Status == types.TorrentStatusDownloaded {
			if files, original, err := p.collectFiles(tr.Id, tr.FolderId, tr.FileId); err == nil {
				t.Files = files
				if original != "" {
					t.OriginalFilename = original
				}
			}
		}
		torrents = append(torrents, t)
	}
	return torrents, nil
}

// ── Download links ───────────────────────────────────────────────────────────

func (p *Premiumize) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return p.accountsManager.GetDownloadLink(id, file, p.fetchDownloadLink)
}

func (p *Premiumize) fetchDownloadLink(acc *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	empty := types.DownloadLink{}
	downloadURL := file.Link

	// folder/list usually already gave us a direct CDN link. If we only have the
	// internal "premiumize://<itemid>" placeholder, resolve a fresh link.
	if downloadURL == "" || strings.HasPrefix(downloadURL, "premiumize://") {
		if file.Id == "" {
			return empty, customerror.HosterUnavailableError
		}
		var data itemDetailsResponse
		resp, err := p.doGet("/item/details", map[string]string{"id": file.Id}, &data)
		if err != nil {
			return empty, err
		}
		if resp.StatusCode == http.StatusNotFound {
			return empty, customerror.HosterUnavailableError
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.ok() || data.Link == "" {
			return empty, customerror.HosterUnavailableError
		}
		downloadURL = data.Link
	}

	now := time.Now()
	dl := types.DownloadLink{
		Debrid:       p.config.Name,
		Token:        acc.Token,
		Filename:     file.Name,
		Size:         file.Size,
		Link:         file.Link, // cache key — must match file.Link
		DownloadLink: downloadURL,
		Id:           file.Id,
		Generated:    now,
		ExpiresAt:    now.Add(p.autoExpiresLinksAfter),
	}
	return dl, nil
}

func (p *Premiumize) fetchDownloadLinks(acc *account.Account) ([]types.DownloadLink, error) {
	return []types.DownloadLink{}, nil
}

func (p *Premiumize) RefreshDownloadLinks() error {
	return p.accountsManager.RefreshLinks(p.fetchDownloadLinks)
}

func (p *Premiumize) deleteDownloadLink(acc *account.Account, dl types.DownloadLink) error {
	return nil
}

func (p *Premiumize) DeleteLink(dl types.DownloadLink) error {
	return p.accountsManager.DeleteDownloadLink(dl, p.deleteDownloadLink)
}

// ── Repair ───────────────────────────────────────────────────────────────────

func (p *Premiumize) CheckFile(ctx context.Context, infohash, link string) error {
	if infohash == "" {
		return nil
	}
	avail := p.IsAvailable([]string{infohash})
	if !avail[strings.ToUpper(infohash)] {
		return customerror.HosterUnavailableError
	}
	return nil
}

// ── Account / profile ────────────────────────────────────────────────────────

func (p *Premiumize) GetProfile() (*types.Profile, error) {
	if p.Profile != nil {
		return p.Profile, nil
	}
	var data accountInfoResponse
	resp, err := p.doGet("/account/info", nil, &data)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !data.ok() {
		return nil, fmt.Errorf("premiumize API error: %s (status %d)", data.Message, resp.StatusCode)
	}

	username := ""
	if data.CustomerId != nil {
		username = fmt.Sprintf("%v", data.CustomerId)
	}
	profile := &types.Profile{
		Name:     p.config.Name,
		Username: username,
		Type:     "premium",
	}
	if data.PremiumUntil > 0 {
		profile.Expiration = time.Unix(data.PremiumUntil, 0)
	}
	p.Profile = profile
	return profile, nil
}

// Premiumize doesn't expose a concurrent-transfer slot count, so report a sane
// default. Tune via the debrid's minimum_free_slot config if needed.
func (p *Premiumize) GetAvailableSlots() (int, error) {
	return 5, nil
}

func (p *Premiumize) syncAccount(acc *account.Account) error {
	return nil
}

func (p *Premiumize) SyncAccounts() {
	p.accountsManager.Sync(p.syncAccount)
}

// ── Speed test ───────────────────────────────────────────────────────────────

func (p *Premiumize) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider: p.config.Name,
		TestedAt: time.Now(),
	}

	start := time.Now()
	resp, err := p.doGet("/account/info", nil, nil)
	latency := time.Since(start)
	if err != nil {
		result.Error = fmt.Sprintf("latency test failed: %v", err)
		return result
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = fmt.Sprintf("latency test unexpected status: %d", resp.StatusCode)
		return result
	}
	result.LatencyMs = latency.Milliseconds()

	current := p.accountsManager.Current()
	if current == nil {
		return result
	}
	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result
	}

	const downloadSize = 1 * 1024 * 1024 // 1MB
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.DownloadLink, nil)
	if err != nil {
		return result
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", downloadSize-1))

	downloadStart := time.Now()
	dlResp, err := current.Client().Do(req)
	if err != nil {
		return result
	}
	defer dlResp.Body.Close()

	bodyData, err := io.ReadAll(dlResp.Body)
	downloadDuration := time.Since(downloadStart)
	if err != nil || len(bodyData) == 0 {
		return result
	}

	result.BytesRead = int64(len(bodyData))
	if downloadDuration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / downloadDuration.Seconds() / (1024 * 1024)
	}
	return result
}
