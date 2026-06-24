package vfs

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
)

// StreamingFile is the FUSE file interface for VFS
type StreamingFile struct {
	item     *CacheItem
	fileSize int64
	closed   atomic.Bool
}

// NewStreamingFile creates a new streaming file handle
func NewStreamingFile(item *CacheItem) *StreamingFile {
	item.Open() // Increment opens count

	return &StreamingFile{
		item:     item,
		fileSize: item.info.Size,
	}
}

// ReadAt implements io.ReaderAt using a background context.
// Prefer ReadAtContext when a caller context is available (e.g. from a FUSE handle).
func (f *StreamingFile) ReadAt(p []byte, off int64) (int, error) {
	return f.ReadAtContext(context.Background(), p, off)
}

// ReadAtContext reads from the file, passing ctx into the download layer so
// the operation can be interrupted by a read timeout or client disconnect.
func (f *StreamingFile) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if f.closed.Load() {
		return 0, errors.New("file closed")
	}

	if off >= f.fileSize {
		return 0, io.EOF
	}

	// Clamp read size
	readSize := int64(len(p))
	if off+readSize > f.fileSize {
		readSize = f.fileSize - off
		p = p[:readSize]
	}

	n, err := f.item.ReadAtContext(ctx, p, off)

	// Handle partial read at EOF
	if n < int(readSize) && err == nil {
		err = io.EOF
	}

	return n, err
}

// footerPrefetchSem globally bounds concurrent background footer prefetches.
// A library scan opens hundreds of files near-simultaneously; without a bound,
// each open spawns its own 10MB footer-download stream, and the resulting
// fan-out livelocks the download layer in lock contention (many per-item
// kickers + stall watchdogs spinning with no real I/O — observed pegging the
// CPU and hanging the mount after a bulk import + Plex rescan). The acquire is
// non-blocking (try-acquire): normal playback opens a handful of files and
// still warms their footers, while a scan burst simply skips prefetch on the
// overflow — the file is fully readable either way, it just isn't pre-warmed.
var footerPrefetchSem = make(chan struct{}, 4)

// Prefetch warms a byte range into the cache in the background (best-effort, non
// blocking). Used on open to pre-fetch the file footer so a media player seeking
// to the end (FLAC seektable, MP4 moov, MKV cues) hits cache instead of stalling
// on a fresh debrid range request. It takes its own open reference so the cache
// item is not evicted mid-prefetch even if the handle closes quickly.
func (f *StreamingFile) Prefetch(off, size int64) {
	if f.closed.Load() || size <= 0 {
		return
	}
	// Non-blocking acquire: cap concurrent prefetches so an open-storm (scan)
	// can't fan out into hundreds of footer streams. Skip if the budget is full.
	select {
	case footerPrefetchSem <- struct{}{}:
	default:
		return
	}
	item := f.item
	item.Open() // hold a ref for the duration of the prefetch
	go func() {
		defer func() { <-footerPrefetchSem }()
		defer item.Release()
		_ = item.Prefetch(context.Background(), off, size)
	}()
}

// Size returns the file size
func (f *StreamingFile) Size() int64 {
	return f.fileSize
}

// Close closes the file handle
func (f *StreamingFile) Close() error {
	if f.closed.Swap(true) {
		return nil
	}
	f.item.Release() // Decrement opens count

	return nil
}
