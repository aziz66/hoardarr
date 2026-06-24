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

// Prefetch warms a byte range into the cache in the background (best-effort, non
// blocking). Used on open to pre-fetch the file footer so a media player seeking
// to the end (FLAC seektable, MP4 moov, MKV cues) hits cache instead of stalling
// on a fresh debrid range request. It takes its own open reference so the cache
// item is not evicted mid-prefetch even if the handle closes quickly.
func (f *StreamingFile) Prefetch(off, size int64) {
	if f.closed.Load() || size <= 0 {
		return
	}
	item := f.item
	item.Open() // hold a ref for the duration of the prefetch
	go func() {
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
