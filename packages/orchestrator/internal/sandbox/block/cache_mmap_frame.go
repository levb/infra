package block

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// FrameFileCache stores each compressed frame as an individual file in a directory.
// Files are named by their C-space offset: frame-<hex_offset>.bin.
type FrameFileCache struct {
	dir    string
	closed atomic.Bool

	fetchers *utils.WaitMap
}

// NewFrameFileCache creates a new per-frame file cache in the given directory.
func NewFrameFileCache(dirPath string) (*FrameFileCache, error) {
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create frame cache dir: %w", err)
	}

	return &FrameFileCache{
		dir:      dirPath,
		fetchers: utils.NewWaitMap(),
	}, nil
}

// framePath returns the file path for a frame at the given C-space offset.
func (fc *FrameFileCache) framePath(off int64) string {
	return filepath.Join(fc.dir, fmt.Sprintf("frame-%x.bin", off))
}

// GetOrFetch returns the path to a cached compressed frame file.
// If the frame is not cached, fetchFn is called with a writable buffer of `length` bytes
// to populate the frame data, which is then written to disk atomically.
func (fc *FrameFileCache) GetOrFetch(off, length int64, fetchFn func(buf []byte) error) (string, error) {
	if fc.closed.Load() {
		return "", NewErrCacheClosed(fc.dir)
	}

	path := fc.framePath(off)

	err := fc.fetchers.Wait(off, func() error {
		// Double-check if file already exists (e.g. from a previous run with same cache dir)
		if _, err := os.Stat(path); err == nil {
			return nil
		}

		buf := make([]byte, length)
		if err := fetchFn(buf); err != nil {
			return err
		}

		// Write atomically: tmp file + rename
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, buf, 0o644); err != nil {
			return fmt.Errorf("failed to write frame file %s: %w", tmp, err)
		}

		if err := os.Rename(tmp, path); err != nil {
			_ = os.Remove(tmp)

			return fmt.Errorf("failed to rename frame file %s -> %s: %w", tmp, path, err)
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	return path, nil
}

// Close removes the cache directory and all frame files.
func (fc *FrameFileCache) Close() error {
	fc.closed.Store(true)

	return os.RemoveAll(fc.dir)
}

// FileSize returns the total on-disk size of all cached frame files.
func (fc *FrameFileCache) FileSize() (int64, error) {
	entries, err := os.ReadDir(fc.dir)
	if err != nil {
		return 0, fmt.Errorf("failed to read frame cache dir: %w", err)
	}

	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}

	return total, nil
}
