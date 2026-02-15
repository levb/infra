package block

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// FrameCache stores each compressed frame as an individual file in a directory.
// Files are named by their C-space offset: frame-<hex_offset>.bin.
type FrameCache struct {
	dir        string
	closed     atomic.Bool
	sizeOnDisk atomic.Int64

	storage    storage.FrameGetter
	objectPath string

	fetchers *utils.WaitMap
}

// NewFrameCache creates a new per-frame file cache in the given directory.
func NewFrameCache(dirPath string, s storage.FrameGetter, objectPath string) (*FrameCache, error) {
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create frame cache dir: %w", err)
	}

	return &FrameCache{
		dir:        dirPath,
		storage:    s,
		objectPath: objectPath,
		fetchers:   utils.NewWaitMap(),
	}, nil
}

// framePath returns the file path for a frame at the given C-space offset.
func (fc *FrameCache) framePath(off int64) string {
	return filepath.Join(fc.dir, fmt.Sprintf("frame-%x.bin", off))
}

// GetOrFetch returns the path to a cached compressed frame file.
// If the frame is not cached, it fetches the compressed data from storage and writes it to disk atomically.
func (fc *FrameCache) GetOrFetch(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) (string, error) {
	if fc.closed.Load() {
		return "", NewErrCacheClosed(fc.dir)
	}

	path := fc.framePath(frameStarts.C)

	err := fc.fetchers.Wait(frameStarts.C, func() error {
		// Double-check if file already exists (e.g. from a previous run with same cache dir)
		if _, err := os.Stat(path); err == nil {
			return nil
		}

		// TODO PERFORMANCE: stream rangeRead response body directly into tmp file via
		// a GetFrameToWriter method, avoiding this large alloc.
		buf := make([]byte, frameSize.C)
		_, err := fc.storage.GetFrame(ctx, fc.objectPath, frameStarts.U, ft, false, buf)
		if err != nil {
			return fmt.Errorf("failed to fetch compressed frame at %#x: %w", frameStarts.C, err)
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

		fc.sizeOnDisk.Add(int64(frameSize.C))

		return nil
	})
	if err != nil {
		return "", err
	}

	return path, nil
}

// Close removes the cache directory and all frame files.
func (fc *FrameCache) Close() error {
	fc.closed.Store(true)

	return os.RemoveAll(fc.dir)
}

// FileSize returns the total on-disk size of all cached frame files.
func (fc *FrameCache) FileSize() (int64, error) {
	return fc.sizeOnDisk.Load(), nil
}
