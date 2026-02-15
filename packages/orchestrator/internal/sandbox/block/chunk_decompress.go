package block

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// DecompressMMapChunker fetches compressed frames from storage, decompresses them
// immediately, and stores the UNCOMPRESSED data in a memory-mapped cache file.
// This is essentially the same as UncompressedMMapChunker but handles compressed
// source data. Both use Cache for block-aligned dirty tracking since the cached
// data is always uncompressed and block-aligned.
type DecompressMMapChunker struct {
	storage    storage.FrameGetter
	objectPath string

	cache   *Cache
	metrics metrics.Metrics

	size    int64 // uncompressed size
	rawSize int64 // C space size (compressed on storage)

	fetchMap *utils.WaitMap
}

// NewDecompressMMapChunker creates a chunker that decompresses data into an mmap cache.
// size is the uncompressed size, rawSize is the on-storage (possibly compressed) size.
func NewDecompressMMapChunker(
	size, rawSize, blockSize int64,
	s storage.FrameGetter,
	objectPath string,
	cachePath string,
	metrics metrics.Metrics,
) (*DecompressMMapChunker, error) {
	// mmap holds decompressed (uncompressed) data
	cache, err := NewCache(size, blockSize, cachePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	return &DecompressMMapChunker{
		size:       size,
		rawSize:    rawSize,
		storage:    s,
		objectPath: objectPath,
		cache:      cache,
		metrics:    metrics,
		fetchMap:   utils.NewWaitMap(),
	}, nil
}

// Slice reads data at the given uncompressed offset.
func (c *DecompressMMapChunker) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, fmt.Errorf("invalid slice params: off=%d length=%d", off, length)
	}
	if off+length > c.size {
		return nil, fmt.Errorf("slice out of bounds: off=%#x length=%d size=%d", off, length, c.size)
	}

	timer := c.metrics.SlicesTimerFactory.Begin()

	b, err := c.cache.Slice(off, length)
	if err == nil {
		timer.Success(ctx, length, attribute.String(pullType, pullTypeLocal))

		return b, nil
	}

	if !errors.As(err, &BytesNotAvailableError{}) {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, failureTypeLocalRead))

		return nil, fmt.Errorf("failed read from cache at offset %d: %w", off, err)
	}

	chunkErr := c.fetchToCache(ctx, off, length, ft)
	if chunkErr != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeRemote),
			attribute.String(failureReason, failureTypeCacheFetch))

		return nil, fmt.Errorf("failed to ensure data at %d-%d: %w", off, off+length, chunkErr)
	}

	b, cacheErr := c.cache.Slice(off, length)
	if cacheErr != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, failureTypeLocalReadAgain))

		return nil, fmt.Errorf("failed to read from cache after ensuring data at %d-%d: %w", off, off+length, cacheErr)
	}

	timer.Success(ctx, length, attribute.String(pullType, pullTypeRemote))

	return b, nil
}

// fetchToCache fetches data and stores into mmap.
// When ft is non-nil, fetches compressed frames and decompresses.
// When ft is nil, fetches uncompressed data directly.
func (c *DecompressMMapChunker) fetchToCache(ctx context.Context, off, length int64, ft *storage.FrameTable) error {
	if ft == nil {
		return c.fetchUncompressedToCache(ctx, off, length)
	}

	return c.fetchDecompressToCache(ctx, off, length, ft)
}

// fetchDecompressToCache fetches a single compressed frame containing off and decompresses into mmap.
// The requested range must not cross a frame boundary.
func (c *DecompressMMapChunker) fetchDecompressToCache(ctx context.Context, off, length int64, ft *storage.FrameTable) error {
	frameStarts, frameSize, err := ft.FrameFor(off)
	if err != nil {
		return fmt.Errorf("failed to get frame for offset %#x: %w", off, err)
	}

	if off+length > frameStarts.U+int64(frameSize.U) {
		return fmt.Errorf("read spans frame boundary: off=%#x length=%d frameStart=%#x frameSize=%d",
			off, length, frameStarts.U, frameSize.U)
	}

	return c.fetchMap.Wait(frameStarts.U, func() error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("error fetching frame at %#x: %w", frameStarts.U, ctx.Err())
		default:
		}

		b, releaseCacheCloseLock, err := c.cache.addressBytes(frameStarts.U, int64(frameSize.U))
		if err != nil {
			return err
		}
		defer releaseCacheCloseLock()

		_, err = c.storage.GetFrame(ctx, c.objectPath, frameStarts.U, ft, true, b)
		if err != nil {
			return fmt.Errorf("failed to read frame at %#x: %w", frameStarts.U, err)
		}

		c.cache.setIsCached(frameStarts.U, int64(frameSize.U))

		return nil
	})
}

// fetchUncompressedToCache fetches uncompressed data directly from storage.
func (c *DecompressMMapChunker) fetchUncompressedToCache(ctx context.Context, off, length int64) error {
	var eg errgroup.Group

	chunks := header.BlocksOffsets(length, storage.MemoryChunkSize)
	startingChunk := header.BlockIdx(off, storage.MemoryChunkSize)
	startingChunkOffset := header.BlockOffset(startingChunk, storage.MemoryChunkSize)

	for _, chunkOff := range chunks {
		fetchOff := startingChunkOffset + chunkOff

		eg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.L().Error(ctx, "recovered from panic in the fetch handler", zap.Any("error", r))
					err = fmt.Errorf("recovered from panic in the fetch handler: %v", r)
				}
			}()

			err = c.fetchMap.Wait(fetchOff, func() error {
				select {
				case <-ctx.Done():
					return fmt.Errorf("error fetching range %d-%d: %w", fetchOff, fetchOff+storage.MemoryChunkSize, ctx.Err())
				default:
				}

				b, releaseCacheCloseLock, err := c.cache.addressBytes(fetchOff, storage.MemoryChunkSize)
				if err != nil {
					return err
				}
				defer releaseCacheCloseLock()

				_, err = c.storage.GetFrame(ctx, c.objectPath, fetchOff, nil, false, b)
				if err != nil {
					return fmt.Errorf("failed to read uncompressed data at %d: %w", fetchOff, err)
				}

				c.cache.setIsCached(fetchOff, int64(len(b)))

				return nil
			})

			return err
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to fetch uncompressed data at %d-%d: %w", off, off+length, err)
	}

	return nil
}

func (c *DecompressMMapChunker) Close() error {
	return c.cache.Close()
}

func (c *DecompressMMapChunker) FileSize() (int64, error) {
	return c.cache.FileSize()
}
