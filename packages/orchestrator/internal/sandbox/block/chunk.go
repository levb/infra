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

// DataSource is an interface for reading block data from either local cache or remote storage.
// Both Chunker (mmap-based) and CompressedChunker (LRU + compressed cache) implement this interface.
type DataSource interface {
	ReadAt(ctx context.Context, b []byte, off int64) (int, error)
	Slice(ctx context.Context, off, length int64) ([]byte, error)
	Close() error
	FileSize() (int64, error)
}

// Verify that both chunker types implement DataSource.
var (
	_ DataSource = (*Chunker)(nil)
	_ DataSource = (*CompressedChunker)(nil)
)

type Chunker struct {
	storage    storage.FrameGetter
	objectPath string
	frameTable *storage.FrameTable

	cache   *Cache
	metrics metrics.Metrics

	size int64

	// TODO: Optimize this so we don't need to keep the fetchers in memory.
	fetchers *utils.WaitMap
}

func NewChunker(
	size, blockSize int64,
	s storage.FrameGetter,
	objectPath string,
	frameTable *storage.FrameTable,
	cachePath string,
	metrics metrics.Metrics,
) (*Chunker, error) {
	cache, err := NewCache(size, blockSize, cachePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create file cache: %w", err)
	}

	chunker := &Chunker{
		size:       size,
		storage:    s,
		objectPath: objectPath,
		frameTable: frameTable,
		cache:      cache,
		fetchers:   utils.NewWaitMap(),
		metrics:    metrics,
	}

	return chunker, nil
}

func (c *Chunker) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	slice, err := c.Slice(ctx, off, int64(len(b)))
	if err != nil {
		return 0, fmt.Errorf("failed to slice cache at %d-%d: %w", off, off+int64(len(b)), err)
	}

	return copy(b, slice), nil
}

func (c *Chunker) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	timer := c.metrics.SlicesTimerFactory.Begin()

	b, err := c.cache.Slice(off, length)
	if err == nil {
		timer.Success(ctx, length,
			attribute.String(pullType, pullTypeLocal))

		return b, nil
	}

	if !errors.As(err, &BytesNotAvailableError{}) {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, failureTypeLocalRead))

		return nil, fmt.Errorf("failed read from cache at offset %d: %w", off, err)
	}

	chunkErr := c.fetchToCache(ctx, off, length)
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

	timer.Success(ctx, length,
		attribute.String(pullType, pullTypeRemote))

	return b, nil
}

// fetchToCache ensures that the data at the given offset and length is available in the cache.
// This chunker only handles uncompressed data; compressed data uses CompressedChunker.
func (c *Chunker) fetchToCache(ctx context.Context, off, length int64) error {
	var eg errgroup.Group

	startingChunk := header.BlockIdx(off, storage.MemoryChunkSize)
	startingChunkOffset := header.BlockOffset(startingChunk, storage.MemoryChunkSize)
	nChunks := header.BlockIdx(length+storage.MemoryChunkSize-1, storage.MemoryChunkSize)

	currentOff := startingChunkOffset
	for range nChunks {
		fetchOff := currentOff
		currentOff += storage.MemoryChunkSize

		eg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.L().Error(ctx, "recovered from panic in the fetch handler", zap.Any("error", r))
					err = fmt.Errorf("recovered from panic in the fetch handler: %v", r)
				}
			}()

			err = c.fetchers.Wait(fetchOff, func() error {
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

				fetchSW := c.metrics.RemoteReadsTimerFactory.Begin()

				fmt.Printf("<>/<> fetchToCache: objectPath: %s, offsetU: %#x, frameTable: %+v\n", c.objectPath, fetchOff, c.frameTable)

				_, err = c.storage.GetFrame(ctx, c.objectPath, fetchOff, c.frameTable, true, b)
				if err != nil {
					fetchSW.Failure(ctx, int64(len(b)),
						attribute.String(failureReason, failureTypeRemoteRead),
					)

					return fmt.Errorf("failed to read chunk from base %d: %w", fetchOff, err)
				}

				c.cache.setIsCached(fetchOff, int64(len(b)))
				fetchSW.Success(ctx, int64(len(b)))

				return nil
			})

			return err
		})
	}

	return eg.Wait()
}

func (c *Chunker) Close() error {
	return c.cache.Close()
}

func (c *Chunker) FileSize() (int64, error) {
	return c.cache.FileSize()
}

const (
	pullType       = "pull-type"
	pullTypeLocal  = "local"
	pullTypeRemote = "remote"

	failureReason = "failure-reason"

	failureTypeLocalRead      = "local-read"
	failureTypeLocalReadAgain = "local-read-again"
	failureTypeRemoteRead     = "remote-read"
	failureTypeCacheFetch     = "cache-fetch"
)
