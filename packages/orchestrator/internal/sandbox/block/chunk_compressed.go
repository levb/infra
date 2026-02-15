package block

import (
	"context"
	"fmt"
	"os"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// DefaultLRUFrameCount is the default number of decompressed frames to keep in the LRU cache.
const DefaultLRUFrameCount = 4

// CompressedFileLRUChunker is a two-level cache chunker:
//   - Level 1: LRU cache for decompressed frames (in-memory)
//   - Level 2: per-frame file cache for compressed frames (on-disk, lazily populated)
type CompressedFileLRUChunker struct {
	storage    storage.FrameGetter
	objectPath string

	// Level 1: Decompressed frame LRU
	frameLRU *lru.Cache[int64, []byte]

	// Level 2: Compressed frame file cache
	compressedCache *FrameCache

	fetchMap *utils.WaitMap
	size     int64 // uncompressed size
	rawSize  int64 // compressed file size
	metrics  metrics.Metrics
}

// NewCompressedFileLRUChunker creates a new two-level cache chunker.
func NewCompressedFileLRUChunker(
	size, rawSize int64,
	s storage.FrameGetter,
	objectPath string,
	cachePath string,
	lruSize int,
	m metrics.Metrics,
) (*CompressedFileLRUChunker, error) {
	if lruSize <= 0 {
		lruSize = DefaultLRUFrameCount
	}

	frameLRU, err := lru.New[int64, []byte](lruSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create frame LRU: %w", err)
	}

	compressedCache, err := NewFrameCache(cachePath, s, objectPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressed cache: %w", err)
	}

	return &CompressedFileLRUChunker{
		storage:         s,
		objectPath:      objectPath,
		frameLRU:        frameLRU,
		fetchMap:        utils.NewWaitMap(),
		compressedCache: compressedCache,
		size:            size,
		rawSize:         rawSize,
		metrics:         m,
	}, nil
}

func (c *CompressedFileLRUChunker) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	timer := c.metrics.SlicesTimerFactory.Begin()

	if off+length > c.size {
		length = c.size - off
	}
	if length <= 0 {
		return []byte{}, nil
	}

	if ft == nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, "nil_frame_table"))

		logger.L().Error(ctx, "CompressMMapLRU: nil frame table",
			zap.String("object", c.objectPath),
			zap.Int64("offset", off),
			zap.Int64("length", length))

		return nil, fmt.Errorf("CompressedFileLRUChunker requires FrameTable for compressed data at offset %d", off)
	}

	frameStarts, frameSize, err := ft.FrameFor(off)
	if err != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, "frame_lookup_failed"))

		return nil, fmt.Errorf("failed to get frame for offset %d: %w", off, err)
	}

	startInFrame := off - frameStarts.U
	endInFrame := startInFrame + length

	if endInFrame > int64(frameSize.U) {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, "cross_frame_read"))

		return nil, fmt.Errorf("read spans frame boundary: off=%#x length=%d frameStart=%#x frameSize=%d",
			off, length, frameStarts.U, frameSize.U)
	}

	data, _, err := c.getOrFetchFrame(ctx, frameStarts, frameSize, ft)
	if err != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeRemote),
			attribute.String(failureReason, failureTypeCacheFetch))

		return nil, err
	}

	timer.Success(ctx, length, attribute.String(pullType, pullTypeRemote))

	if endInFrame > int64(len(data)) {
		endInFrame = int64(len(data))
	}

	return data[startInFrame:endInFrame], nil
}

func (c *CompressedFileLRUChunker) getOrFetchFrame(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) ([]byte, bool, error) {
	if data, ok := c.frameLRU.Get(frameStarts.U); ok {
		return data, true, nil
	}

	err := c.fetchMap.Wait(frameStarts.U, func() error {
		framePath, compressedBuf, err := c.compressedCache.getOrFetch(ctx, frameStarts, frameSize, ft)
		if err != nil {
			return err
		}

		var data []byte

		if compressedBuf != nil {
			// We fetched the compressed frame from upstream, but haven't
			// persisted it to disk yet. Decompress in-memory and persist
			// asynchronously in the background.
			var eg errgroup.Group

			eg.Go(func() error {
				if err := c.compressedCache.Persist(frameStarts, frameSize, compressedBuf); err != nil {
					// Best-effort — frame will be in LRU, disk is for re-access after eviction.
					logger.L().Error(ctx, "failed to persist compressed frame to disk", zap.Error(err))
				}

				return nil
			})

			eg.Go(func() error {
				var err error
				data, err = storage.DecompressFrame(ft.CompressionType, compressedBuf, frameSize.U)
				if err != nil {
					return fmt.Errorf("failed to decompress frame at %#x: %w", frameStarts.U, err)
				}

				return nil
			})

			if err := eg.Wait(); err != nil {
				return err
			}
		} else {
			// Slow path: frame already on disk (previous run or LRU eviction re-access)
			f, err := os.Open(framePath)
			if err != nil {
				return fmt.Errorf("failed to open cached frame file %s: %w", framePath, err)
			}
			defer f.Close()

			data, err = storage.DecompressReader(ft.CompressionType, f, int(frameSize.U))
			if err != nil {
				return fmt.Errorf("failed to decompress frame at %#x: %w", frameStarts.U, err)
			}
		}

		c.frameLRU.Add(frameStarts.U, data)

		return nil
	})
	if err != nil {
		return nil, false, err
	}

	data, ok := c.frameLRU.Get(frameStarts.U)
	if !ok {
		return nil, false, fmt.Errorf("frame at %#x not in LRU after fetch", frameStarts.U)
	}

	return data, false, nil
}

func (c *CompressedFileLRUChunker) Close() error {
	if c.frameLRU != nil {
		c.frameLRU.Purge()
	}

	if c.compressedCache != nil {
		return c.compressedCache.Close()
	}

	return nil
}

func (c *CompressedFileLRUChunker) FileSize() (int64, error) {
	return c.compressedCache.FileSize()
}
