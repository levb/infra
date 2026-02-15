package block

import (
	"context"
	"fmt"
	"os"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// DefaultLRUFrameCount is the default number of decompressed frames to keep in the LRU cache.
const DefaultLRUFrameCount = 16

// CompressMMapLRUChunker is a two-level cache chunker:
//   - Level 1: LRU cache for decompressed frames (in-memory)
//   - Level 2: per-frame file cache for compressed frames (on-disk, lazily populated)
type CompressMMapLRUChunker struct {
	storage    storage.FrameGetter
	objectPath string

	// Level 1: Decompressed frame LRU
	frameLRU      *lru.Cache[int64, []byte]
	decompressMap *utils.WaitMap

	// Level 2: Compressed frame file cache
	compressedCache *FrameFileCache

	virtSize int64 // uncompressed size
	rawSize  int64 // compressed file size
	metrics  metrics.Metrics
}

// NewCompressMMapLRUChunker creates a new two-level cache chunker.
func NewCompressMMapLRUChunker(
	virtSize, rawSize int64,
	s storage.FrameGetter,
	objectPath string,
	cachePath string,
	lruSize int,
	m metrics.Metrics,
) (*CompressMMapLRUChunker, error) {
	if lruSize <= 0 {
		lruSize = DefaultLRUFrameCount
	}

	frameLRU, err := lru.New[int64, []byte](lruSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create frame LRU: %w", err)
	}

	compressedCache, err := NewFrameFileCache(cachePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressed cache: %w", err)
	}

	return &CompressMMapLRUChunker{
		storage:         s,
		objectPath:      objectPath,
		frameLRU:        frameLRU,
		decompressMap:   utils.NewWaitMap(),
		compressedCache: compressedCache,
		virtSize:        virtSize,
		rawSize:         rawSize,
		metrics:         m,
	}, nil
}

func (c *CompressMMapLRUChunker) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	timer := c.metrics.SlicesTimerFactory.Begin()

	if off+length > c.virtSize {
		length = c.virtSize - off
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

		return nil, fmt.Errorf("CompressMMapLRUChunker requires FrameTable for compressed data at offset %d", off)
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

	// Fast path: entire read fits in one frame
	if endInFrame <= int64(frameSize.U) {
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

	// Slow path: read spans multiple frames
	logger.L().Error(ctx, "CompressMMapLRU: cross-frame read",
		zap.String("object", c.objectPath),
		zap.Int64("offset", off),
		zap.Int64("length", length),
		zap.Int32("frameSize", frameSize.U),
		zap.Int64("frameStartsU", frameStarts.U))

	return nil, fmt.Errorf("read spans frame boundary - off=%#x length=%d startInFrame=%d endInFrame=%d frameSize=%d frameStartsU=%#x",
		off, length, startInFrame, endInFrame, frameSize.U, frameStarts.U)
}

func (c *CompressMMapLRUChunker) getOrFetchFrame(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) ([]byte, bool, error) {
	if data, ok := c.frameLRU.Get(frameStarts.U); ok {
		return data, true, nil
	}

	err := c.decompressMap.Wait(frameStarts.U, func() error {
		_, err := c.fetchDecompressAndCache(ctx, frameStarts, frameSize, ft)

		return err
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

func (c *CompressMMapLRUChunker) fetchDecompressAndCache(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) ([]byte, error) {
	framePath, err := c.ensureCompressedOnDisk(ctx, frameStarts, frameSize, ft)
	if err != nil {
		return nil, err
	}

	// Stream compressed data from file through decoder — no intermediate []byte in memory
	f, err := os.Open(framePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open cached frame file %s: %w", framePath, err)
	}
	defer f.Close()

	data, err := storage.DecompressReader(ft.CompressionType, f, int(frameSize.U))
	if err != nil {
		return nil, fmt.Errorf("failed to decompress frame at %#x: %w", frameStarts.U, err)
	}

	c.frameLRU.Add(frameStarts.U, data)

	return data, nil
}

func (c *CompressMMapLRUChunker) ensureCompressedOnDisk(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) (string, error) {
	return c.compressedCache.GetOrFetch(frameStarts.C, int64(frameSize.C), func(buf []byte) error {
		_, err := c.storage.GetFrame(ctx, c.objectPath, frameStarts.U, ft, false, buf)
		if err != nil {
			return fmt.Errorf("failed to fetch compressed frame at %#x: %w", frameStarts.C, err)
		}

		return nil
	})
}

func (c *CompressMMapLRUChunker) Close() error {
	if c.frameLRU != nil {
		c.frameLRU.Purge()
	}

	if c.compressedCache != nil {
		return c.compressedCache.Close()
	}

	return nil
}

func (c *CompressMMapLRUChunker) FileSize() (int64, error) {
	return c.compressedCache.FileSize()
}
