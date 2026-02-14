package block

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/klauspost/compress/zstd"
	lz4 "github.com/pierrec/lz4/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// CompressMMapLRUChunker is a two-level cache chunker:
//   - Level 1: LRU cache for decompressed frames (in-memory)
//   - Level 2: mmap cache for compressed frames (on-disk, lazily populated)
type CompressMMapLRUChunker struct {
	storage    storage.FrameGetter
	objectPath string

	// Level 1: Decompressed frame LRU
	frameLRU        *FrameLRU
	decompressGroup singleflight.Group

	// Level 2: Compressed frame mmap cache
	compressedCache *MMapFrameCache

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

	frameLRU, err := NewFrameLRU(lruSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create frame LRU: %w", err)
	}

	compressedCache, err := NewFrameCache(rawSize, cachePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressed cache: %w", err)
	}

	return &CompressMMapLRUChunker{
		storage:         s,
		objectPath:      objectPath,
		frameLRU:        frameLRU,
		compressedCache: compressedCache,
		virtSize:        virtSize,
		rawSize:         rawSize,
		metrics:         m,
	}, nil
}

func (c *CompressMMapLRUChunker) Chunk(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
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
	if frame, ok := c.frameLRU.get(frameStarts.U); ok {
		return frame.data, true, nil
	}

	key := strconv.FormatInt(frameStarts.U, 10)
	dataI, err, _ := c.decompressGroup.Do(key, func() (any, error) {
		if frame, ok := c.frameLRU.get(frameStarts.U); ok {
			return frame.data, nil
		}

		return c.fetchDecompressAndCache(ctx, frameStarts, frameSize, ft)
	})
	if err != nil {
		return nil, false, err
	}

	return dataI.([]byte), false, nil
}

func (c *CompressMMapLRUChunker) fetchDecompressAndCache(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) ([]byte, error) {
	compressedData, _, err := c.ensureCompressedInMmap(ctx, frameStarts, frameSize, ft)
	if err != nil {
		return nil, err
	}

	var data []byte
	switch ft.CompressionType {
	case storage.CompressionZstd:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer dec.Close()

		data, err = dec.DecodeAll(compressedData, make([]byte, 0, frameSize.U))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress frame at %#x: %w", frameStarts.U, err)
		}

	case storage.CompressionLZ4:
		data = make([]byte, frameSize.U)
		n, err := io.ReadFull(lz4.NewReader(bytes.NewReader(compressedData)), data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress frame at %#x: %w", frameStarts.U, err)
		}
		data = data[:n]

	default:
		return nil, fmt.Errorf("unsupported compression type: %d", ft.CompressionType)
	}

	c.frameLRU.put(frameStarts.U, int64(frameSize.U), data)

	return data, nil
}

func (c *CompressMMapLRUChunker) ensureCompressedInMmap(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) ([]byte, bool, error) {
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
