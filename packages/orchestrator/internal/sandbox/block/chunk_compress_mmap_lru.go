package block

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

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
//
// On Slice:
//  1. Check LRU for decompressed frame → hit = return directly
//  2. Check mmap for compressed frame → hit = decompress → LRU → return
//  3. Fetch compressed from storage → mmap → decompress → LRU → return
//
// Benefits:
//   - After first fetch, LRU misses read from local mmap (not network)
//   - Compressed frames stay cached locally even after LRU eviction
type CompressMMapLRUChunker struct {
	storage    storage.FrameGetter
	objectPath string

	// Level 1: Decompressed frame LRU
	frameLRU        *FrameLRU
	decompressGroup singleflight.Group // dedup concurrent decompression requests

	// Level 2: Compressed frame mmap cache
	compressedCache *MMapFrameCache

	virtSize int64 // uncompressed size - used to cap requests
	rawSize  int64 // compressed file size - used for mmap sizing
	metrics  metrics.Metrics

	// Stats counters
	slices            atomic.Int64
	sliceBytes        atomic.Int64
	fetches           atomic.Int64
	fetchBytes        atomic.Int64
	decompressions    atomic.Int64
	decompInputBytes  atomic.Int64
	decompOutputBytes atomic.Int64
	decompDurationNs  atomic.Int64
	compressionType   atomic.Value // stores storage.CompressionType
}

var _ Chunker = (*CompressMMapLRUChunker)(nil)

// NewCompressMMapLRUChunker creates a new two-level cache chunker.
//
// Parameters:
//   - virtSize: total uncompressed size (used to cap requests)
//   - rawSize: total compressed file size (used to size the mmap)
//   - s: storage provider to fetch compressed frames from
//   - objectPath: path to object in storage
//   - cachePath: path for compressed frame mmap file
//   - lruSize: number of decompressed frames to keep in LRU (0 for default)
//   - m: metrics collector
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

	// Level 1: Decompressed frame LRU
	frameLRU, err := NewFrameLRU(lruSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create frame LRU: %w", err)
	}

	// Level 2: Compressed frame mmap cache - sized to rawSize (compressed file size)
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

// Slice returns a slice of data at the given offset and length.
// The returned slice references internal LRU data and MUST NOT be modified.
// ft is the frame table subset for the specific mapping being read.
func (c *CompressMMapLRUChunker) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	data, _, err := c.sliceWithStats(ctx, off, length, ft)

	return data, err
}

func (c *CompressMMapLRUChunker) sliceWithStats(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, bool, error) {
	c.slices.Add(1)
	c.sliceBytes.Add(length)
	if ft != nil {
		c.compressionType.CompareAndSwap(nil, ft.CompressionType)
	}

	timer := c.metrics.SlicesTimerFactory.Begin()

	// Clamp length to available data
	if off+length > c.virtSize {
		length = c.virtSize - off
	}
	if length <= 0 {
		return []byte{}, true, nil
	}

	// CompressMMapLRUChunker requires a FrameTable - it only handles compressed data
	if ft == nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, "nil_frame_table"))

		logger.L().Error(ctx, "CompressMMapLRU: nil frame table",
			zap.String("object", c.objectPath),
			zap.Int64("offset", off),
			zap.Int64("length", length))

		return nil, false, fmt.Errorf("CompressMMapLRUChunker requires FrameTable for compressed data at offset %d", off)
	}

	// Find the frame containing the start offset using the passed frame table subset
	frameStarts, frameSize, err := ft.FrameFor(off)
	if err != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, "frame_lookup_failed"))

		return nil, false, fmt.Errorf("failed to get frame for offset %d: %w", off, err)
	}

	startInFrame := off - frameStarts.U
	endInFrame := startInFrame + length

	// Fast path: entire read fits in one frame
	if endInFrame <= int64(frameSize.U) {
		data, wasHit, err := c.getOrFetchFrame(ctx, frameStarts, frameSize, ft)
		if err != nil {
			timer.Failure(ctx, length,
				attribute.String(pullType, pullTypeRemote),
				attribute.String(failureReason, failureTypeCacheFetch))

			return nil, false, err
		}

		timer.Success(ctx, length, attribute.String(pullType, pullTypeRemote))

		// Cap endInFrame to actual decompressed data length (last frame may be smaller)
		if endInFrame > int64(len(data)) {
			endInFrame = int64(len(data))
		}

		return data[startInFrame:endInFrame], wasHit, nil
	}

	// Slow path: read spans multiple frames
	timer.Failure(ctx, length,
		attribute.String(pullType, pullTypeLocal),
		attribute.String(failureReason, "cross_frame_span"))

	logger.L().Error(ctx, "CompressMMapLRU: cross-frame read",
		zap.String("object", c.objectPath),
		zap.Int64("offset", off),
		zap.Int64("length", length),
		zap.Int64("startInFrame", startInFrame),
		zap.Int64("endInFrame", endInFrame),
		zap.Int32("frameSize", frameSize.U),
		zap.Int64("frameStartsU", frameStarts.U))

	return nil, false, fmt.Errorf("read spans frame boundary - off=%#x length=%d startInFrame=%d endInFrame=%d frameSize=%d frameStartsU=%#x",
		off, length, startInFrame, endInFrame, frameSize.U, frameStarts.U)
}

// getOrFetchFrame returns decompressed frame data, checking LRU then mmap then storage.
// Returns: data, wasLRUHit, error
// ft is the frame table subset for the specific mapping being read.
func (c *CompressMMapLRUChunker) getOrFetchFrame(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) ([]byte, bool, error) {
	// Level 1: Check LRU for decompressed frame
	if frame, ok := c.frameLRU.get(frameStarts.U); ok {
		return frame.data, true, nil
	}

	// Dedup concurrent requests for same frame
	key := strconv.FormatInt(frameStarts.U, 10)
	dataI, err, _ := c.decompressGroup.Do(key, func() (any, error) {
		// Double-check LRU
		if frame, ok := c.frameLRU.get(frameStarts.U); ok {
			return frame.data, nil
		}

		return c.fetchDecompressAndCache(ctx, frameStarts, frameSize, ft)
	})
	if err != nil {
		logger.L().Error(ctx, "CompressMMapLRU: getOrFetchFrame failed",
			zap.String("object", c.objectPath),
			zap.Int64("frameStartU", frameStarts.U),
			zap.Int64("frameStartC", frameStarts.C),
			zap.Int32("frameSizeU", frameSize.U),
			zap.Int32("frameSizeC", frameSize.C),
			zap.Error(err))

		return nil, false, err
	}

	return dataI.([]byte), false, nil
}

// fetchDecompressAndCache ensures compressed frame is in mmap, decompresses, and stores in LRU.
// ft is the frame table subset for the specific mapping being read.
func (c *CompressMMapLRUChunker) fetchDecompressAndCache(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) ([]byte, error) {
	// Level 2: Ensure compressed frame is in mmap cache
	fetchStart := time.Now()
	compressedData, _, err := c.ensureCompressedInMmap(ctx, frameStarts, frameSize, ft)
	if err != nil {
		logger.L().Error(ctx, "CompressMMapLRU: ensureCompressedInMmap failed",
			zap.String("object", c.objectPath),
			zap.Int64("frameStartU", frameStarts.U),
			zap.Int64("frameStartC", frameStarts.C),
			zap.Duration("fetchDuration", time.Since(fetchStart)),
			zap.Error(err))

		return nil, err
	}

	// Decompress with timing
	decompStart := time.Now()

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

	decompDur := time.Since(decompStart)
	c.decompressions.Add(1)
	c.decompInputBytes.Add(int64(len(compressedData)))
	c.decompOutputBytes.Add(int64(len(data)))
	c.decompDurationNs.Add(decompDur.Nanoseconds())

	// Store in LRU
	c.frameLRU.put(frameStarts.U, int64(frameSize.U), data)

	return data, nil
}

// ensureCompressedInMmap returns compressed frame data from mmap, fetching from storage if needed.
// ft is the frame table subset for the specific mapping being read.
func (c *CompressMMapLRUChunker) ensureCompressedInMmap(ctx context.Context, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) ([]byte, bool, error) {
	return c.compressedCache.GetOrFetch(frameStarts.C, int64(frameSize.C), func(buf []byte) error {
		_, err := c.storage.GetFrame(ctx, c.objectPath, frameStarts.U, ft, false, buf)
		if err != nil {
			return fmt.Errorf("failed to fetch compressed frame at %#x: %w", frameStarts.C, err)
		}

		c.fetches.Add(1)
		c.fetchBytes.Add(int64(frameSize.C))

		return nil
	})
}

// Close releases all resources.
func (c *CompressMMapLRUChunker) Close() error {
	if c.frameLRU != nil {
		c.frameLRU.Purge()
	}

	if c.compressedCache != nil {
		return c.compressedCache.Close()
	}

	return nil
}

// FileSize returns the on-disk size of the compressed mmap cache file.
func (c *CompressMMapLRUChunker) FileSize() (int64, error) {
	return c.compressedCache.FileSize()
}
