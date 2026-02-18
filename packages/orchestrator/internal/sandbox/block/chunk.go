package block

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// Storage combines frame-based access (compressed) with seekable access
// (uncompressed). StorageProvider satisfies this.
type Storage interface {
	storage.FrameGetter
	OpenSeekable(ctx context.Context, path string, seekableObjectType storage.SeekableObjectType) (storage.Seekable, error)
}

// Chunker fetches data from storage into a shared mmap cache.
// Each GetBlock routes to compressed or uncompressed fetch based on the
// FrameTable and AssetInfo. Either way data lands decompressed in mmap,
// so subsequent reads are always local cache hits.
type Chunker struct {
	storage    Storage
	assets     AssetInfo
	objectType storage.SeekableObjectType

	cache   *Cache
	metrics metrics.Metrics

	fetchMu  sync.Mutex
	fetchMap map[fetchKey]*fetchSession
}

var _ Reader = (*Chunker)(nil)

// NewChunker creates a Chunker backed by a new mmap cache at cachePath.
func NewChunker(
	assets AssetInfo,
	blockSize int64,
	s Storage,
	objectType storage.SeekableObjectType,
	cachePath string,
	m metrics.Metrics,
) (*Chunker, error) {
	cache, err := NewCache(assets.Size, blockSize, cachePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	return &Chunker{
		storage:    s,
		assets:     assets,
		objectType: objectType,
		cache:      cache,
		metrics:    m,
		fetchMap:   make(map[fetchKey]*fetchSession),
	}, nil
}

func (c *Chunker) ReadBlock(ctx context.Context, b []byte, off int64, ft *storage.FrameTable) (int, error) {
	slice, err := c.GetBlock(ctx, off, int64(len(b)), ft)
	if err != nil {
		return 0, fmt.Errorf("failed to get block at %d-%d: %w", off, off+int64(len(b)), err)
	}

	return copy(b, slice), nil
}

// GetBlock reads data at the given uncompressed offset.
// If ft is non-nil and a matching compressed asset exists, fetches via compressed path.
// Otherwise falls back to uncompressed streaming.
func (c *Chunker) GetBlock(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, fmt.Errorf("invalid slice params: off=%d length=%d", off, length)
	}
	if off+length > c.assets.Size {
		return nil, fmt.Errorf("slice out of bounds: off=%#x length=%d size=%d", off, length, c.assets.Size)
	}

	useCompressed := c.assets.HasCompressed(ft)

	timer := c.metrics.SlicesTimerFactory.Begin(
		attribute.String(chunkerTypeAttr, ChunkerTypeDecompressMMap),
		attribute.Bool(compressedAttr, useCompressed),
	)

	// Fast path: already in mmap cache.
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

	var (
		session    *fetchSession
		sessionErr error
	)

	if useCompressed {
		session, sessionErr = c.getOrCreateCompressedSession(ctx, off, ft)
	} else {
		session = c.getOrCreateUncompressedSession(ctx, off)
	}

	if sessionErr != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeRemote),
			attribute.String(failureReason, "session_create"))

		return nil, sessionErr
	}

	if err := session.registerAndWait(ctx, off, length); err != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeRemote),
			attribute.String(failureReason, failureTypeCacheFetch))

		return nil, fmt.Errorf("failed to fetch data at %#x: %w", off, err)
	}

	b, cacheErr := c.cache.Slice(off, length)
	if cacheErr != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeLocal),
			attribute.String(failureReason, failureTypeLocalReadAgain))

		return nil, fmt.Errorf("failed to read from cache after fetch at %d-%d: %w", off, off+length, cacheErr)
	}

	timer.Success(ctx, length,
		attribute.String(pullType, pullTypeRemote))

	return b, nil
}

// --- Compressed path ---

func (c *Chunker) getOrCreateCompressedSession(ctx context.Context, off int64, ft *storage.FrameTable) (*fetchSession, error) {
	frameStarts, frameSize, err := ft.FrameFor(off)
	if err != nil {
		return nil, fmt.Errorf("failed to get frame for offset %#x: %w", off, err)
	}

	key := fetchKey{offset: frameStarts.U, compressed: true}

	c.fetchMu.Lock()
	if existing, ok := c.fetchMap[key]; ok {
		c.fetchMu.Unlock()

		return existing, nil
	}

	s := newFetchSession(frameStarts.U, int64(frameSize.U), c.cache.BlockSize(), c.cache.isCached)
	c.fetchMap[key] = s
	c.fetchMu.Unlock()

	go c.runCompressedFetch(context.WithoutCancel(ctx), s, key, frameStarts, frameSize, ft)

	return s, nil
}

func (c *Chunker) runCompressedFetch(ctx context.Context, s *fetchSession, key fetchKey, frameStarts storage.FrameOffset, frameSize storage.FrameSize, ft *storage.FrameTable) {
	ctx, cancel := context.WithTimeout(ctx, decompressFetchTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			c.removeFetchSession(key)
			s.fail(fmt.Errorf("frame fetch panicked: %v", r))
		}
	}()

	fetchSW := c.metrics.RemoteReadsTimerFactory.Begin(
		attribute.String(chunkerTypeAttr, ChunkerTypeDecompressMMap),
		attribute.Bool(compressedAttr, true),
	)

	mmapSlice, releaseLock, err := c.cache.addressBytes(frameStarts.U, int64(frameSize.U))
	if err != nil {
		fetchSW.Failure(ctx, int64(frameSize.C),
			attribute.String(failureReason, "mmap_address"))
		c.removeFetchSession(key)
		s.fail(err)

		return
	}
	defer releaseLock()

	// onProgress publishes blocks to cache and wakes waiters as they complete.
	compressedPath := storage.V4DataPath(c.assets.BasePath, ft.CompressionType)
	var prevTotal int64
	onProgress := func(totalWritten int64) {
		newBytes := totalWritten - prevTotal
		c.cache.setIsCached(frameStarts.U+prevTotal, newBytes)
		s.advance(totalWritten)
		prevTotal = totalWritten
	}

	_, err = c.storage.GetFrame(ctx, compressedPath, frameStarts.U, ft, true, mmapSlice[:frameSize.U], onProgress)
	if err != nil {
		fetchSW.Failure(ctx, int64(frameSize.C),
			attribute.String(failureReason, failureTypeRemoteRead))
		c.removeFetchSession(key)
		s.fail(fmt.Errorf("failed to fetch compressed frame at %#x: %w", frameStarts.U, err))

		return
	}

	fetchSW.Success(ctx, int64(frameSize.U))
	c.removeFetchSession(key)
	s.complete()
}

// --- Uncompressed path ---

func (c *Chunker) getOrCreateUncompressedSession(ctx context.Context, off int64) *fetchSession {
	chunkOff := (off / storage.MemoryChunkSize) * storage.MemoryChunkSize
	chunkLen := min(int64(storage.MemoryChunkSize), c.assets.Size-chunkOff)
	key := fetchKey{offset: chunkOff, compressed: false}

	c.fetchMu.Lock()
	if existing, ok := c.fetchMap[key]; ok {
		c.fetchMu.Unlock()

		return existing
	}

	s := newFetchSession(chunkOff, chunkLen, c.cache.BlockSize(), c.cache.isCached)
	c.fetchMap[key] = s
	c.fetchMu.Unlock()

	go c.runUncompressedFetch(context.WithoutCancel(ctx), s, key)

	return s
}

func (c *Chunker) runUncompressedFetch(ctx context.Context, s *fetchSession, key fetchKey) {
	ctx, cancel := context.WithTimeout(ctx, decompressFetchTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			c.removeFetchSession(key)
			s.fail(fmt.Errorf("uncompressed fetch panicked: %v", r))
		}
	}()

	obj, err := c.storage.OpenSeekable(ctx, c.assets.BasePath, c.objectType)
	if err != nil {
		c.removeFetchSession(key)
		s.fail(fmt.Errorf("failed to open seekable %s: %w", c.assets.BasePath, err))

		return
	}

	mmapSlice, releaseLock, err := c.cache.addressBytes(s.chunkOff, s.chunkLen)
	if err != nil {
		c.removeFetchSession(key)
		s.fail(err)

		return
	}
	defer releaseLock()

	fetchTimer := c.metrics.RemoteReadsTimerFactory.Begin(
		attribute.String(chunkerTypeAttr, ChunkerTypeDecompressMMap),
		attribute.Bool(compressedAttr, false),
	)

	reader, err := obj.OpenRangeReader(ctx, s.chunkOff, s.chunkLen)
	if err != nil {
		fetchTimer.Failure(ctx, s.chunkLen,
			attribute.String(failureReason, failureTypeRemoteRead))
		c.removeFetchSession(key)
		s.fail(fmt.Errorf("failed to open range reader at %d: %w", s.chunkOff, err))

		return
	}
	defer reader.Close()

	blockSize := c.cache.BlockSize()
	var totalRead int64
	var prevCompleted int64

	for totalRead < s.chunkLen {
		readEnd := min(totalRead+blockSize, s.chunkLen)
		n, readErr := reader.Read(mmapSlice[totalRead:readEnd])
		totalRead += int64(n)

		completedBlocks := totalRead / blockSize
		if completedBlocks > prevCompleted {
			newBytes := (completedBlocks - prevCompleted) * blockSize
			c.cache.setIsCached(s.chunkOff+prevCompleted*blockSize, newBytes)
			prevCompleted = completedBlocks

			s.advance(completedBlocks * blockSize)
		}

		if errors.Is(readErr, io.EOF) {
			if totalRead > prevCompleted*blockSize {
				c.cache.setIsCached(s.chunkOff+prevCompleted*blockSize, totalRead-prevCompleted*blockSize)
			}

			break
		}

		if readErr != nil {
			fetchTimer.Failure(ctx, s.chunkLen,
				attribute.String(failureReason, failureTypeRemoteRead))
			c.removeFetchSession(key)
			s.fail(fmt.Errorf("failed reading at offset %d after %d bytes: %w", s.chunkOff, totalRead, readErr))

			return
		}
	}

	fetchTimer.Success(ctx, s.chunkLen)
	c.removeFetchSession(key)
	s.complete()
}

// removeFetchSession removes a session from the fetchMap.
// Must be called BEFORE complete()/fail() to prevent stale session reuse.
func (c *Chunker) removeFetchSession(key fetchKey) {
	c.fetchMu.Lock()
	delete(c.fetchMap, key)
	c.fetchMu.Unlock()
}

func (c *Chunker) Close() error {
	return c.cache.Close()
}

func (c *Chunker) FileSize() (int64, error) {
	return c.cache.FileSize()
}

// fetchKey distinguishes compressed and uncompressed fetch sessions that
// may have overlapping U-space offsets.
type fetchKey struct {
	offset     int64
	compressed bool
}

// AssetInfo describes which storage variants exist for a build artifact.
type AssetInfo struct {
	BasePath        string // uncompressed path (e.g., "build-123/memfile")
	Size            int64  // uncompressed size (from either source)
	HasUncompressed bool   // true if the uncompressed object exists in storage
	HasLZ4          bool   // true if a .lz4 compressed variant exists
	HasZst          bool   // true if a .zst compressed variant exists
}

// HasCompressed reports whether a compressed asset matching ft's type exists.
func (a *AssetInfo) HasCompressed(ft *storage.FrameTable) bool {
	if ft == nil {
		return false
	}

	switch ft.CompressionType {
	case storage.CompressionLZ4:
		return a.HasLZ4
	case storage.CompressionZstd:
		return a.HasZst
	default:
		return false
	}
}

// --- Metric attribute constants ---

const (
	pullType       = "pull-type"
	pullTypeLocal  = "local"
	pullTypeRemote = "remote"

	failureReason = "failure-reason"

	failureTypeLocalRead      = "local-read"
	failureTypeLocalReadAgain = "local-read-again"
	failureTypeRemoteRead     = "remote-read"
	failureTypeCacheFetch     = "cache-fetch"

	chunkerTypeAttr = "chunker"
	compressedAttr  = "compressed"

	ChunkerTypeDecompressMMap = "decompress-mmap"

	// decompressFetchTimeout is the maximum time a single frame/chunk fetch may take.
	decompressFetchTimeout = 60 * time.Second
)
