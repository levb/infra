package block

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type rangeWaiter struct {
	off    int64
	length int64
	ch     chan error // buffered cap 1
}

const (
	fetchStateRunning = iota
	fetchStateDone
	fetchStateErrored
)

type fetchSession struct {
	mu       sync.Mutex
	chunkOff int64
	chunkLen int64
	cache    *Cache
	waiters  []*rangeWaiter
	state    int
	fetchErr error
}

// registerAndWait adds a waiter for the given range and blocks until the range
// is cached or the context is cancelled. Returns nil if the range was already
// cached before registering.
func (s *fetchSession) registerAndWait(ctx context.Context, off, length int64) error {
	s.mu.Lock()

	// Fast path: already cached
	if s.cache.isCached(off, length) {
		s.mu.Unlock()
		return nil
	}

	// Session already done
	if s.state == fetchStateDone {
		s.mu.Unlock()
		if s.cache.isCached(off, length) {
			return nil
		}
		return fmt.Errorf("fetch completed but range %d-%d not cached", off, off+length)
	}

	// Session errored
	if s.state == fetchStateErrored {
		s.mu.Unlock()
		if s.cache.isCached(off, length) {
			return nil
		}
		return fmt.Errorf("fetch failed: %w", s.fetchErr)
	}

	w := &rangeWaiter{
		off:    off,
		length: length,
		ch:     make(chan error, 1),
	}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// notifyWaiters checks all waiters and notifies those whose ranges are now cached.
func (s *fetchSession) notifyWaiters(sendErr error) {
	remaining := s.waiters[:0]
	for _, w := range s.waiters {
		if sendErr != nil {
			if s.cache.isCached(w.off, w.length) {
				w.ch <- nil
			} else {
				w.ch <- sendErr
			}
			continue
		}
		if s.cache.isCached(w.off, w.length) {
			w.ch <- nil
			continue
		}
		remaining = append(remaining, w)
	}
	s.waiters = remaining
}

type StreamingChunker struct {
	base    storage.StreamingReader
	cache   *Cache
	metrics metrics.Metrics

	size int64

	fetchMu  sync.Mutex
	fetchMap map[int64]*fetchSession
}

func NewStreamingChunker(
	size, blockSize int64,
	base storage.StreamingReader,
	cachePath string,
	metrics metrics.Metrics,
) (*StreamingChunker, error) {
	cache, err := NewCache(size, blockSize, cachePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create file cache: %w", err)
	}

	return &StreamingChunker{
		size:     size,
		base:     base,
		cache:    cache,
		metrics:  metrics,
		fetchMap: make(map[int64]*fetchSession),
	}, nil
}

func (c *StreamingChunker) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	slice, err := c.Slice(ctx, off, int64(len(b)))
	if err != nil {
		return 0, fmt.Errorf("failed to slice cache at %d-%d: %w", off, off+int64(len(b)), err)
	}

	return copy(b, slice), nil
}

func (c *StreamingChunker) WriteTo(ctx context.Context, w io.Writer) (int64, error) {
	for i := int64(0); i < c.size; i += storage.MemoryChunkSize {
		chunk := make([]byte, storage.MemoryChunkSize)

		n, err := c.ReadAt(ctx, chunk, i)
		if err != nil {
			return 0, fmt.Errorf("failed to slice cache at %d-%d: %w", i, i+storage.MemoryChunkSize, err)
		}

		_, err = w.Write(chunk[:n])
		if err != nil {
			return 0, fmt.Errorf("failed to write chunk %d to writer: %w", i, err)
		}
	}

	return c.size, nil
}

func (c *StreamingChunker) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	timer := c.metrics.SlicesTimerFactory.Begin()

	// Fast path: already cached
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

	// Compute which 4MB chunks overlap with the requested range
	firstChunkOff := (off / storage.MemoryChunkSize) * storage.MemoryChunkSize
	lastChunkOff := ((off + length - 1) / storage.MemoryChunkSize) * storage.MemoryChunkSize

	var eg errgroup.Group

	for fetchOff := firstChunkOff; fetchOff <= lastChunkOff; fetchOff += storage.MemoryChunkSize {
		eg.Go(func() error {
			// Clip request to this chunk's boundaries
			chunkEnd := fetchOff + storage.MemoryChunkSize
			clippedOff := max(off, fetchOff)
			clippedEnd := min(off+length, chunkEnd, c.size)
			clippedLen := clippedEnd - clippedOff

			if clippedLen <= 0 {
				return nil
			}

			session := c.getOrCreateSession(fetchOff)
			return session.registerAndWait(ctx, clippedOff, clippedLen)
		})
	}

	if err := eg.Wait(); err != nil {
		timer.Failure(ctx, length,
			attribute.String(pullType, pullTypeRemote),
			attribute.String(failureReason, failureTypeCacheFetch))
		return nil, fmt.Errorf("failed to ensure data at %d-%d: %w", off, off+length, err)
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

func (c *StreamingChunker) getOrCreateSession(fetchOff int64) *fetchSession {
	s := &fetchSession{
		chunkOff: fetchOff,
		chunkLen: min(int64(storage.MemoryChunkSize), c.size-fetchOff),
		cache:    c.cache,
		state:    fetchStateRunning,
	}

	c.fetchMu.Lock()
	if existing, ok := c.fetchMap[fetchOff]; ok {
		c.fetchMu.Unlock()

		return existing
	}
	c.fetchMap[fetchOff] = s
	c.fetchMu.Unlock()

	go c.runFetch(s)

	return s
}

func (c *StreamingChunker) runFetch(s *fetchSession) {
	defer func() {
		c.fetchMu.Lock()
		delete(c.fetchMap, s.chunkOff)
		c.fetchMu.Unlock()
	}()

	mmapSlice, releaseLock, err := c.cache.addressBytes(s.chunkOff, s.chunkLen)
	if err != nil {
		s.mu.Lock()
		s.state = fetchStateErrored
		s.fetchErr = err
		s.notifyWaiters(err)
		s.mu.Unlock()

		return
	}
	defer releaseLock()

	fetchTimer := c.metrics.RemoteReadsTimerFactory.Begin()

	err = c.progressiveRead(s, mmapSlice)

	if err != nil {
		fetchTimer.Failure(context.Background(), s.chunkLen,
			attribute.String(failureReason, failureTypeRemoteRead))

		s.mu.Lock()
		s.state = fetchStateErrored
		s.fetchErr = err
		s.notifyWaiters(err)
		s.mu.Unlock()

		return
	}

	fetchTimer.Success(context.Background(), s.chunkLen)

	s.mu.Lock()
	s.state = fetchStateDone
	s.notifyWaiters(nil)
	s.mu.Unlock()
}

func (c *StreamingChunker) progressiveRead(s *fetchSession, mmapSlice []byte) error {
	// Use background context so the fetch continues even if the original caller cancels
	reader, err := c.base.OpenRangeReader(context.Background(), s.chunkOff, s.chunkLen)
	if err != nil {
		return fmt.Errorf("failed to open range reader at %d: %w", s.chunkOff, err)
	}
	defer reader.Close()

	blockSize := c.cache.BlockSize()
	var totalRead int64
	var prevCompleted int64

	for totalRead < s.chunkLen {
		// Cap each Read to blockSize so the HTTP/GCS client returns after each
		// block rather than buffering the entire remaining range.
		readEnd := min(totalRead+blockSize, s.chunkLen)
		n, readErr := reader.Read(mmapSlice[totalRead:readEnd])
		totalRead += int64(n)

		completedBlocks := totalRead / blockSize
		if completedBlocks > prevCompleted {
			newBytes := (completedBlocks - prevCompleted) * blockSize
			c.cache.setIsCached(s.chunkOff+prevCompleted*blockSize, newBytes)
			prevCompleted = completedBlocks

			s.mu.Lock()
			s.notifyWaiters(nil)
			s.mu.Unlock()
		}

		if errors.Is(readErr, io.EOF) {
			// Mark final partial block if any
			if totalRead > prevCompleted*blockSize {
				c.cache.setIsCached(s.chunkOff+prevCompleted*blockSize, totalRead-prevCompleted*blockSize)

				s.mu.Lock()
				s.notifyWaiters(nil)
				s.mu.Unlock()
			}
			break
		}

		if readErr != nil {
			return fmt.Errorf("failed reading at offset %d after %d bytes: %w", s.chunkOff, totalRead, readErr)
		}
	}

	return nil
}



func (c *StreamingChunker) Close() error {
	return c.cache.Close()
}

func (c *StreamingChunker) FileSize() (int64, error) {
	return c.cache.FileSize()
}
