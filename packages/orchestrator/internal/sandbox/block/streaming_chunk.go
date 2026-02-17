package block

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const (
	// defaultFetchTimeout is the maximum time a single 4MB chunk fetch may take.
	// Acts as a safety net: if the upstream hangs, the goroutine won't live forever.
	defaultFetchTimeout = 60 * time.Second
)

type rangeWaiter struct {
	// endByte is the byte offset (relative to chunkOff) at which this waiter's
	// entire requested range is cached. Equal to the end of the last block
	// overlapping the requested range. Always a multiple of blockSize.
	endByte int64
	ch      chan error // buffered cap 1
}

type fetchState int

const (
	fetchStateRunning = fetchState(iota)
	fetchStateDone
	fetchStateErrored
)

type fetchSession struct {
	mu       sync.Mutex
	chunkOff int64
	chunkLen int64
	cache    *Cache
	waiters  []*rangeWaiter // sorted by endByte ascending
	state    fetchState
	fetchErr error

	// bytesReady is the byte count (from chunkOff) up to which all blocks are
	// fully written to mmap and marked cached. Always a multiple of blockSize
	// during progressive reads. Used to cheaply determine which sorted waiters
	// are satisfied without calling isCached.
	bytesReady int64
}

// registerAndWait adds a waiter for the given range and blocks until the range
// is cached or the context is cancelled. Returns nil if the range was already
// cached before registering.
func (s *fetchSession) registerAndWait(ctx context.Context, off, length int64) error {
	blockSize := s.cache.BlockSize()
	// endByte is the byte offset (relative to chunkOff) past which all blocks
	// covering [off, off+length) are fully cached.
	lastBlockIdx := (off + length - 1 - s.chunkOff) / blockSize
	endByte := (lastBlockIdx + 1) * blockSize

	// Fast path: already cached (handles pre-existing cache from prior sessions).
	if s.cache.isCached(off, length) {
		return nil
	}

	s.mu.Lock()
	if s.state == fetchStateDone {
		s.mu.Unlock()
		if s.cache.isCached(off, length) {
			return nil
		}

		return fmt.Errorf("fetch completed but range %d-%d not cached", off, off+length)
	}

	// Session errored — partial data may still be usable.
	if s.state == fetchStateErrored {
		fetchErr := s.fetchErr
		s.mu.Unlock()
		if s.cache.isCached(off, length) {
			return nil
		}

		return fmt.Errorf("fetch failed: %w", fetchErr)
	}

	w := &rangeWaiter{
		endByte: endByte,
		ch:      make(chan error, 1),
	}

	// Insert in sorted order so notifyWaiters can iterate front-to-back.
	idx, _ := slices.BinarySearchFunc(s.waiters, endByte, func(w *rangeWaiter, target int64) int {
		return cmp.Compare(w.endByte, target)
	})
	s.waiters = slices.Insert(s.waiters, idx, w)

	s.mu.Unlock()

	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// notifyWaiters notifies waiters whose ranges are satisfied.
//
// Because waiters are sorted by endByte and the fetch fills the chunk
// sequentially, we only need to walk from the front until we hit a waiter
// whose endByte exceeds bytesReady — all subsequent waiters are unsatisfied.
//
// In terminal states (done/errored) all remaining waiters are notified.
// Must be called with s.mu held.
func (s *fetchSession) notifyWaiters(sendErr error) {
	// Terminal: notify every remaining waiter.
	if s.state != fetchStateRunning {
		for _, w := range s.waiters {
			if sendErr != nil && w.endByte > s.bytesReady {
				w.ch <- sendErr
			}
			close(w.ch)
		}
		s.waiters = nil

		return
	}

	// Progress: pop satisfied waiters from the sorted front.
	i := 0
	for i < len(s.waiters) && s.waiters[i].endByte <= s.bytesReady {
		close(s.waiters[i].ch)
		i++
	}
	s.waiters = s.waiters[i:]
}

type StreamingChunker struct {
	upstream     storage.StreamingReader
	cache        *Cache
	metrics      metrics.Metrics
	fetchTimeout time.Duration

	size int64

	fetchMu  sync.Mutex
	fetchMap map[int64]*fetchSession
}

func NewStreamingChunker(
	size, blockSize int64,
	upstream storage.StreamingReader,
	cachePath string,
	metrics metrics.Metrics,
) (*StreamingChunker, error) {
	cache, err := NewCache(size, blockSize, cachePath, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create file cache: %w", err)
	}

	return &StreamingChunker{
		size:         size,
		upstream:     upstream,
		cache:        cache,
		metrics:      metrics,
		fetchTimeout: defaultFetchTimeout,
		fetchMap:     make(map[int64]*fetchSession),
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
	chunk := make([]byte, storage.MemoryChunkSize)

	for i := int64(0); i < c.size; i += storage.MemoryChunkSize {
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
	firstChunkOff := header.BlockOffset(header.BlockIdx(off, storage.MemoryChunkSize), storage.MemoryChunkSize)
	lastChunkOff := header.BlockOffset(header.BlockIdx(off+length-1, storage.MemoryChunkSize), storage.MemoryChunkSize)

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

			session := c.getOrCreateSession(ctx, fetchOff)

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

func (c *StreamingChunker) getOrCreateSession(ctx context.Context, fetchOff int64) *fetchSession {
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

	// Detach from the caller's cancel signal so the shared fetch goroutine
	// continues even if the first caller's context is cancelled. Trace/value
	// context is preserved for metrics.
	go c.runFetch(context.WithoutCancel(ctx), s)

	return s
}

func (s *fetchSession) setState(state fetchState, err error, onlyIfRunning bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !onlyIfRunning || s.state == fetchStateRunning {
		s.state = state
		s.fetchErr = err
		s.notifyWaiters(err)
	}
}

func (c *StreamingChunker) runFetch(ctx context.Context, s *fetchSession) {
	ctx, cancel := context.WithTimeout(ctx, c.fetchTimeout)
	defer cancel()

	defer func() {
		c.fetchMu.Lock()
		delete(c.fetchMap, s.chunkOff)
		c.fetchMu.Unlock()
	}()

	// Panic recovery: ensure waiters are always notified even if the fetch
	// goroutine panics (e.g. nil pointer in upstream reader, mmap fault).
	// Without this, waiters would block forever on their channels.
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("fetch panicked: %v", r)
			s.setState(fetchStateErrored, err, true)
		}
	}()

	mmapSlice, releaseLock, err := c.cache.addressBytes(s.chunkOff, s.chunkLen)
	if err != nil {
		s.setState(fetchStateErrored, err, false)

		return
	}
	defer releaseLock()

	fetchTimer := c.metrics.RemoteReadsTimerFactory.Begin()

	err = c.progressiveRead(ctx, s, mmapSlice)
	if err != nil {
		fetchTimer.Failure(ctx, s.chunkLen,
			attribute.String(failureReason, failureTypeRemoteRead))

		s.setState(fetchStateErrored, err, false)

		return
	}

	fetchTimer.Success(ctx, s.chunkLen)
	s.setState(fetchStateDone, nil, false)
}

func (c *StreamingChunker) progressiveRead(ctx context.Context, s *fetchSession, mmapSlice []byte) error {
	reader, err := c.upstream.OpenRangeReader(ctx, s.chunkOff, s.chunkLen)
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
			s.bytesReady = completedBlocks * blockSize
			s.notifyWaiters(nil)
			s.mu.Unlock()
		}

		if errors.Is(readErr, io.EOF) {
			// Mark final partial block if any
			if totalRead > prevCompleted*blockSize {
				c.cache.setIsCached(s.chunkOff+prevCompleted*blockSize, totalRead-prevCompleted*blockSize)
			}
			// Remaining waiters are notified in runFetch via the Done state.
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
