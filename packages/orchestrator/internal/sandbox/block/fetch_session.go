package block

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
)

type rangeWaiter struct {
	// endByte: relative to session start, block-aligned (except at tail).
	endByte int64
	ch      chan error // buffered cap 1
}

// fetchSession coordinates concurrent waiters for a single fetch unit
// (a 4 MB chunk or a compressed frame). The fetch goroutine calls
// advance/setDone/setError; callers block on registerAndWait.
type fetchSession struct {
	mu        sync.Mutex
	chunkOff  int64 // absolute start offset in U-space
	chunkLen  int64 // total length of this chunk/frame
	blockSize int64 // progress tracking granularity

	waiters  []*rangeWaiter // sorted by endByte ascending
	fetchErr error

	// bytesReady is the byte count (from chunkOff) up to which all blocks
	// are fully written and marked cached. Atomic so registerAndWait can do
	// a lock-free fast-path check: bytesReady only increases, so a
	// Load() >= endByte guarantees data availability without taking the mutex.
	bytesReady atomic.Int64

	// isCachedFn checks persistent cache for data from previous sessions.
	isCachedFn func(off, length int64) bool
}

// terminated reports whether the session reached a terminal state
// (done or errored). Must be called with mu held.
func (s *fetchSession) terminated() bool {
	return s.fetchErr != nil || s.bytesReady.Load() == s.chunkLen
}

func newFetchSession(chunkOff, chunkLen, blockSize int64, isCachedFn func(off, length int64) bool) *fetchSession {
	return &fetchSession{
		chunkOff:   chunkOff,
		chunkLen:   chunkLen,
		blockSize:  blockSize,
		isCachedFn: isCachedFn,
	}
}

// registerAndWait blocks until [off, off+length) is cached or ctx is cancelled.
func (s *fetchSession) registerAndWait(ctx context.Context, off, length int64) error {
	relEnd := off + length - s.chunkOff
	var endByte int64
	if s.blockSize > 0 {
		lastBlockIdx := (relEnd - 1) / s.blockSize
		endByte = min((lastBlockIdx+1)*s.blockSize, s.chunkLen)
	} else {
		endByte = s.chunkLen
	}

	// Lock-free fast path: bytesReady only increases, so >= endByte
	// guarantees data is available without taking the mutex.
	if s.bytesReady.Load() >= endByte {
		return nil
	}

	s.mu.Lock()

	// Re-check under lock.
	if s.bytesReady.Load() >= endByte {
		s.mu.Unlock()

		return nil
	}

	// Terminal but range not covered — only happens on error
	// (setDone sets bytesReady=chunkLen). Check cache for prior session data.
	if s.terminated() {
		fetchErr := s.fetchErr
		s.mu.Unlock()

		if s.isCachedFn != nil && s.isCachedFn(off, length) {
			return nil
		}

		if fetchErr != nil {
			return fmt.Errorf("fetch failed: %w", fetchErr)
		}

		return nil
	}

	// Fetch in progress — register waiter.
	w := &rangeWaiter{endByte: endByte, ch: make(chan error, 1)}
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

// advance updates progress and wakes satisfied waiters.
func (s *fetchSession) advance(bytesReady int64) {
	s.mu.Lock()
	s.bytesReady.Store(bytesReady)
	s.notifyWaiters(nil)
	s.mu.Unlock()
}

// setDone marks the session as successfully completed.
func (s *fetchSession) setDone() {
	s.mu.Lock()
	s.bytesReady.Store(s.chunkLen)
	s.notifyWaiters(nil)
	s.mu.Unlock()
}

// setError records the error and wakes remaining waiters.
// When onlyIfRunning is true, a no-op if the session already terminated
// (used for panic recovery to avoid overriding a successful completion).
func (s *fetchSession) setError(err error, onlyIfRunning bool) {
	s.mu.Lock()
	if onlyIfRunning && s.terminated() {
		s.mu.Unlock()

		return
	}

	s.fetchErr = err
	s.notifyWaiters(err)
	s.mu.Unlock()
}

// notifyWaiters releases satisfied waiters. Must be called with mu held.
func (s *fetchSession) notifyWaiters(sendErr error) {
	ready := s.bytesReady.Load()

	// Terminal: notify every remaining waiter.
	if s.terminated() {
		for _, w := range s.waiters {
			if sendErr != nil && w.endByte > ready {
				w.ch <- sendErr
			}

			close(w.ch)
		}

		s.waiters = nil

		return
	}

	// Progress: pop satisfied waiters from the sorted front.
	i := 0
	for i < len(s.waiters) && s.waiters[i].endByte <= ready {
		close(s.waiters[i].ch)
		i++
	}

	s.waiters = s.waiters[i:]
}
