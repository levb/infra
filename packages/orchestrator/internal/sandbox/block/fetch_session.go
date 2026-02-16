package block

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
)

// rangeWaiter represents a goroutine waiting for a byte range to become available.
type rangeWaiter struct {
	// endByte is the byte offset (relative to session start) at which this
	// waiter's entire requested range is cached. Always aligned to blockSize
	// (unless it's the tail of the chunk).
	endByte int64
	ch      chan error // buffered cap 1
}

type fetchState int

const (
	fetchStateRunning = fetchState(iota)
	fetchStateDone
	fetchStateErrored
)

// fetchSession coordinates concurrent waiters for a single fetch unit
// (a 4MB chunk for uncompressed data, or a compressed frame for the
// compressed chunkers). Waiters are progressively released as data
// becomes available.
//
// Usage from the fetch goroutine:
//   - Call advance(bytesReady) as blocks of data become available.
//   - Call complete() when all data is ready.
//   - Call fail(err) on errors. Waiters whose range is already within
//     bytesReady still succeed; others receive the error.
//
// Usage from callers:
//   - Call registerAndWait(ctx, off, length) to block until the range
//     is available. Returns nil immediately if already satisfied.
type fetchSession struct {
	mu        sync.Mutex
	chunkOff  int64 // absolute start offset in U-space
	chunkLen  int64 // total length of this chunk/frame
	blockSize int64 // progress tracking granularity

	waiters    []*rangeWaiter // sorted by endByte ascending
	state      fetchState
	fetchErr   error
	bytesReady int64

	// isCachedFn optionally checks if a range is already available from a
	// persistent cache (e.g., mmap). This handles data cached by previous
	// sessions that have since been cleaned up. May be nil.
	isCachedFn func(off, length int64) bool
}

func newFetchSession(chunkOff, chunkLen, blockSize int64, isCachedFn func(off, length int64) bool) *fetchSession {
	return &fetchSession{
		chunkOff:   chunkOff,
		chunkLen:   chunkLen,
		blockSize:  blockSize,
		state:      fetchStateRunning,
		isCachedFn: isCachedFn,
	}
}

// registerAndWait blocks until the requested range is available or the
// context is cancelled. Returns nil immediately if the range is already
// within bytesReady or isCachedFn reports it as cached.
func (s *fetchSession) registerAndWait(ctx context.Context, off, length int64) error {
	// Compute the endByte threshold for this waiter (relative to chunkOff).
	relEnd := off + length - s.chunkOff
	var endByte int64
	if s.blockSize > 0 {
		lastBlockIdx := (relEnd - 1) / s.blockSize
		endByte = min((lastBlockIdx+1)*s.blockSize, s.chunkLen)
	} else {
		endByte = s.chunkLen
	}

	// Fast path: persistent cache from a previous session.
	if s.isCachedFn != nil && s.isCachedFn(off, length) {
		return nil
	}

	s.mu.Lock()

	if s.state == fetchStateDone {
		s.mu.Unlock()
		// Double-check persistent cache in case of races.
		if s.isCachedFn != nil && s.isCachedFn(off, length) {
			return nil
		}

		return nil // done means all data is available
	}

	// Session errored — partial data may still be usable.
	if s.state == fetchStateErrored {
		fetchErr := s.fetchErr
		ready := s.bytesReady
		s.mu.Unlock()

		if ready >= endByte {
			return nil
		}

		if s.isCachedFn != nil && s.isCachedFn(off, length) {
			return nil
		}

		return fmt.Errorf("fetch failed: %w", fetchErr)
	}

	// Already enough progress.
	if s.bytesReady >= endByte {
		s.mu.Unlock()

		return nil
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

// advance updates bytesReady and releases waiters whose ranges are now satisfied.
// Must be called from the fetch goroutine as data becomes available.
func (s *fetchSession) advance(bytesReady int64) {
	s.mu.Lock()
	s.bytesReady = bytesReady
	s.notifyWaiters(nil)
	s.mu.Unlock()
}

// complete marks the session as done and releases all remaining waiters.
func (s *fetchSession) complete() {
	s.mu.Lock()
	s.state = fetchStateDone
	s.bytesReady = s.chunkLen
	s.notifyWaiters(nil)
	s.mu.Unlock()
}

// fail marks the session as errored and releases remaining waiters with the error.
// Waiters whose ranges were already satisfied receive nil (success).
func (s *fetchSession) fail(err error) {
	s.mu.Lock()
	if s.state == fetchStateRunning {
		s.state = fetchStateErrored
		s.fetchErr = err
		s.notifyWaiters(err)
	}
	s.mu.Unlock()
}

// notifyWaiters releases waiters whose ranges are satisfied.
// In terminal states all remaining waiters are notified.
// Must be called with s.mu held.
func (s *fetchSession) notifyWaiters(sendErr error) {
	// Terminal: notify every remaining waiter.
	if s.state != fetchStateRunning {
		for _, w := range s.waiters {
			if sendErr != nil && w.endByte > s.bytesReady {
				w.ch <- sendErr
			} else {
				w.ch <- nil
			}
		}
		s.waiters = nil

		return
	}

	// Progress: pop satisfied waiters from the sorted front.
	i := 0
	for i < len(s.waiters) && s.waiters[i].endByte <= s.bytesReady {
		s.waiters[i].ch <- nil
		i++
	}
	s.waiters = s.waiters[i:]
}
