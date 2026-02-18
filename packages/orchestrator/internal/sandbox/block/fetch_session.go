package block

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
)

type rangeWaiter struct {
	// endByte: relative to session start, block-aligned (except at tail).
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
// (a 4 MB chunk or a compressed frame). The fetch goroutine calls
// advance/complete/fail; callers block on registerAndWait.
type fetchSession struct {
	mu        sync.Mutex
	chunkOff  int64 // absolute start offset in U-space
	chunkLen  int64 // total length of this chunk/frame
	blockSize int64 // progress tracking granularity

	waiters    []*rangeWaiter // sorted by endByte ascending
	state      fetchState
	fetchErr   error
	bytesReady int64

	// isCachedFn checks persistent cache for data from previous sessions.
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

		return nil
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

// advance updates progress and wakes satisfied waiters.
func (s *fetchSession) advance(bytesReady int64) {
	s.mu.Lock()
	s.bytesReady = bytesReady
	s.notifyWaiters(nil)
	s.mu.Unlock()
}

// complete wakes all remaining waiters with success.
func (s *fetchSession) complete() {
	s.mu.Lock()
	s.state = fetchStateDone
	s.bytesReady = s.chunkLen
	s.notifyWaiters(nil)
	s.mu.Unlock()
}

// fail wakes remaining waiters. Already-satisfied waiters still get nil.
func (s *fetchSession) fail(err error) {
	s.mu.Lock()
	if s.state == fetchStateRunning {
		s.state = fetchStateErrored
		s.fetchErr = err
		s.notifyWaiters(err)
	}
	s.mu.Unlock()
}

// notifyWaiters releases satisfied waiters. Must be called with s.mu held.
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
