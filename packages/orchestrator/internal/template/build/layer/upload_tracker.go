package layer

import (
	"context"
	"sync"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox"
)

// UploadTracker tracks in-flight uploads and allows waiting for all previous uploads to complete.
// This prevents race conditions where a layer's cache entry is saved before its
// dependencies (previous layers) are fully uploaded.
type UploadTracker struct {
	mu      sync.Mutex
	waitChs []chan struct{}

	// pending collects frame tables from compressed uploads across all layers
	pending *sandbox.PendingFrameTables

	// dataFileWg tracks in-flight data file uploads (phase 1).
	// All layers must complete their data uploads before any layer
	// can finalize compressed headers.
	dataFileWg sync.WaitGroup
}

func NewUploadTracker() *UploadTracker {
	return &UploadTracker{
		waitChs: make([]chan struct{}, 0),
		pending: &sandbox.PendingFrameTables{},
	}
}

// Pending returns the shared PendingFrameTables for collecting frame tables.
func (t *UploadTracker) Pending() *sandbox.PendingFrameTables {
	return t.pending
}

// StartDataFileUpload registers that a data file upload is starting.
// Returns a function that must be called when the data upload completes.
// Must be called even on error (use defer) to avoid deadlocking the WaitGroup.
func (t *UploadTracker) StartDataFileUpload() func() {
	t.dataFileWg.Add(1)

	return func() {
		t.dataFileWg.Done()
	}
}

// WaitForAllDataFileUploads blocks until all data file uploads have completed.
func (t *UploadTracker) WaitForAllDataFileUploads(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		t.dataFileWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StartUpload registers that a new upload has started.
// Returns a function that should be called when the upload completes.
func (t *UploadTracker) StartUpload() (complete func(), waitForPrevious func(context.Context) error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Create a channel for this upload
	ch := make(chan struct{})
	t.waitChs = append(t.waitChs, ch)

	// Capture the channels we need to wait for (all previous uploads)
	previousChs := make([]chan struct{}, len(t.waitChs)-1)
	copy(previousChs, t.waitChs[:len(t.waitChs)-1])

	complete = func() {
		close(ch)
	}

	waitForPrevious = func(ctx context.Context) error {
		for _, prevCh := range previousChs {
			select {
			case <-prevCh:
				// Previous upload completed
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	}

	return complete, waitForPrevious
}
