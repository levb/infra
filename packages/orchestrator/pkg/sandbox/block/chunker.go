package block

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const chunkSize = storage.MemoryChunkSize // 4 MB

var (
	cMeter     = otel.Meter("e2b/block/chunker")
	sliceTimer = utils.Must(telemetry.NewTimerFactory(cMeter,
		"orchestrator.blocks.slices",
		"Time taken to retrieve memory slices",
		"Total bytes requested", "Total page faults"))
	fetchTimer = utils.Must(telemetry.NewTimerFactory(cMeter,
		"orchestrator.blocks.chunks.fetch",
		"Time taken to fetch chunks from remote store",
		"Total bytes fetched", "Total remote fetches"))

	attrHit      = telemetry.PrecomputeAttrs(telemetry.Success, attribute.String("pull-type", "local"))
	attrMiss     = telemetry.PrecomputeAttrs(telemetry.Success, attribute.String("pull-type", "remote"))
	attrReadErr  = telemetry.PrecomputeAttrs(telemetry.Failure, attribute.String("pull-type", "local"), attribute.String("failure-reason", "local-read"))
	attrFetchErr = telemetry.PrecomputeAttrs(telemetry.Failure, attribute.String("pull-type", "remote"), attribute.String("failure-reason", "cache-fetch"))
	attrReread   = telemetry.PrecomputeAttrs(telemetry.Failure, attribute.String("pull-type", "local"), attribute.String("failure-reason", "local-read-again"))
	attrFillOK   = telemetry.PrecomputeAttrs(telemetry.Success)
	attrFillErr  = telemetry.PrecomputeAttrs(telemetry.Failure, attribute.String("failure-reason", "remote-read"))
)

// A fetch tracks one in-progress 4 MB chunk read. Waiters block on sig,
// which is closed-and-replaced each time pos advances — a broadcast
// primitive that composes with select for context cancellation.
type fetch struct {
	mu  sync.Mutex
	pos int64         // bytes from chunk start written to mmap
	err error         // non-nil after failure
	sig chan struct{} // closed on every advance / completion / failure
}

func (f *fetch) waitFor(ctx context.Context, need int64) error {
	for {
		f.mu.Lock()
		p, e, ch := f.pos, f.err, f.sig
		f.mu.Unlock()

		if p >= need {
			return nil
		}
		if e != nil {
			return e
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *fetch) advance(n int64) {
	f.mu.Lock()
	old := f.sig
	f.pos = n
	f.sig = make(chan struct{})
	f.mu.Unlock()
	close(old)
}

func (f *fetch) done(n int64) {
	f.mu.Lock()
	old := f.sig
	f.pos = n
	f.mu.Unlock()
	close(old)
}

func (f *fetch) fail(err error) {
	f.mu.Lock()
	if f.err != nil { // already failed (e.g. panic recovery after normal error)
		f.mu.Unlock()
		return
	}
	old := f.sig
	f.err = err
	f.mu.Unlock()
	close(old)
}

// Chunker is a read cache for a remote file. It mmaps a local sparse file
// and fills it on demand in 4 MB aligned chunks via streaming reads.
// Concurrent readers of the same chunk share one fetch goroutine with
// block-granularity progressive notification.
type Chunker struct {
	size      int64
	blockSize int64
	cache     *Cache
	upstream  storage.StreamingReader

	mu       sync.Mutex
	inflight map[int64]*fetch
}

func NewChunker(size, blockSize int64, upstream storage.StreamingReader, cachePath string) (*Chunker, error) {
	cache, err := NewCache(size, blockSize, cachePath, false)
	if err != nil {
		return nil, err
	}
	return &Chunker{
		size:      size,
		blockSize: blockSize,
		cache:     cache,
		upstream:  upstream,
		inflight:  make(map[int64]*fetch),
	}, nil
}

func (c *Chunker) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	end := min(off+length, c.size)
	t := sliceTimer.Begin()

	// Fast path: all requested bytes are in the bitmap.
	b, err := c.cache.Slice(off, end-off)
	if err == nil {
		t.RecordRaw(ctx, length, attrHit)
		return b, nil
	}
	if !errors.Is(err, ErrNotCached) {
		t.RecordRaw(ctx, length, attrReadErr)
		return nil, err
	}

	// Ensure each overlapping 4 MB chunk is being fetched.
	// Sequential: a Slice almost never spans two chunks, and when it does
	// the second iteration is typically a cache hit.
	for base := (off / chunkSize) * chunkSize; base < end; base += chunkSize {
		n := min(chunkSize, c.size-base)

		c.mu.Lock()
		f, ok := c.inflight[base]
		if !ok {
			if c.cache.isCached(base, n) {
				c.mu.Unlock()
				continue
			}
			f = &fetch{sig: make(chan struct{})}
			c.inflight[base] = f
			go c.fill(context.WithoutCancel(ctx), f, base, n)
		}
		c.mu.Unlock()

		hi := min(end, base+chunkSize)
		need := hi - base
		if err := f.waitFor(ctx, need); err != nil {
			t.RecordRaw(ctx, length, attrFetchErr)
			return nil, err
		}
	}

	// Data is in the mmap; the waiter confirmed it, so skip the bitmap check.
	b, err = c.cache.sliceDirect(off, end-off)
	if err != nil {
		t.RecordRaw(ctx, length, attrReread)
		return nil, err
	}
	t.RecordRaw(ctx, length, attrMiss)
	return b, nil
}

func (c *Chunker) ReadAt(ctx context.Context, b []byte, off int64) (int, error) {
	s, err := c.Slice(ctx, off, int64(len(b)))
	if err != nil {
		return 0, err
	}
	return copy(b, s), nil
}

func (c *Chunker) WriteTo(ctx context.Context, w io.Writer) (int64, error) {
	buf := make([]byte, chunkSize)
	for off := int64(0); off < c.size; off += chunkSize {
		n, err := c.ReadAt(ctx, buf, off)
		if err != nil {
			return 0, err
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return 0, err
		}
	}
	return c.size, nil
}

func (c *Chunker) Close() error   { return c.cache.Close() }
func (c *Chunker) FileSize() (int64, error) { return c.cache.FileSize() }

// fill streams data from upstream into the mmap, notifying waiters at
// block boundaries. Runs as a detached goroutine so one caller's context
// cancellation doesn't kill the fetch for everyone.
func (c *Chunker) fill(ctx context.Context, f *fetch, off, n int64) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	defer func() {
		c.mu.Lock()
		delete(c.inflight, off)
		c.mu.Unlock()
	}()
	defer func() {
		if r := recover(); r != nil {
			logger.L().Error(ctx, "panic in chunk fetch", zap.Any("error", r))
			f.fail(fmt.Errorf("panic: %v", r))
		}
	}()

	dst, unlock, err := c.cache.addressBytes(off, n)
	if err != nil {
		f.fail(err)
		return
	}
	defer unlock()

	t := fetchTimer.Begin()

	r, err := c.upstream.OpenRangeReader(ctx, off, n)
	if err != nil {
		t.RecordRaw(ctx, n, attrFillErr)
		f.fail(err)
		return
	}
	defer r.Close()

	batch := max(c.blockSize, 16*1024) // min 16 KB per read
	var total int64
	var notified int64

	for total < n {
		nr, readErr := r.Read(dst[total:min(total+batch, n)])
		total += int64(nr)

		blocks := (total / c.blockSize) * c.blockSize
		if blocks > notified {
			notified = blocks
			f.advance(blocks)
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.RecordRaw(ctx, n, attrFillErr)
			f.fail(readErr)
			return
		}
	}

	// Mark cached BEFORE removing from inflight — closes the TOCTOU window
	// where a new Slice could miss both the inflight map and the bitmap.
	c.cache.setIsCached(off, n)
	t.RecordRaw(ctx, n, attrFillOK)
	f.done(n)
}
