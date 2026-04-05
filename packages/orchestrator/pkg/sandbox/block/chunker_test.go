package block

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const testBlockSize = header.PageSize // 4 KB

func makeTestData(t testing.TB, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

// --- Test upstreams ---

// memUpstream serves data from memory with optional per-Read delay.
type memUpstream struct {
	data      []byte
	blockSize int64
	delay     time.Duration
}

func (u *memUpstream) OpenRangeReader(_ context.Context, off, length int64) (io.ReadCloser, error) {
	end := min(off+length, int64(len(u.data)))
	return &blockReader{data: u.data[off:end], blockSize: int(u.blockSize), delay: u.delay}, nil
}

type blockReader struct {
	data      []byte
	pos       int
	blockSize int
	delay     time.Duration
}

func (r *blockReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	end := min(r.pos+r.blockSize, len(r.data))
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

func (r *blockReader) Close() error { return nil }

// countingUpstream wraps another upstream and counts OpenRangeReader calls.
type countingUpstream struct {
	inner *memUpstream
	count atomic.Int64
}

func (c *countingUpstream) OpenRangeReader(ctx context.Context, off, length int64) (io.ReadCloser, error) {
	c.count.Add(1)
	return c.inner.OpenRangeReader(ctx, off, length)
}

// failingUpstream returns an error from OpenRangeReader when failErr is set.
type failingUpstream struct {
	data      []byte
	failCount atomic.Int32
	failErr   error
}

func (u *failingUpstream) OpenRangeReader(_ context.Context, off, length int64) (io.ReadCloser, error) {
	if u.failErr != nil {
		u.failCount.Add(1)
		return nil, u.failErr
	}
	end := min(off+length, int64(len(u.data)))
	return io.NopCloser(bytes.NewReader(u.data[off:end])), nil
}

// errorAfterNUpstream fails mid-stream after delivering failAfter bytes.
type errorAfterNUpstream struct {
	data      []byte
	failAfter int64
	blockSize int64
}

func (u *errorAfterNUpstream) OpenRangeReader(_ context.Context, off, length int64) (io.ReadCloser, error) {
	end := min(off+length, int64(len(u.data)))
	return &errorAfterNReader{data: u.data[off:end], blockSize: int(u.blockSize), failAfter: int(u.failAfter - off)}, nil
}

type errorAfterNReader struct {
	data      []byte
	pos       int
	blockSize int
	failAfter int
}

func (r *errorAfterNReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	if r.pos >= r.failAfter {
		return 0, fmt.Errorf("simulated upstream error")
	}
	end := min(r.pos+r.blockSize, len(r.data))
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

func (r *errorAfterNReader) Close() error { return nil }

// panicUpstream panics mid-stream after delivering panicAfter bytes.
type panicUpstream struct {
	data       []byte
	blockSize  int64
	panicAfter int64
}

func (u *panicUpstream) OpenRangeReader(_ context.Context, off, length int64) (io.ReadCloser, error) {
	end := min(off+length, int64(len(u.data)))
	return &panicReader{data: u.data[off:end], blockSize: int(u.blockSize), panicAfter: int(u.panicAfter - off)}, nil
}

type panicReader struct {
	data       []byte
	pos        int
	blockSize  int
	panicAfter int
}

func (r *panicReader) Read(p []byte) (int, error) {
	if r.pos >= r.panicAfter {
		panic("simulated upstream panic")
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	end := min(r.pos+r.blockSize, len(r.data))
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	return n, nil
}

func (r *panicReader) Close() error { return nil }

// streamingFunc adapts a closure into storage.StreamingReader.
type streamingFunc func(ctx context.Context, off, length int64) (io.ReadCloser, error)

func (f streamingFunc) OpenRangeReader(ctx context.Context, off, length int64) (io.ReadCloser, error) {
	return f(ctx, off, length)
}

// --- Tests ---

func newChunker(t testing.TB, size int64, upstream storage.StreamingReader) *Chunker {
	t.Helper()
	c, err := NewChunker(size, testBlockSize, upstream, filepath.Join(t.TempDir(), "cache"))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func TestChunker_BasicSlice(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	c := newChunker(t, int64(len(data)), &memUpstream{data: data, blockSize: testBlockSize})

	s, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], s)
}

func TestChunker_CacheHit(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	up := &countingUpstream{inner: &memUpstream{data: data, blockSize: testBlockSize}}
	c := newChunker(t, int64(len(data)), up)

	_, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond) // let full chunk finish

	n := up.count.Load()
	require.Positive(t, n)

	s, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], s)
	assert.Equal(t, n, up.count.Load()) // no new fetches
}

func TestChunker_RetryAfterError(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	up := &failingUpstream{data: data, failErr: errors.New("boom")}
	c := newChunker(t, int64(len(data)), up)

	_, err := c.Slice(t.Context(), 0, testBlockSize)
	require.Error(t, err)
	require.Positive(t, up.failCount.Load())

	up.failErr = nil // clear
	s, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	assert.Equal(t, data[:testBlockSize], s)
}

func TestChunker_ConcurrentSameChunk(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	up := &countingUpstream{inner: &memUpstream{data: data, blockSize: testBlockSize}}
	c := newChunker(t, int64(len(data)), up)

	n := 10
	results := make([][]byte, n)
	var eg errgroup.Group
	for i := range n {
		eg.Go(func() error {
			s, err := c.Slice(t.Context(), 0, testBlockSize)
			if err != nil {
				return err
			}
			results[i] = append([]byte(nil), s...)
			return nil
		})
	}
	require.NoError(t, eg.Wait())
	for i := range n {
		assert.Equal(t, data[:testBlockSize], results[i])
	}
}

func TestChunker_DifferentChunks(t *testing.T) {
	t.Parallel()
	size := storage.MemoryChunkSize * 2
	data := makeTestData(t, size)
	c := newChunker(t, int64(size), &memUpstream{data: data, blockSize: testBlockSize})

	s0, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	assert.Equal(t, data[:testBlockSize], s0)

	off1 := int64(storage.MemoryChunkSize)
	s1, err := c.Slice(t.Context(), off1, testBlockSize)
	require.NoError(t, err)
	assert.Equal(t, data[off1:off1+testBlockSize], s1)
}

func TestChunker_FullChunkCachedAfterPartialRequest(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	up := &countingUpstream{inner: &memUpstream{data: data, blockSize: testBlockSize}}
	c := newChunker(t, int64(len(data)), up)

	_, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)

	lastOff := int64(storage.MemoryChunkSize) - testBlockSize
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	s, err := c.Slice(ctx, lastOff, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[lastOff:], s)
	assert.Equal(t, int64(1), up.count.Load())
}

func TestChunker_SharedSession(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	gate := make(chan struct{})
	opens := atomic.Int64{}

	up := streamingFunc(func(_ context.Context, off, length int64) (io.ReadCloser, error) {
		opens.Add(1)
		<-gate
		return io.NopCloser(bytes.NewReader(data[off:min(off+length, int64(len(data)))])), nil
	})
	c := newChunker(t, int64(len(data)), up)

	var eg errgroup.Group
	var sA, sB []byte
	eg.Go(func() error {
		s, err := c.Slice(t.Context(), 0, testBlockSize)
		sA = append([]byte(nil), s...)
		return err
	})
	eg.Go(func() error {
		s, err := c.Slice(t.Context(), int64(storage.MemoryChunkSize)-testBlockSize, testBlockSize)
		sB = append([]byte(nil), s...)
		return err
	})

	time.Sleep(10 * time.Millisecond)
	close(gate)
	require.NoError(t, eg.Wait())
	assert.Equal(t, data[:testBlockSize], sA)
	assert.Equal(t, data[storage.MemoryChunkSize-testBlockSize:], sB)
	assert.Equal(t, int64(1), opens.Load())
}

func TestChunker_EarlyReturn(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		blockSize int64
		delay     time.Duration
		blocks    []int
	}{
		{"hugepage", header.HugepageSize, 50 * time.Millisecond, []int{0, 1}},
		{"4K", header.PageSize, 100 * time.Microsecond, []int{1, 512, 1022}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := makeTestData(t, storage.MemoryChunkSize)
			gate := make(chan struct{})
			up := streamingFunc(func(_ context.Context, off, length int64) (io.ReadCloser, error) {
				<-gate
				return &blockReader{data: data[off:min(off+length, int64(len(data)))], blockSize: int(tc.blockSize), delay: tc.delay}, nil
			})

			c, err := NewChunker(int64(len(data)), tc.blockSize, up, filepath.Join(t.TempDir(), "cache"))
			require.NoError(t, err)
			defer c.Close()

			n := len(tc.blocks)
			order := make(chan int, n)
			var eg errgroup.Group
			for i, blk := range tc.blocks {
				off := int64(blk) * tc.blockSize
				eg.Go(func() error {
					_, err := c.Slice(t.Context(), off, tc.blockSize)
					if err != nil {
						return err
					}
					order <- i
					return nil
				})
			}
			time.Sleep(10 * time.Millisecond)
			close(gate)
			require.NoError(t, eg.Wait())
			close(order)

			var got []int
			for idx := range order {
				got = append(got, idx)
			}
			expected := make([]int, n)
			for i := range expected {
				expected[i] = i
			}
			assert.Equal(t, expected, got)
		})
	}
}

func TestChunker_ErrorKeepsPartialData(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	up := &errorAfterNUpstream{data: data, failAfter: int64(storage.MemoryChunkSize / 2), blockSize: testBlockSize}
	c := newChunker(t, int64(len(data)), up)

	_, err := c.Slice(t.Context(), int64(storage.MemoryChunkSize)-testBlockSize, testBlockSize)
	require.Error(t, err)

	s, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], s)
}

func TestChunker_ContextCancellation(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	up := &memUpstream{data: data, blockSize: testBlockSize, delay: 1 * time.Millisecond}
	c := newChunker(t, int64(len(data)), up)

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Millisecond)
	defer cancel()
	_, err := c.Slice(ctx, int64(storage.MemoryChunkSize)-testBlockSize, testBlockSize)
	require.Error(t, err)

	time.Sleep(200 * time.Millisecond)
	s, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], s)
}

func TestChunker_LastBlockPartial(t *testing.T) {
	t.Parallel()
	size := storage.MemoryChunkSize - 100
	data := makeTestData(t, size)
	c := newChunker(t, int64(size), &memUpstream{data: data, blockSize: testBlockSize})

	off := (int64(size) / testBlockSize) * testBlockSize
	s, err := c.Slice(t.Context(), off, int64(size)-off)
	require.NoError(t, err)
	require.Equal(t, data[off:], s)
}

func TestChunker_MultiChunkSlice(t *testing.T) {
	t.Parallel()
	size := storage.MemoryChunkSize * 2
	data := makeTestData(t, size)
	c := newChunker(t, int64(size), &memUpstream{data: data, blockSize: testBlockSize})

	off := int64(storage.MemoryChunkSize) - testBlockSize
	length := int64(testBlockSize * 2)
	s, err := c.Slice(t.Context(), off, length)
	require.NoError(t, err)
	require.Equal(t, data[off:off+length], s)
}

func TestChunker_PanicRecovery(t *testing.T) {
	t.Parallel()
	data := makeTestData(t, storage.MemoryChunkSize)
	up := &panicUpstream{data: data, blockSize: testBlockSize, panicAfter: int64(storage.MemoryChunkSize / 2)}
	c := newChunker(t, int64(len(data)), up)

	_, err := c.Slice(t.Context(), int64(storage.MemoryChunkSize)-testBlockSize, testBlockSize)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic")

	s, err := c.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], s)
}

// --- Benchmark ---

func BenchmarkChunkerSlice_CacheHit(b *testing.B) {
	provider := metric.NewMeterProvider()
	b.Cleanup(func() { provider.Shutdown(context.Background()) })

	c, err := NewChunker(
		int64(16384)*4096, 4096,
		nil, // never called on cache hit
		filepath.Join(b.TempDir(), "cache"),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { c.Close() })

	c.cache.setIsCached(0, c.size)
	ctx := context.Background()

	b.ResetTimer()
	for i := range b.N {
		off := int64(i%16384) * 4096
		s, err := c.Slice(ctx, off, 4096)
		if err != nil {
			b.Fatal(err)
		}
		if len(s) == 0 {
			b.Fatal("empty")
		}
	}
}
