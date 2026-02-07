package block

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const (
	testBlockSize = header.PageSize // 4KB
)

// slowUpstream simulates GCS: implements both SeekableReader and StreamingReader.
// OpenRangeReader returns a reader that yields blockSize bytes per Read() call
// with a configurable delay between calls.
type slowUpstream struct {
	data      []byte
	blockSize int64
	delay     time.Duration
}

var (
	_ storage.SeekableReader  = (*slowUpstream)(nil)
	_ storage.StreamingReader = (*slowUpstream)(nil)
)

func (s *slowUpstream) ReadAt(_ context.Context, buffer []byte, off int64) (int, error) {
	end := min(off+int64(len(buffer)), int64(len(s.data)))
	n := copy(buffer, s.data[off:end])
	return n, nil
}

func (s *slowUpstream) Size(_ context.Context) (int64, error) {
	return int64(len(s.data)), nil
}

func (s *slowUpstream) OpenRangeReader(_ context.Context, off, length int64) (io.ReadCloser, error) {
	end := min(off+length, int64(len(s.data)))

	return &slowReader{
		data:      s.data[off:end],
		blockSize: int(s.blockSize),
		delay:     s.delay,
	}, nil
}

type slowReader struct {
	data      []byte
	pos       int
	blockSize int
	delay     time.Duration
}

func (r *slowReader) Read(p []byte) (int, error) {
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

func (r *slowReader) Close() error {
	return nil
}

// fastUpstream simulates NFS: same interfaces but no delay.
type fastUpstream = slowUpstream

// errorAfterNUpstream fails after reading n bytes.
type errorAfterNUpstream struct {
	data      []byte
	failAfter int64
	blockSize int64
}

var _ storage.StreamingReader = (*errorAfterNUpstream)(nil)

func (u *errorAfterNUpstream) OpenRangeReader(_ context.Context, off, length int64) (io.ReadCloser, error) {
	end := min(off+length, int64(len(u.data)))

	return &errorAfterNReader{
		data:      u.data[off:end],
		blockSize: int(u.blockSize),
		failAfter: int(u.failAfter - off),
	}, nil
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

func (r *errorAfterNReader) Close() error {
	return nil
}

func newTestMetrics(t *testing.T) metrics.Metrics {
	t.Helper()

	m, err := metrics.NewMetrics(noop.NewMeterProvider())
	require.NoError(t, err)

	return m
}

func makeTestData(t *testing.T, size int) []byte {
	t.Helper()

	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)

	return data
}

func TestStreamingChunker_BasicSlice(t *testing.T) {
	t.Parallel()

	data := makeTestData(t, storage.MemoryChunkSize)
	upstream := &fastUpstream{data: data, blockSize: testBlockSize}

	chunker, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	// Read first page
	slice, err := chunker.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice)
}

func TestStreamingChunker_CacheHit(t *testing.T) {
	t.Parallel()

	data := makeTestData(t, storage.MemoryChunkSize)
	readCount := atomic.Int64{}

	upstream := &countingUpstream{
		inner:     &fastUpstream{data: data, blockSize: testBlockSize},
		readCount: &readCount,
	}

	chunker, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	// First read: triggers fetch
	_, err = chunker.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)

	// Wait for the full chunk to be fetched
	time.Sleep(50 * time.Millisecond)

	firstCount := readCount.Load()
	require.Greater(t, firstCount, int64(0))

	// Second read: should hit cache
	slice, err := chunker.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice)

	// No additional reads should have happened
	assert.Equal(t, firstCount, readCount.Load())
}

type countingUpstream struct {
	inner     *fastUpstream
	readCount *atomic.Int64
}

var (
	_ storage.SeekableReader  = (*countingUpstream)(nil)
	_ storage.StreamingReader = (*countingUpstream)(nil)
)

func (c *countingUpstream) ReadAt(ctx context.Context, buffer []byte, off int64) (int, error) {
	c.readCount.Add(1)
	return c.inner.ReadAt(ctx, buffer, off)
}

func (c *countingUpstream) Size(ctx context.Context) (int64, error) {
	return c.inner.Size(ctx)
}

func (c *countingUpstream) OpenRangeReader(ctx context.Context, off, length int64) (io.ReadCloser, error) {
	c.readCount.Add(1)
	return c.inner.OpenRangeReader(ctx, off, length)
}

func TestStreamingChunker_ConcurrentSameChunk(t *testing.T) {
	t.Parallel()

	data := makeTestData(t, storage.MemoryChunkSize)
	// Use a slow upstream so requests will overlap
	upstream := &slowUpstream{
		data:      data,
		blockSize: testBlockSize,
		delay:     50 * time.Microsecond,
	}

	chunker, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	numGoroutines := 10
	offsets := make([]int64, numGoroutines)
	for i := range numGoroutines {
		offsets[i] = int64(i) * testBlockSize
	}

	results := make([][]byte, numGoroutines)

	var eg errgroup.Group

	for i := range numGoroutines {
		eg.Go(func() error {
			slice, err := chunker.Slice(t.Context(), offsets[i], testBlockSize)
			if err != nil {
				return fmt.Errorf("goroutine %d failed: %w", i, err)
			}
			results[i] = make([]byte, len(slice))
			copy(results[i], slice)
			return nil
		})
	}

	require.NoError(t, eg.Wait())

	for i := range numGoroutines {
		require.Equal(t, data[offsets[i]:offsets[i]+testBlockSize], results[i],
			"goroutine %d got wrong data", i)
	}
}

func TestStreamingChunker_EarlyReturn(t *testing.T) {
	t.Parallel()

	data := makeTestData(t, storage.MemoryChunkSize)
	upstream := &slowUpstream{
		data:      data,
		blockSize: testBlockSize,
		delay:     100 * time.Microsecond,
	}

	chunker, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	// Time how long it takes to get the first block
	start := time.Now()
	_, err = chunker.Slice(t.Context(), 0, testBlockSize)
	earlyLatency := time.Since(start)
	require.NoError(t, err)

	// Time how long it takes to get the last block (on a fresh chunker)
	chunker2, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache2",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker2.Close()

	lastOff := int64(len(data)) - testBlockSize
	start = time.Now()
	_, err = chunker2.Slice(t.Context(), lastOff, testBlockSize)
	lateLatency := time.Since(start)
	require.NoError(t, err)

	// The early slice should return significantly faster
	t.Logf("early latency: %v, late latency: %v", earlyLatency, lateLatency)
	assert.Less(t, earlyLatency, lateLatency,
		"first-block latency should be less than last-block latency")
}

func TestStreamingChunker_ErrorKeepsPartialData(t *testing.T) {
	t.Parallel()

	chunkSize := storage.MemoryChunkSize
	data := makeTestData(t, chunkSize)
	failAfter := int64(chunkSize / 2) // Fail at 2MB

	upstream := &errorAfterNUpstream{
		data:      data,
		failAfter: failAfter,
		blockSize: testBlockSize,
	}

	chunker, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	// Request the last page — this should fail because upstream dies at 2MB
	lastOff := int64(chunkSize) - testBlockSize
	_, err = chunker.Slice(t.Context(), lastOff, testBlockSize)
	require.Error(t, err)

	// But first page (within first 2MB) should still be cached and servable
	slice, err := chunker.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice)
}

func TestStreamingChunker_ContextCancellation(t *testing.T) {
	t.Parallel()

	data := makeTestData(t, storage.MemoryChunkSize)
	upstream := &slowUpstream{
		data:      data,
		blockSize: testBlockSize,
		delay:     1 * time.Millisecond,
	}

	chunker, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	// Request with a context that we'll cancel quickly
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Millisecond)
	defer cancel()

	lastOff := int64(storage.MemoryChunkSize) - testBlockSize
	_, err = chunker.Slice(ctx, lastOff, testBlockSize)
	// This should fail with context cancellation
	require.Error(t, err)

	// But another caller with a valid context should still get the data
	// because the fetch goroutine uses background context
	time.Sleep(200 * time.Millisecond) // Wait for fetch to complete
	slice, err := chunker.Slice(t.Context(), 0, testBlockSize)
	require.NoError(t, err)
	require.Equal(t, data[:testBlockSize], slice)
}

func TestStreamingChunker_LastBlockPartial(t *testing.T) {
	t.Parallel()

	// File size not aligned to blockSize
	size := storage.MemoryChunkSize - 100
	data := makeTestData(t, size)
	upstream := &fastUpstream{data: data, blockSize: testBlockSize}

	chunker, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	// Read the last partial block
	lastBlockOff := (int64(size) / testBlockSize) * testBlockSize
	remaining := int64(size) - lastBlockOff

	slice, err := chunker.Slice(t.Context(), lastBlockOff, remaining)
	require.NoError(t, err)
	require.Equal(t, data[lastBlockOff:], slice)
}

func TestStreamingChunker_MultiChunkSlice(t *testing.T) {
	t.Parallel()

	// Two 4MB chunks
	size := storage.MemoryChunkSize * 2
	data := makeTestData(t, size)
	upstream := &fastUpstream{data: data, blockSize: testBlockSize}

	chunker, err := NewStreamingChunker(
		int64(len(data)), testBlockSize,
		upstream, t.TempDir()+"/cache",
		newTestMetrics(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	// Request spanning two chunks: last page of chunk 0 + first page of chunk 1
	off := int64(storage.MemoryChunkSize) - testBlockSize
	length := testBlockSize * 2

	slice, err := chunker.Slice(t.Context(), off, int64(length))
	require.NoError(t, err)
	require.Equal(t, data[off:off+int64(length)], slice)
}

// --- Benchmarks ---
//
// Upstreams return data instantly (no sleep). Simulated latency is computed
// analytically based on the offsets requested and a hypothetical per-block
// network delay:
//
//   Streaming model: caller at offset O waits (O/blockSize + 1) * perBlockDelay
//   Bulk model:      every caller waits   (chunkSize/blockSize) * perBlockDelay

const simulatedPerBlockDelay = 10 * time.Microsecond // ~10ms for a full 4MB chunk (1024 blocks)

func newBenchmarkMetrics(b *testing.B) metrics.Metrics {
	b.Helper()

	m, err := metrics.NewMetrics(noop.NewMeterProvider())
	require.NoError(b, err)

	return m
}

// simulatedGCSDeliveryTime models GCS streaming: data arrives sequentially,
// one block per simulatedPerBlockDelay. Returns the time until byte position
// `pos` within a chunk has been delivered.
func simulatedGCSDeliveryTime(pos, blockSize int64) time.Duration {
	blockIndex := pos / blockSize
	return time.Duration(blockIndex+1) * simulatedPerBlockDelay
}

type benchChunker interface {
	Slice(ctx context.Context, off, length int64) ([]byte, error)
	Close() error
}

func BenchmarkRandomAccess(b *testing.B) {
	size := int64(storage.MemoryChunkSize)
	data := make([]byte, size)
	upstream := &fastUpstream{data: data, blockSize: testBlockSize}

	tcs := []struct {
		name        string
		newChunker  func(b *testing.B, m metrics.Metrics) benchChunker
		simLatency  func(offsets []int64) float64
	}{
		{
			name: "StreamingChunker",
			newChunker: func(b *testing.B, m metrics.Metrics) benchChunker {
				c, err := NewStreamingChunker(size, testBlockSize, upstream, b.TempDir()+"/cache", m)
				require.NoError(b, err)
				return c
			},
			simLatency: func(offsets []int64) float64 {
				// Each caller returns when GCS has delivered their block position.
				var total time.Duration
				for _, off := range offsets {
					total += simulatedGCSDeliveryTime(off%size, testBlockSize)
				}
				return float64(total.Microseconds()) / float64(len(offsets))
			},
		},
		{
			name: "Chunker",
			newChunker: func(b *testing.B, m metrics.Metrics) benchChunker {
				c, err := NewChunker(size, testBlockSize, upstream, b.TempDir()+"/cache", m)
				require.NoError(b, err)
				return c
			},
			simLatency: func(offsets []int64) float64 {
				// Every caller waits for the full chunk (last block delivered).
				var total time.Duration
				for range offsets {
					total += simulatedGCSDeliveryTime(size-testBlockSize, testBlockSize)
				}
				return float64(total.Microseconds()) / float64(len(offsets))
			},
		},
	}

	for _, tc := range tcs {
		b.Run(tc.name, func(b *testing.B) {
			m := newBenchmarkMetrics(b)
			numBlocks := size / testBlockSize
			rng := mathrand.New(mathrand.NewPCG(42, 0))
			const numCallers = 20

			// Suppress default ns/op metric — only simulated latency matters.
			b.ReportMetric(0, "ns/op")

			for i := 0; i < b.N; i++ {
				offsets := make([]int64, numCallers)
				for j := range offsets {
					offsets[j] = rng.Int64N(numBlocks) * testBlockSize
				}

				chunker := tc.newChunker(b, m)

				var eg errgroup.Group
				for _, off := range offsets {
					eg.Go(func() error {
						_, err := chunker.Slice(context.Background(), off, testBlockSize)
						return err
					})
				}
				require.NoError(b, eg.Wait())

				b.ReportMetric(tc.simLatency(offsets), "simulated-avg-us/caller")
				chunker.Close()
			}
		})
	}
}

