package block

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	lz4 "github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const (
	testFrameSize = 256 * 1024 // 256 KB per frame for fast tests
	testFileSize  = testFrameSize * 4
)

// testFrameGetter implements storage.FrameGetter for testing.
// It serves both compressed frames (via GetFrame with ft!=nil) and
// uncompressed data (via GetFrame with ft==nil).
type testFrameGetter struct {
	uncompressed []byte
	compressed   map[int64][]byte // keyed by C-space offset
	frameTable   *storage.FrameTable
	delay        time.Duration
	fetchCount   atomic.Int64
}

func (g *testFrameGetter) GetFrame(_ context.Context, _ string, offsetU int64, ft *storage.FrameTable, decompress bool, buf []byte, readSize int64, onRead func(int64)) (storage.Range, error) {
	g.fetchCount.Add(1)

	if g.delay > 0 {
		time.Sleep(g.delay)
	}

	// Uncompressed path: ft is nil, serve raw data directly.
	if ft == nil {
		end := min(offsetU+int64(len(buf)), int64(len(g.uncompressed)))
		n := copy(buf, g.uncompressed[offsetU:end])

		if onRead != nil {
			batchSize := int64(testBlockSize)
			if readSize > 0 {
				batchSize = readSize
			}
			for written := batchSize; written <= int64(n); written += batchSize {
				onRead(written)
			}
			if int64(n)%batchSize != 0 {
				onRead(int64(n))
			}
		}

		return storage.Range{Start: offsetU, Length: n}, nil
	}

	// Compressed path: use frame table.
	starts, size, err := ft.FrameFor(offsetU)
	if err != nil {
		return storage.Range{}, fmt.Errorf("testFrameGetter: %w", err)
	}

	if decompress {
		uEnd := min(starts.U+int64(size.U), int64(len(g.uncompressed)))
		n := copy(buf, g.uncompressed[starts.U:uEnd])

		if onRead != nil {
			batchSize := int64(testBlockSize)
			if readSize > 0 {
				batchSize = readSize
			}
			// Simulate progressive delivery in readSize chunks.
			for written := batchSize; written <= int64(n); written += batchSize {
				onRead(written)
			}
			if int64(n)%batchSize != 0 {
				onRead(int64(n))
			}
		}

		return storage.Range{Start: starts.U, Length: n}, nil
	}

	cData, ok := g.compressed[starts.C]
	if !ok {
		return storage.Range{}, fmt.Errorf("testFrameGetter: no compressed data at C-offset %d", starts.C)
	}
	n := copy(buf, cData)

	return storage.Range{Start: starts.C, Length: n}, nil
}

// makeCompressedTestData creates test data, LZ4-compresses it into frames,
// and returns the FrameTable and a testFrameGetter ready for use.
func makeCompressedTestData(t *testing.T, dataSize, frameSize int, delay time.Duration) ([]byte, *storage.FrameTable, *testFrameGetter) {
	t.Helper()

	data := make([]byte, dataSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	ft := &storage.FrameTable{CompressionType: storage.CompressionLZ4}
	compressed := make(map[int64][]byte)

	var cOffset int64
	for i := 0; i < len(data); i += frameSize {
		end := min(i+frameSize, len(data))
		frame := data[i:end]

		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		_, err := w.Write(frame)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		cData := make([]byte, buf.Len())
		copy(cData, buf.Bytes())
		compressed[cOffset] = cData

		ft.Frames = append(ft.Frames, storage.FrameSize{
			U: int32(end - i),
			C: int32(len(cData)),
		})

		cOffset += int64(len(cData))
	}

	getter := &testFrameGetter{
		uncompressed: data,
		compressed:   compressed,
		frameTable:   ft,
		delay:        delay,
	}

	return data, ft, getter
}

type chunkerTestCase struct {
	name       string
	newChunker func(t *testing.T, data []byte, delay time.Duration) (*Chunker, *storage.FrameTable)
}

func allChunkerTestCases() []chunkerTestCase {
	return []chunkerTestCase{
		{
			name: "Chunker_Compressed",
			newChunker: func(t *testing.T, data []byte, delay time.Duration) (*Chunker, *storage.FrameTable) {
				t.Helper()
				_, ft, getter := makeCompressedTestData(t, len(data), testFrameSize, delay)
				// Use the getter's uncompressed data as the source truth
				// since compression may round-trip differently.
				copy(data, getter.uncompressed)
				c, err := NewChunker(
					AssetInfo{
						BasePath: "test-object",
						Size:     int64(len(data)),
						HasLZ4:   true,
					},
					testBlockSize,
					getter,
					t.TempDir()+"/cache",
					newTestMetrics(t),
					newTestFlags(t),
				)
				require.NoError(t, err)

				return c, ft
			},
		},
		{
			name: "Chunker_Uncompressed",
			newChunker: func(t *testing.T, data []byte, delay time.Duration) (*Chunker, *storage.FrameTable) {
				t.Helper()
				getter := &testUncompressedStorage{data: data, delay: delay}
				c, err := NewChunker(
					AssetInfo{
						BasePath: "test-object",
						Size:     int64(len(data)),
					},
					testBlockSize,
					getter,
					t.TempDir()+"/cache",
					newTestMetrics(t),
					newTestFlags(t),
				)
				require.NoError(t, err)

				return c, nil // no FT for uncompressed
			},
		},
	}
}

func TestChunker_ConcurrentSameOffset(t *testing.T) {
	t.Parallel()

	for _, tc := range allChunkerTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := makeTestData(t, testFileSize)
			chunker, ft := tc.newChunker(t, data, 100*time.Microsecond)
			defer chunker.Close()

			const numGoroutines = 20
			off := int64(0)
			readLen := int64(testBlockSize)

			results := make([][]byte, numGoroutines)
			var eg errgroup.Group

			for i := range numGoroutines {
				eg.Go(func() error {
					slice, err := chunker.GetBlock(t.Context(), off, readLen, ft)
					if err != nil {
						return fmt.Errorf("goroutine %d: %w", i, err)
					}
					results[i] = make([]byte, len(slice))
					copy(results[i], slice)

					return nil
				})
			}

			require.NoError(t, eg.Wait())

			for i := range numGoroutines {
				assert.Equal(t, data[off:off+readLen], results[i],
					"goroutine %d got wrong data", i)
			}
		})
	}
}

func TestChunker_ConcurrentDifferentOffsets(t *testing.T) {
	t.Parallel()

	for _, tc := range allChunkerTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := makeTestData(t, testFileSize)
			chunker, ft := tc.newChunker(t, data, 50*time.Microsecond)
			defer chunker.Close()

			const numGoroutines = 10
			readLen := int64(testBlockSize)

			// Pick offsets spread across the file.
			offsets := make([]int64, numGoroutines)
			for i := range numGoroutines {
				offsets[i] = int64(i) * readLen
				if offsets[i]+readLen > int64(len(data)) {
					offsets[i] = 0
				}
			}

			results := make([][]byte, numGoroutines)
			var eg errgroup.Group

			for i := range numGoroutines {
				eg.Go(func() error {
					slice, err := chunker.GetBlock(t.Context(), offsets[i], readLen, ft)
					if err != nil {
						return fmt.Errorf("goroutine %d (off=%d): %w", i, offsets[i], err)
					}
					results[i] = make([]byte, len(slice))
					copy(results[i], slice)

					return nil
				})
			}

			require.NoError(t, eg.Wait())

			for i := range numGoroutines {
				assert.Equal(t, data[offsets[i]:offsets[i]+readLen], results[i],
					"goroutine %d (off=%d) got wrong data", i, offsets[i])
			}
		})
	}
}

func TestChunker_ConcurrentMixed(t *testing.T) {
	t.Parallel()

	for _, tc := range allChunkerTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := makeTestData(t, testFileSize)
			chunker, ft := tc.newChunker(t, data, 50*time.Microsecond)
			defer chunker.Close()

			// Mix of ReadBlock, GetBlock, and repeated same-offset reads.
			const numGoroutines = 15
			readLen := int64(testBlockSize)

			var eg errgroup.Group

			for i := range numGoroutines {
				off := int64((i % 4) * testBlockSize) // 4 distinct offsets
				eg.Go(func() error {
					if i%2 == 0 {
						// GetBlock path
						slice, err := chunker.GetBlock(t.Context(), off, readLen, ft)
						if err != nil {
							return fmt.Errorf("goroutine %d GetBlock: %w", i, err)
						}
						if !bytes.Equal(data[off:off+readLen], slice) {
							return fmt.Errorf("goroutine %d GetBlock: data mismatch at off=%d", i, off)
						}
					} else {
						// ReadBlock path
						buf := make([]byte, readLen)
						n, err := chunker.ReadBlock(t.Context(), buf, off, ft)
						if err != nil {
							return fmt.Errorf("goroutine %d ReadBlock: %w", i, err)
						}
						if !bytes.Equal(data[off:off+int64(n)], buf[:n]) {
							return fmt.Errorf("goroutine %d ReadBlock: data mismatch at off=%d", i, off)
						}
					}

					return nil
				})
			}

			require.NoError(t, eg.Wait())
		})
	}
}

func TestChunker_ConcurrentStress(t *testing.T) {
	t.Parallel()

	for _, tc := range allChunkerTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := makeTestData(t, testFileSize)
			chunker, ft := tc.newChunker(t, data, 0) // no delay for stress
			defer chunker.Close()

			const numGoroutines = 50
			const opsPerGoroutine = 5
			readLen := int64(testBlockSize)

			var eg errgroup.Group

			for i := range numGoroutines {
				eg.Go(func() error {
					for j := range opsPerGoroutine {
						off := int64(((i*opsPerGoroutine)+j)%(len(data)/int(readLen))) * readLen
						slice, err := chunker.GetBlock(t.Context(), off, readLen, ft)
						if err != nil {
							return fmt.Errorf("goroutine %d op %d: %w", i, j, err)
						}
						if !bytes.Equal(data[off:off+readLen], slice) {
							return fmt.Errorf("goroutine %d op %d: data mismatch at off=%d", i, j, off)
						}
					}

					return nil
				})
			}

			require.NoError(t, eg.Wait())
		})
	}
}

func TestChunker_ConcurrentReadBlock_CrossFrame(t *testing.T) {
	t.Parallel()

	// Test cross-frame ReadBlock for both compressed and uncompressed modes.
	for _, tc := range allChunkerTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := makeTestData(t, testFileSize)
			chunker, ft := tc.newChunker(t, data, 50*time.Microsecond)
			defer chunker.Close()

			const numGoroutines = 10

			// Read spanning multiple blocks/frames.
			readLen := testBlockSize * 2
			if int64(readLen) > int64(len(data)) {
				readLen = len(data)
			}

			var eg errgroup.Group

			for i := range numGoroutines {
				off := int64(0) // all read from start
				eg.Go(func() error {
					buf := make([]byte, readLen)
					n, err := chunker.ReadBlock(t.Context(), buf, off, ft)
					if err != nil {
						return fmt.Errorf("goroutine %d: %w", i, err)
					}
					if !bytes.Equal(data[off:off+int64(n)], buf[:n]) {
						return fmt.Errorf("goroutine %d: data mismatch", i)
					}

					return nil
				})
			}

			require.NoError(t, eg.Wait())
		})
	}
}

// TestChunker_FetchDedup verifies that concurrent requests for the same data
// don't cause duplicate upstream fetches.
func TestChunker_FetchDedup(t *testing.T) {
	t.Parallel()

	t.Run("DecompressMMapChunker_Compressed", func(t *testing.T) {
		t.Parallel()

		data := make([]byte, testFileSize)
		_, err := rand.Read(data)
		require.NoError(t, err)

		_, ft, getter := makeCompressedTestData(t, testFileSize, testFrameSize, 10*time.Millisecond)
		copy(data, getter.uncompressed)

		chunker, err := NewChunker(
			AssetInfo{
				BasePath: "test-object",
				Size:     int64(len(data)),
				HasLZ4:   true,
			},
			testBlockSize,
			getter,
			t.TempDir()+"/cache",
			newTestMetrics(t),
			newTestFlags(t),
		)
		require.NoError(t, err)
		defer chunker.Close()

		const numGoroutines = 10

		var eg errgroup.Group
		for range numGoroutines {
			eg.Go(func() error {
				// All request offset 0 (same frame).
				_, err := chunker.GetBlock(t.Context(), 0, testBlockSize, ft)

				return err
			})
		}
		require.NoError(t, eg.Wait())

		// With frameFlight dedup, only 1 fetch should have happened.
		assert.Equal(t, int64(1), getter.fetchCount.Load(),
			"expected 1 fetch (dedup), got %d", getter.fetchCount.Load())
	})
}

// testUncompressedStorage implements storage.FrameGetter for uncompressed-only tests.
// GetFrame serves raw uncompressed data when ft is nil.
type testUncompressedStorage struct {
	data       []byte
	delay      time.Duration
	fetchCount atomic.Int64
}

func (t *testUncompressedStorage) GetFrame(_ context.Context, _ string, offsetU int64, ft *storage.FrameTable, _ bool, buf []byte, readSize int64, onRead func(int64)) (storage.Range, error) {
	t.fetchCount.Add(1)

	if t.delay > 0 {
		time.Sleep(t.delay)
	}

	if ft != nil {
		return storage.Range{}, fmt.Errorf("testUncompressedStorage: compressed GetFrame not supported")
	}

	end := min(offsetU+int64(len(buf)), int64(len(t.data)))
	n := copy(buf, t.data[offsetU:end])

	if onRead != nil {
		batchSize := int64(testBlockSize)
		if readSize > 0 {
			batchSize = readSize
		}
		for written := batchSize; written <= int64(n); written += batchSize {
			onRead(written)
		}
		if int64(n)%batchSize != 0 {
			onRead(int64(n))
		}
	}

	return storage.Range{Start: offsetU, Length: n}, nil
}

// TestChunker_DualMode_SharedCache verifies that a single chunker
// instance correctly serves both compressed and uncompressed callers, sharing
// the mmap cache across modes. If region X is fetched via compressed path,
// a subsequent uncompressed request for region X is served from cache (no fetch).
func TestChunker_DualMode_SharedCache(t *testing.T) {
	t.Parallel()

	data, ft, getter := makeCompressedTestData(t, testFileSize, testFrameSize, 0)

	// Create ONE chunker with both compressed and uncompressed assets available.
	chunker, err := NewChunker(
		AssetInfo{
			BasePath: "test-object",
			Size:     int64(len(data)),
			HasLZ4:   true,
		},
		testBlockSize,
		getter,
		t.TempDir()+"/cache",
		newTestMetrics(t),
		newTestFlags(t),
	)
	require.NoError(t, err)
	defer chunker.Close()

	readLen := int64(testBlockSize)

	// --- Phase 1: Compressed caller fetches frame 0 ---
	slice1, err := chunker.GetBlock(t.Context(), 0, readLen, ft)
	require.NoError(t, err)
	assert.Equal(t, data[0:readLen], slice1, "compressed read: data mismatch at offset 0")

	fetchesAfterPhase1 := getter.fetchCount.Load()
	assert.Equal(t, int64(1), fetchesAfterPhase1, "expected 1 fetch for frame 0")

	// --- Phase 2: Uncompressed caller reads offset 0 — should be served from cache ---
	slice2, err := chunker.GetBlock(t.Context(), 0, readLen, nil)
	require.NoError(t, err)
	assert.Equal(t, data[0:readLen], slice2, "uncompressed read from cache: data mismatch at offset 0")

	// No new fetches should have occurred.
	assert.Equal(t, fetchesAfterPhase1, getter.fetchCount.Load(),
		"uncompressed read of cached region should not trigger any fetch")

	// --- Phase 3: Uncompressed caller reads a new region (frame 1) ---
	frame1Off := int64(testFrameSize) // start of frame 1
	slice3, err := chunker.GetBlock(t.Context(), frame1Off, readLen, nil)
	require.NoError(t, err)
	assert.Equal(t, data[frame1Off:frame1Off+readLen], slice3,
		"uncompressed read: data mismatch at frame 1")

	// This should have triggered a new fetch via GetFrame (uncompressed path).
	assert.Greater(t, getter.fetchCount.Load(), fetchesAfterPhase1,
		"new region should trigger a fetch")
	fetchesAfterPhase3 := getter.fetchCount.Load()

	// --- Phase 4: Compressed caller reads frame 1 — should be served from cache ---
	slice4, err := chunker.GetBlock(t.Context(), frame1Off, readLen, ft)
	require.NoError(t, err)
	assert.Equal(t, data[frame1Off:frame1Off+readLen], slice4,
		"compressed read from cache: data mismatch at frame 1")

	// No new fetches for frame 1.
	assert.Equal(t, fetchesAfterPhase3, getter.fetchCount.Load(),
		"compressed read of cached region should not trigger new fetch")
}
