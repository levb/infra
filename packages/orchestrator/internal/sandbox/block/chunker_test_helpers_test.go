package block

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

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

var _ storage.Seekable = (*slowUpstream)(nil)

func (s *slowUpstream) StoreFile(_ context.Context, _ string) error {
	return fmt.Errorf("slowUpstream: StoreFile not implemented")
}

func (s *slowUpstream) ReadAt(_ context.Context, buffer []byte, off int64) (int, error) {
	end := min(off+int64(len(buffer)), int64(len(s.data)))
	n := copy(buffer, s.data[off:end])

	return n, nil
}

func (s *slowUpstream) Size(_ context.Context) (int64, int64, error) {
	return int64(len(s.data)), 0, nil
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

//nolint:unparam // size is always testFileSize in current tests but kept as parameter for flexibility
func makeTestData(t *testing.T, size int) []byte {
	t.Helper()

	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)

	return data
}
