package storage

import (
	"context"
	"errors"
	"io"
	"os"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// wrappers used in the storage read path.
var (
	_ io.Reader   = (*offsetReader)(nil)  // ReaderAt → sequential Reader adapter
	_ RangeReader = (*sectionReader)(nil) // os.File SectionReader for range reads
	_ RangeReader = (*reader)(nil)        // decompression + OTEL timer/span/gauge/cancel
	_ RangeReader = (*rangeCloser)(nil)   // RangeReader wrapper to add Close(ctx) to an io.ReadCloser
)

// offsetReader adapts an io.ReaderAt into a sequential io.Reader
// starting at the given offset.
type offsetReader struct {
	wrapped io.ReaderAt
	offset  int64
}

func (r *offsetReader) Read(p []byte) (n int, err error) {
	n, err = r.wrapped.ReadAt(p, r.offset)
	r.offset += int64(n)

	return
}

func newOffsetReader(reader io.ReaderAt, offset int64) *offsetReader {
	return &offsetReader{reader, offset}
}

type rangeCloser struct {
	io.ReadCloser
}

func (p *rangeCloser) Close(context.Context) error { return p.ReadCloser.Close() }

// NewRangeReader adapts a plain io.ReadCloser into a RangeReader whose Close
// ignores the context.
func NewRangeReader(rc io.ReadCloser) RangeReader { return &rangeCloser{rc} }

// sectionReader exposes a bounded section of an os.File as a RangeReader,
// closing the underlying file on Close.
type sectionReader struct {
	*io.SectionReader

	file *os.File
}

func newSectionReader(f *os.File, off, length int64) *sectionReader {
	return &sectionReader{
		SectionReader: io.NewSectionReader(f, off, length),
		file:          f,
	}
}

func (r *sectionReader) Close(context.Context) error {
	return r.file.Close()
}

// reader is the storage read path's composable wrapper. It always wraps an
// inner RangeReader; the with* builder methods layer on optional concerns —
// decompression and the OTEL timer/span/gauge/cancel — applied on Read/Close.
// It counts bytes read and captures the first non-EOF read error.
type reader struct {
	inner RangeReader

	dec io.ReadCloser // when set, Read decompresses

	timer  *telemetry.Stopwatch
	span   trace.Span
	gauge  metric.Int64UpDownCounter
	cancel context.CancelFunc

	bytes   int64
	readErr error
}

func newReader(inner RangeReader) *reader {
	return &reader{inner: inner}
}

// withDecompress makes Read return decompressed bytes. It is the one fallible
// builder step (unknown codec), so take it first — the rest of the chain is
// infallible.
func (r *reader) withDecompress(ct CompressionType) (*reader, error) {
	dec, err := newDecoder(r.inner, ct)
	if err != nil {
		return nil, err
	}
	r.dec = dec

	return r, nil
}

// withTimer records an OTEL timer as Success/Failure on Close.
func (r *reader) withTimer(t *telemetry.Stopwatch) *reader {
	r.timer = t

	return r
}

// withSpan ends an OTEL span on Close, recording any read/close error.
func (r *reader) withSpan(s trace.Span) *reader {
	r.span = s

	return r
}

// withGauge increments an up/down counter now and decrements it on Close,
// tracking the number of concurrently-open readers.
func (r *reader) withGauge(ctx context.Context, g metric.Int64UpDownCounter) *reader {
	r.gauge = g
	g.Add(ctx, 1)

	return r
}

// withCancel runs a context.CancelFunc on Close.
func (r *reader) withCancel(c context.CancelFunc) *reader {
	r.cancel = c

	return r
}

func (r *reader) Read(p []byte) (int, error) {
	var n int
	var err error
	if r.dec != nil {
		n, err = r.dec.Read(p)
	} else {
		n, err = r.inner.Read(p)
	}
	r.bytes += int64(n)

	if err != nil && !errors.Is(err, io.EOF) {
		r.readErr = err
	}

	return n, err
}

func (r *reader) Close(ctx context.Context) error {
	var closeErr error
	if r.dec != nil {
		closeErr = r.dec.Close()
	}
	if innerErr := r.inner.Close(ctx); closeErr == nil {
		closeErr = innerErr
	}

	if r.timer != nil {
		if r.readErr != nil || closeErr != nil {
			r.timer.Failure(ctx, r.bytes)
		} else {
			r.timer.Success(ctx, r.bytes)
		}
	}

	if r.span != nil {
		if closeErr != nil {
			recordError(r.span, closeErr)
		} else if r.readErr != nil {
			recordError(r.span, r.readErr)
		}

		r.span.End()
	}

	if r.gauge != nil {
		r.gauge.Add(ctx, -1)
	}

	if r.cancel != nil {
		r.cancel()
	}

	return closeErr
}
