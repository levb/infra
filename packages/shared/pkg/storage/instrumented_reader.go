package storage

import (
	"context"
	"io"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// instrumentedReadCloser wraps an io.ReadCloser to track bytes read
// and finalize an OTEL span + timer on Close.
type instrumentedReadCloser struct {
	inner     io.ReadCloser
	timer     *telemetry.Stopwatch
	span      trace.Span
	ctx       context.Context //nolint:containedctx // needed to record metrics in Close()
	bytesRead atomic.Int64
}

func (r *instrumentedReadCloser) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	r.bytesRead.Add(int64(n))

	return n, err
}

func (r *instrumentedReadCloser) Close() error {
	err := r.inner.Close()
	total := r.bytesRead.Load()

	r.span.SetAttributes(attribute.Int64("bytes_read", total))

	if err != nil {
		r.timer.Failure(r.ctx, total)
		recordError(r.span, err)
	} else {
		r.timer.Success(r.ctx, total)
	}

	r.span.End()

	return err
}
