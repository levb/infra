package storage

import (
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/otel/attribute"
)

// instrumentedBasic wraps Basic with OTEL spans and timing.
type instrumentedBasic struct {
	inner Basic
}

func (b *instrumentedBasic) Upload(ctx context.Context, objectPath string, in io.Reader) (n int64, e error) {
	ctx, span := tracer.Start(ctx, "backend.Upload")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	timer := backendUploadTimerFactory.Begin(
		attribute.String("object_path", objectPath))

	n, e = b.inner.Upload(ctx, objectPath, in)
	if e != nil {
		timer.Failure(ctx, n)

		return n, e
	}

	timer.Success(ctx, n)
	span.SetAttributes(attribute.Int64("bytes_written", n))

	return n, nil
}

func (b *instrumentedBasic) StartDownload(ctx context.Context, objectPath string) (io.ReadCloser, error) {
	ctx, span := tracer.Start(ctx, "backend.StartDownload")
	span.SetAttributes(attribute.String("object_path", objectPath))

	timer := backendDownloadTimerFactory.Begin(
		attribute.String("object_path", objectPath))

	rc, err := b.inner.StartDownload(ctx, objectPath)
	if err != nil {
		timer.Failure(ctx, 0)
		recordError(span, err)
		span.End()

		return nil, err
	}

	// Wrap the reader to track bytes and finalize span+timer on Close
	return &instrumentedReadCloser{
		inner: rc,
		timer: timer,
		span:  span,
		ctx:   ctx,
	}, nil
}

// instrumentedRangeGetter wraps RangeGetter with OTEL spans and timing.
type instrumentedRangeGetter struct {
	inner RangeGetter
}

func (g *instrumentedRangeGetter) RangeGet(ctx context.Context, objectPath string, offset int64, length int) (io.ReadCloser, error) {
	ctx, span := tracer.Start(ctx, "backend.RangeGet")
	span.SetAttributes(
		attribute.String("object_path", objectPath),
		attribute.Int64("offset", offset),
		attribute.Int("length", length),
	)

	timer := backendRangeGetTimerFactory.Begin(
		attribute.String("object_path", objectPath))

	rc, err := g.inner.RangeGet(ctx, objectPath, offset, length)
	if err != nil {
		timer.Failure(ctx, 0)
		recordError(span, err)
		span.End()

		return nil, err
	}

	return &instrumentedReadCloser{
		inner: rc,
		timer: timer,
		span:  span,
		ctx:   ctx,
	}, nil
}

// instrumentedManager wraps Manager with OTEL spans and timing.
type instrumentedManager struct {
	inner Manager
}

func (m *instrumentedManager) Size(ctx context.Context, objectPath string) (virtSize, rawSize int64, e error) {
	ctx, span := tracer.Start(ctx, "backend.Size")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	timer := backendSizeTimerFactory.Begin(
		attribute.String("object_path", objectPath))

	virtSize, rawSize, e = m.inner.Size(ctx, objectPath)
	if e != nil {
		timer.Failure(ctx, 0)

		return 0, 0, e
	}

	timer.Success(ctx, virtSize)
	span.SetAttributes(
		attribute.Int64("virt_size", virtSize),
		attribute.Int64("raw_size", rawSize),
	)

	return virtSize, rawSize, nil
}

func (m *instrumentedManager) DeleteWithPrefix(ctx context.Context, prefix string) (e error) {
	ctx, span := tracer.Start(ctx, "backend.DeleteWithPrefix")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	span.SetAttributes(attribute.String("prefix", prefix))

	timer := backendDeleteTimerFactory.Begin(
		attribute.String("prefix", prefix))

	e = m.inner.DeleteWithPrefix(ctx, prefix)
	if e != nil {
		timer.Failure(ctx, 0)

		return e
	}

	timer.Success(ctx, 0)

	return nil
}

func (m *instrumentedManager) String() string {
	return fmt.Sprintf("[Instrumented] %s", m.inner.String())
}

// WrapBackendInstrumentation wraps a Backend's methods with OTEL spans and timing.
// PublicUploader and MultipartUploaderFactory are passed through unwrapped.
func WrapBackendInstrumentation(inner *Backend) *Backend {
	return &Backend{
		Basic:                    &instrumentedBasic{inner: inner.Basic},
		RangeGetter:              &instrumentedRangeGetter{inner: inner.RangeGetter},
		Manager:                  &instrumentedManager{inner: inner.Manager},
		PublicUploader:           inner.PublicUploader,
		MultipartUploaderFactory: inner.MultipartUploaderFactory,
	}
}

// PublicUploadURL is a pass-through on Backend (no wrapping needed).
// It's defined on PublicUploader interface and passed through in WrapBackendInstrumentation.

var (
	_ Basic       = (*instrumentedBasic)(nil)
	_ RangeGetter = (*instrumentedRangeGetter)(nil)
	_ Manager     = (*instrumentedManager)(nil)
)
