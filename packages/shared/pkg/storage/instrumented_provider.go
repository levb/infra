package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// InstrumentedProvider wraps a StorageProvider with OTEL spans and timing
// for all methods. It sits between callers and the inner provider (which
// may be a Cache or a plain Storage).
type InstrumentedProvider struct {
	inner StorageProvider
}

var _ StorageProvider = (*InstrumentedProvider)(nil)

// WrapProviderInstrumentation wraps a StorageProvider with OTEL spans and metrics.
func WrapProviderInstrumentation(inner StorageProvider) *InstrumentedProvider {
	return &InstrumentedProvider{inner: inner}
}

func (p *InstrumentedProvider) GetFrame(ctx context.Context, objectPath string, offsetU int64, frameTable *FrameTable, decompress bool, buf []byte) (rng Range, e error) {
	ctx, span := tracer.Start(ctx, "provider.GetFrame")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	compressed := IsCompressed(frameTable)
	span.SetAttributes(
		attribute.String("object_path", objectPath),
		attribute.Int64("offset_u", offsetU),
		attribute.Int("buf_len", len(buf)),
		attribute.Bool("decompress", decompress),
		attribute.Bool("compressed", compressed),
	)

	timer := providerGetFrameTimerFactory.Begin(
		attribute.Bool("compressed", compressed),
		attribute.Bool("decompress", decompress),
	)

	rng, e = p.inner.GetFrame(ctx, objectPath, offsetU, frameTable, decompress, buf)
	if e != nil {
		timer.Failure(ctx, int64(len(buf)))

		return rng, e
	}

	timer.Success(ctx, int64(rng.Length))

	return rng, nil
}

func (p *InstrumentedProvider) StoreFile(ctx context.Context, inFilePath, asObjectPath string, opts *FramedUploadOptions) (ft *FrameTable, e error) {
	ctx, span := tracer.Start(ctx, "provider.StoreFile")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	compressed := opts != nil && opts.CompressionType != CompressionNone
	span.SetAttributes(
		attribute.String("object_path", asObjectPath),
		attribute.Bool("compressed", compressed),
	)

	timer := providerStoreFileTimerFactory.Begin(
		attribute.Bool("compressed", compressed),
	)

	ft, e = p.inner.StoreFile(ctx, inFilePath, asObjectPath, opts)
	if e != nil {
		timer.Failure(ctx, 0)

		return nil, e
	}

	timer.Success(ctx, 0)

	return ft, nil
}

func (p *InstrumentedProvider) GetBlob(ctx context.Context, objectPath string) (data []byte, e error) {
	ctx, span := tracer.Start(ctx, "provider.GetBlob")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	span.SetAttributes(attribute.String("object_path", objectPath))

	timer := providerGetBlobTimerFactory.Begin(
		attribute.String("object_path", objectPath),
	)

	data, e = p.inner.GetBlob(ctx, objectPath)
	if e != nil {
		timer.Failure(ctx, 0)

		return nil, e
	}

	timer.Success(ctx, int64(len(data)))

	return data, nil
}

func (p *InstrumentedProvider) CopyBlob(ctx context.Context, objectPath string, dst io.Writer) (n int64, e error) {
	ctx, span := tracer.Start(ctx, "provider.CopyBlob")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	span.SetAttributes(attribute.String("object_path", objectPath))

	timer := providerCopyBlobTimerFactory.Begin(
		attribute.String("object_path", objectPath),
	)

	n, e = p.inner.CopyBlob(ctx, objectPath, dst)
	if e != nil {
		timer.Failure(ctx, n)

		return n, e
	}

	timer.Success(ctx, n)

	return n, nil
}

func (p *InstrumentedProvider) StoreBlob(ctx context.Context, objectPath string, in io.Reader) (e error) {
	ctx, span := tracer.Start(ctx, "provider.StoreBlob")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	span.SetAttributes(attribute.String("object_path", objectPath))

	timer := providerStoreBlobTimerFactory.Begin(
		attribute.String("object_path", objectPath),
	)

	e = p.inner.StoreBlob(ctx, objectPath, in)
	if e != nil {
		timer.Failure(ctx, 0)

		return e
	}

	timer.Success(ctx, 0)

	return nil
}

func (p *InstrumentedProvider) PublicUploadURL(ctx context.Context, objectPath string, ttl time.Duration) (string, error) {
	ctx, span := tracer.Start(ctx, "provider.PublicUploadURL")
	defer span.End()

	span.SetAttributes(attribute.String("object_path", objectPath))

	return p.inner.PublicUploadURL(ctx, objectPath, ttl)
}

func (p *InstrumentedProvider) Size(ctx context.Context, objectPath string) (virtSize, rawSize int64, e error) {
	ctx, span := tracer.Start(ctx, "provider.Size")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	span.SetAttributes(attribute.String("object_path", objectPath))

	timer := providerSizeTimerFactory.Begin(
		attribute.String("object_path", objectPath),
	)

	virtSize, rawSize, e = p.inner.Size(ctx, objectPath)
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

func (p *InstrumentedProvider) DeleteWithPrefix(ctx context.Context, prefix string) (e error) {
	ctx, span := tracer.Start(ctx, "provider.DeleteWithPrefix")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	span.SetAttributes(attribute.String("prefix", prefix))

	timer := providerDeleteTimerFactory.Begin(
		attribute.String("prefix", prefix),
	)

	e = p.inner.DeleteWithPrefix(ctx, prefix)
	if e != nil {
		timer.Failure(ctx, 0)

		return e
	}

	timer.Success(ctx, 0)

	return nil
}

func (p *InstrumentedProvider) String() string {
	return fmt.Sprintf("[Instrumented] %s", p.inner.String())
}
