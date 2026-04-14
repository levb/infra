package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/lock"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	kilobyte = 1024
	megabyte = 1024 * kilobyte
)

type cachedBlob struct {
	path      string
	chunkSize int64
	inner     Store
	innerPath string
	flags     featureFlagsClient
	tracer    trace.Tracer

	wg sync.WaitGroup
}

func (b *cachedBlob) GetBlob(ctx context.Context) (data []byte, e error) {
	ctx, span := b.tracer.Start(ctx, "read object")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	// Try cache first
	cached, err := b.readFromCache(ctx)
	if err == nil {
		recordCacheRead(ctx, true, int64(len(cached)), cacheTypeBlob, cacheOpWriteTo)

		return cached, nil
	}

	recordCacheReadError(ctx, cacheTypeBlob, cacheOpWriteTo, err)

	// Cache miss: fetch from inner
	data, err = b.inner.GetBlob(ctx, b.innerPath)
	if err != nil {
		return nil, err
	}

	if !skipCacheWriteback(ctx) {
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		b.goCtxWithoutCancel(ctx, func(ctx context.Context) {
			ctx, span := b.tracer.Start(ctx, "write file back to cache")
			defer span.End()

			count, err := b.writeFileToCache(ctx, bytes.NewReader(dataCopy))
			if err != nil {
				recordCacheWriteError(ctx, cacheTypeBlob, cacheOpWriteTo, err)
				recordError(span, err)

				return
			}

			recordCacheWrite(ctx, count, cacheTypeBlob, cacheOpWriteTo)
		})
	}

	recordCacheRead(ctx, false, int64(len(data)), cacheTypeBlob, cacheOpWriteTo)

	return data, nil
}

// PutBlob writes data to the inner backend, and optionally caches it.
func (b *cachedBlob) PutBlob(ctx context.Context, data []byte) {
	if b.flags != nil && b.flags.BoolFlag(ctx, featureflags.EnableWriteThroughCacheFlag) {
		b.goCtxWithoutCancel(ctx, func(ctx context.Context) {
			ctx, span := b.tracer.Start(ctx, "write data to cache")
			defer span.End()

			count, err := b.writeFileToCache(ctx, bytes.NewReader(data))
			if err != nil {
				recordError(span, err)
				recordCacheWriteError(ctx, cacheTypeBlob, cacheOpPut, err)
			} else {
				recordCacheWrite(ctx, count, cacheTypeBlob, cacheOpPut)
			}
		})
	}
}

func (b *cachedBlob) goCtxWithoutCancel(ctx context.Context, fn func(context.Context)) {
	b.wg.Go(func() {
		fn(context.WithoutCancel(ctx))
	})
}

func (b *cachedBlob) fullFilename() string {
	return fmt.Sprintf("%s/content.bin", b.path)
}

func (b *cachedBlob) readFromCache(ctx context.Context) (_ []byte, e error) {
	ctx, span := b.tracer.Start(ctx, "read cached object")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	path := b.fullFilename()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read cached file %s: %w", path, err)
	}

	return data, nil
}

func (b *cachedBlob) writeFileToCache(ctx context.Context, input io.Reader) (int64, error) {
	path := b.fullFilename()

	output, err := lock.OpenFile(ctx, path)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire lock on file %s: %w", path, err)
	}
	defer utils.CleanupCtx(ctx, "failed to unlock file", output.Close)

	count, err := io.Copy(output, input)
	if ignoreEOF(err) != nil {
		return 0, fmt.Errorf("failed to write to cache file %s: %w", path, err)
	}

	if err := output.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit cache file %s: %w", path, err)
	}

	return count, nil
}
