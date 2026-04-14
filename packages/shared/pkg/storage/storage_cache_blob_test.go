package storage

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

var noopTracer = noop.TracerProvider{}.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/storage")

func TestCachedObjectProvider_Put(t *testing.T) {
	t.Parallel()

	t.Run("can be cached successfully", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		cacheDir := filepath.Join(tempDir, "cache")
		data := []byte("hello world")

		err := os.MkdirAll(cacheDir, os.ModePerm)
		require.NoError(t, err)

		inner := NewMockStore(t)
		inner.On("PutBlob", mock.Anything, "test/path", mock.Anything).Return(nil)

		featureFlags := NewMockFeatureFlagsClient(t)
		featureFlags.EXPECT().BoolFlag(mock.Anything, mock.Anything).Return(true)

		cb := cachedBlob{path: cacheDir, inner: inner, innerPath: "test/path", chunkSize: 1024, flags: featureFlags, tracer: noopTracer}

		// Write to inner + cache
		cb.PutBlob(t.Context(), data)
		require.NoError(t, inner.PutBlob(t.Context(), "test/path", data))

		// file is written asynchronously, wait for it to finish
		cb.wg.Wait()

		// prevent the provider from falling back to cache
		cb.inner = nil

		gotData, err := cb.GetBlob(t.Context())
		require.NoError(t, err)
		assert.Equal(t, data, gotData)
	})

	t.Run("uncached reads will be cached the second time", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		cacheDir := filepath.Join(tempDir, "cache")
		err := os.MkdirAll(cacheDir, 0o777)
		require.NoError(t, err)

		const dataSize = 10 * megabyte
		actualData := generateData(t, dataSize)

		inner := NewMockStore(t)
		inner.On("GetBlob", mock.Anything, "test/path").
			Return(func(_ context.Context, _ string) ([]byte, error) {
				return actualData, nil
			})

		cb := cachedBlob{path: cacheDir, inner: inner, innerPath: "test/path", chunkSize: 1024, tracer: noopTracer}

		read, err := cb.GetBlob(t.Context())
		require.NoError(t, err)
		assert.Equal(t, actualData, read)

		cb.wg.Wait()

		cb.inner = nil

		read, err = cb.GetBlob(t.Context())
		require.NoError(t, err)
		assert.Equal(t, actualData, read)
	})
}

func generateData(t *testing.T, count int) []byte {
	t.Helper()

	data := make([]byte, count)
	for i := range count {
		data[i] = byte(rand.Intn(256))
	}

	return data
}
