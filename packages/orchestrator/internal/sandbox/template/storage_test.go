package template

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/cfg"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func testBlockMetrics(t *testing.T) blockmetrics.Metrics {
	t.Helper()
	m, err := blockmetrics.NewMetrics(noop.NewMeterProvider())
	require.NoError(t, err)

	return m
}

func testDiffStore(t *testing.T) *build.DiffStore {
	t.Helper()
	ds, err := build.NewDiffStore(cfg.Config{}, nil, t.TempDir(), 5*time.Minute, 0)
	require.NoError(t, err)

	return ds
}

func makeSerializedHeader(t *testing.T, buildId uuid.UUID, size uint64, withFrameTable bool) []byte { //nolint:unparam // size kept as parameter for test clarity
	t.Helper()

	metadata := &header.Metadata{
		Version:     4,
		BuildId:     buildId,
		BaseBuildId: buildId,
		Size:        size,
		BlockSize:   uint64(storage.MemoryChunkSize),
		Generation:  1,
	}

	h, err := header.NewHeader(metadata, nil)
	require.NoError(t, err)

	if withFrameTable {
		ft := &storage.FrameTable{
			CompressionType: storage.CompressionZstd,
			StartAt:         storage.FrameOffset{U: 0, C: 0},
			Frames:          []storage.FrameSize{{U: int32(size), C: int32(size / 2)}},
		}
		err = h.AddFrames(ft)
		require.NoError(t, err)
	}

	data, err := header.Serialize(h.Metadata, h.Mapping)
	require.NoError(t, err)

	return data
}

// lz4Compress wraps header data in LZ4 block compression for test fixtures.
func lz4Compress(t *testing.T, data []byte) []byte {
	t.Helper()
	compressed, err := storage.CompressLZ4(data)
	require.NoError(t, err)

	return compressed
}

func TestNewStorage_UseCompressedAssets_PrefersCompressedHeader(t *testing.T) { //nolint:paralleltest // mutates storage.UseCompressedAssets global
	saved := storage.UseCompressedAssets
	t.Cleanup(func() { storage.UseCompressedAssets = saved })
	storage.UseCompressedAssets = true

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	dataSize := uint64(storage.MemoryChunkSize)

	defaultHeaderData := makeSerializedHeader(t, buildId, dataSize, false)
	compressedHeaderData := makeSerializedHeader(t, buildId, dataSize, true)

	provider := storage.NewMockStorageProvider(t)

	headerPath := files.HeaderPath(string(build.Memfile))
	compressedHeaderPath := files.CompressedHeaderPath(string(build.Memfile))

	provider.EXPECT().GetBlob(mock.Anything, headerPath).Return(defaultHeaderData, nil)
	provider.EXPECT().GetBlob(mock.Anything, compressedHeaderPath).Return(lz4Compress(t, compressedHeaderData), nil)
	// Size is called by NewFile -> NewStorage's build.NewFile path
	provider.EXPECT().Size(mock.Anything, mock.Anything).Return(int64(dataSize), int64(dataSize), nil).Maybe()

	store := testDiffStore(t)
	metrics := testBlockMetrics(t)

	ctx := context.Background()
	s, err := NewStorage(ctx, store, buildId.String(), build.Memfile, nil, provider, metrics)
	require.NoError(t, err)

	// The compressed header has a frame table; the default does not.
	// If compressed header was preferred, at least one mapping should have a frame table.
	hasFrameTable := false
	for _, m := range s.Header().Mapping {
		if m.FrameTable != nil {
			hasFrameTable = true

			break
		}
	}
	assert.True(t, hasFrameTable, "compressed header should be preferred when UseCompressedAssets=true")
}

func TestNewStorage_UseCompressedAssets_FallsBackToDefault(t *testing.T) { //nolint:paralleltest // mutates storage.UseCompressedAssets global
	saved := storage.UseCompressedAssets
	t.Cleanup(func() { storage.UseCompressedAssets = saved })
	storage.UseCompressedAssets = true

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	dataSize := uint64(storage.MemoryChunkSize)

	defaultHeaderData := makeSerializedHeader(t, buildId, dataSize, false)

	provider := storage.NewMockStorageProvider(t)

	headerPath := files.HeaderPath(string(build.Memfile))
	compressedHeaderPath := files.CompressedHeaderPath(string(build.Memfile))

	provider.EXPECT().GetBlob(mock.Anything, headerPath).Return(defaultHeaderData, nil)
	provider.EXPECT().GetBlob(mock.Anything, compressedHeaderPath).Return(nil, storage.ErrObjectNotExist)
	provider.EXPECT().Size(mock.Anything, mock.Anything).Return(int64(dataSize), int64(dataSize), nil).Maybe()

	store := testDiffStore(t)
	metrics := testBlockMetrics(t)

	ctx := context.Background()
	s, err := NewStorage(ctx, store, buildId.String(), build.Memfile, nil, provider, metrics)
	require.NoError(t, err)

	// Should fall back to default header (no frame table).
	for _, m := range s.Header().Mapping {
		assert.Nil(t, m.FrameTable, "fallback header should not have frame table")
	}
}

func TestNewStorage_NoCompressedAssets_OnlyFetchesDefault(t *testing.T) { //nolint:paralleltest // mutates storage.UseCompressedAssets global
	saved := storage.UseCompressedAssets
	t.Cleanup(func() { storage.UseCompressedAssets = saved })
	storage.UseCompressedAssets = false

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	dataSize := uint64(storage.MemoryChunkSize)

	defaultHeaderData := makeSerializedHeader(t, buildId, dataSize, false)

	provider := storage.NewMockStorageProvider(t)

	headerPath := files.HeaderPath(string(build.Memfile))

	provider.EXPECT().GetBlob(mock.Anything, headerPath).Return(defaultHeaderData, nil)
	// The compressed header path should NOT be called - mock assertions will catch this.
	provider.EXPECT().Size(mock.Anything, mock.Anything).Return(int64(dataSize), int64(dataSize), nil).Maybe()

	store := testDiffStore(t)
	metrics := testBlockMetrics(t)

	ctx := context.Background()
	s, err := NewStorage(ctx, store, buildId.String(), build.Memfile, nil, provider, metrics)
	require.NoError(t, err)

	// Default header should not have frame table.
	for _, m := range s.Header().Mapping {
		assert.Nil(t, m.FrameTable, "default header should not have frame table")
	}

	// Mock assertions will verify the compressed path was never called.
}
