package sandbox

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// storeFileCall records a StoreFile invocation.
type storeFileCall struct {
	objectPath string
	opts       *storage.FramedUploadOptions
}

// storeBlobCall records a StoreBlob invocation.
type storeBlobCall struct {
	objectPath string
	data       []byte
}

func createTempFile(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte("test-data"), 0o644))

	return p
}

func makeHeader(t *testing.T, buildId uuid.UUID, size uint64) *header.Header { //nolint:unparam // size kept as parameter for test clarity
	t.Helper()
	h, err := header.NewHeader(&header.Metadata{
		Version:     4,
		BuildId:     buildId,
		BaseBuildId: buildId,
		Size:        size,
		BlockSize:   uint64(storage.MemoryChunkSize),
		Generation:  1,
	}, nil)
	require.NoError(t, err)

	return h
}

func TestUploadData_CompressionEnabled(t *testing.T) { //nolint:paralleltest // mutates storage.EnableGCSCompression global
	saved := storage.EnableGCSCompression
	t.Cleanup(func() { storage.EnableGCSCompression = saved })
	storage.EnableGCSCompression = true

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	tmpDir := t.TempDir()

	rootfsPath := createTempFile(t, tmpDir, "rootfs.ext4")
	memfilePath := createTempFile(t, tmpDir, "memfile")
	snapfilePath := createTempFile(t, tmpDir, "snapfile")
	metadataPath := createTempFile(t, tmpDir, "metadata.json")

	provider := storage.NewMockStorageProvider(t)

	var mu sync.Mutex
	var storeFileCalls []storeFileCall

	// Mock StoreFile to record calls and return a frame table for compressed uploads.
	provider.EXPECT().StoreFile(
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, _ string, objectPath string, opts *storage.FramedUploadOptions) (*storage.FrameTable, error) {
		mu.Lock()
		storeFileCalls = append(storeFileCalls, storeFileCall{objectPath: objectPath, opts: opts})
		mu.Unlock()

		if opts != nil {
			return &storage.FrameTable{
				CompressionType: storage.CompressionZstd,
				StartAt:         storage.FrameOffset{U: 0, C: 0},
				Frames:          []storage.FrameSize{{U: 4096, C: 2048}},
			}, nil
		}

		return nil, nil
	})

	// Mock StoreBlob for snapfile, metadata, and uncompressed headers.
	var blobMu sync.Mutex
	var blobCalls []storeBlobCall

	provider.EXPECT().StoreBlob(
		mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, objectPath string, in io.Reader) error {
		data, err := io.ReadAll(in)
		if err != nil {
			return err
		}
		blobMu.Lock()
		blobCalls = append(blobCalls, storeBlobCall{objectPath: objectPath, data: data})
		blobMu.Unlock()

		return nil
	})

	h := makeHeader(t, buildId, uint64(storage.MemoryChunkSize))
	tb := NewTemplateBuild(h, h, provider, files)

	ctx := context.Background()
	result, err := tb.UploadData(ctx, metadataPath, snapfilePath, &memfilePath, &rootfsPath)
	require.NoError(t, err)

	// Verify StoreFile was called with the expected paths and opts.
	pathOpts := make(map[string]bool) // path -> hasOpts
	for _, c := range storeFileCalls {
		pathOpts[c.objectPath] = c.opts != nil
	}

	// Uncompressed uploads (nil opts)
	assert.False(t, pathOpts[files.StorageRootfsPath()], "rootfs uncompressed should have nil opts")
	assert.False(t, pathOpts[files.StorageMemfilePath()], "memfile uncompressed should have nil opts")

	// Compressed uploads (non-nil opts)
	assert.True(t, pathOpts[files.StorageRootfsCompressedPath(storage.CompressionZstd)], "rootfs compressed should have non-nil opts")
	assert.True(t, pathOpts[files.StorageMemfileCompressedPath(storage.CompressionZstd)], "memfile compressed should have non-nil opts")

	// Result should have frame tables from compressed uploads.
	assert.NotNil(t, result.RootfsFrameTable, "rootfs frame table should be non-nil")
	assert.NotNil(t, result.MemfileFrameTable, "memfile frame table should be non-nil")

	// Verify uncompressed headers were uploaded via StoreBlob.
	var blobPaths []string
	for _, c := range blobCalls {
		blobPaths = append(blobPaths, c.objectPath)
	}

	assert.Contains(t, blobPaths, files.StorageRootfsHeaderPath(), "uncompressed rootfs header should be uploaded")
	assert.Contains(t, blobPaths, files.StorageMemfileHeaderPath(), "uncompressed memfile header should be uploaded")
}

func TestUploadData_CompressionDisabled(t *testing.T) { //nolint:paralleltest // mutates storage.EnableGCSCompression global
	saved := storage.EnableGCSCompression
	t.Cleanup(func() { storage.EnableGCSCompression = saved })
	storage.EnableGCSCompression = false

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	tmpDir := t.TempDir()

	rootfsPath := createTempFile(t, tmpDir, "rootfs.ext4")
	memfilePath := createTempFile(t, tmpDir, "memfile")
	snapfilePath := createTempFile(t, tmpDir, "snapfile")
	metadataPath := createTempFile(t, tmpDir, "metadata.json")

	provider := storage.NewMockStorageProvider(t)

	var mu sync.Mutex
	var storeFileCalls []storeFileCall

	provider.EXPECT().StoreFile(
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, _ string, objectPath string, opts *storage.FramedUploadOptions) (*storage.FrameTable, error) {
		mu.Lock()
		storeFileCalls = append(storeFileCalls, storeFileCall{objectPath: objectPath, opts: opts})
		mu.Unlock()

		return nil, nil
	})

	var blobMu sync.Mutex
	var blobCalls []storeBlobCall

	provider.EXPECT().StoreBlob(
		mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, objectPath string, in io.Reader) error {
		data, err := io.ReadAll(in)
		if err != nil {
			return err
		}
		blobMu.Lock()
		blobCalls = append(blobCalls, storeBlobCall{objectPath: objectPath, data: data})
		blobMu.Unlock()

		return nil
	})

	h := makeHeader(t, buildId, uint64(storage.MemoryChunkSize))
	tb := NewTemplateBuild(h, h, provider, files)

	ctx := context.Background()
	result, err := tb.UploadData(ctx, metadataPath, snapfilePath, &memfilePath, &rootfsPath)
	require.NoError(t, err)

	// Collect StoreFile paths.
	var filePaths []string
	for _, c := range storeFileCalls {
		filePaths = append(filePaths, c.objectPath)
	}

	// Only uncompressed data paths should be called via StoreFile.
	assert.Contains(t, filePaths, files.StorageRootfsPath())
	assert.Contains(t, filePaths, files.StorageMemfilePath())
	assert.NotContains(t, filePaths, files.StorageRootfsCompressedPath(storage.CompressionZstd))
	assert.NotContains(t, filePaths, files.StorageMemfileCompressedPath(storage.CompressionZstd))

	// No frame tables when compression is disabled.
	assert.Nil(t, result.RootfsFrameTable)
	assert.Nil(t, result.MemfileFrameTable)

	// Uncompressed headers should still be uploaded via StoreBlob.
	var blobPaths []string
	for _, c := range blobCalls {
		blobPaths = append(blobPaths, c.objectPath)
	}

	assert.Contains(t, blobPaths, files.StorageRootfsHeaderPath(), "uncompressed rootfs header should be uploaded")
	assert.Contains(t, blobPaths, files.StorageMemfileHeaderPath(), "uncompressed memfile header should be uploaded")
}

func TestUploadCompressedHeaders_CompressionEnabled(t *testing.T) { //nolint:paralleltest // mutates storage.EnableGCSCompression global
	saved := storage.EnableGCSCompression
	t.Cleanup(func() { storage.EnableGCSCompression = saved })
	storage.EnableGCSCompression = true

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	dataSize := uint64(storage.MemoryChunkSize)

	memfileHeader := makeHeader(t, buildId, dataSize)
	rootfsHeader := makeHeader(t, buildId, dataSize)

	// Create pending frame tables with entries for this build.
	pending := NewPendingFrameTables()
	ft := &storage.FrameTable{
		CompressionType: storage.CompressionZstd,
		StartAt:         storage.FrameOffset{U: 0, C: 0},
		Frames:          []storage.FrameSize{{U: int32(dataSize), C: int32(dataSize / 2)}},
	}
	pending.Add(buildId.String()+"/rootfs.ext4", ft)
	pending.Add(buildId.String()+"/memfile", ft)

	provider := storage.NewMockStorageProvider(t)

	var mu sync.Mutex
	var blobCalls []storeBlobCall

	provider.EXPECT().StoreBlob(
		mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, objectPath string, in io.Reader) error {
		data, err := io.ReadAll(in)
		if err != nil {
			return err
		}
		mu.Lock()
		blobCalls = append(blobCalls, storeBlobCall{objectPath: objectPath, data: data})
		mu.Unlock()

		return nil
	})

	tb := NewTemplateBuild(memfileHeader, rootfsHeader, provider, files)

	ctx := context.Background()
	err := tb.UploadCompressedHeaders(ctx, pending)
	require.NoError(t, err)

	// Should have 2 StoreBlob calls (compressed headers only).
	assert.Len(t, blobCalls, 2, "expected 2 compressed header uploads")

	// Build lookup map for verification.
	blobMap := make(map[string][]byte)
	for _, c := range blobCalls {
		blobMap[c.objectPath] = c.data
	}

	// Verify compressed rootfs header HAS frame table in its mapping.
	compressedRootfsData, ok := blobMap[files.StorageRootfsHeaderCompressedPath(storage.CompressionZstd)]
	require.True(t, ok, "compressed rootfs header should be uploaded")
	compressedRootfsH, err := header.Deserialize(compressedRootfsData)
	require.NoError(t, err)
	hasFrameTable := false
	for _, m := range compressedRootfsH.Mapping {
		if m.FrameTable != nil {
			hasFrameTable = true

			break
		}
	}
	assert.True(t, hasFrameTable, "compressed rootfs header should have frame table in mapping")

	// Verify compressed memfile header HAS frame table.
	compressedMemfileData, ok := blobMap[files.StorageMemfileHeaderCompressedPath(storage.CompressionZstd)]
	require.True(t, ok, "compressed memfile header should be uploaded")
	compressedMemfileH, err := header.Deserialize(compressedMemfileData)
	require.NoError(t, err)
	hasFrameTable = false
	for _, m := range compressedMemfileH.Mapping {
		if m.FrameTable != nil {
			hasFrameTable = true

			break
		}
	}
	assert.True(t, hasFrameTable, "compressed memfile header should have frame table in mapping")
}

func TestUploadCompressedHeaders_CompressionDisabled(t *testing.T) { //nolint:paralleltest // mutates storage.EnableGCSCompression global
	saved := storage.EnableGCSCompression
	t.Cleanup(func() { storage.EnableGCSCompression = saved })
	storage.EnableGCSCompression = false

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	dataSize := uint64(storage.MemoryChunkSize)

	memfileHeader := makeHeader(t, buildId, dataSize)
	rootfsHeader := makeHeader(t, buildId, dataSize)

	pending := NewPendingFrameTables()
	ft := &storage.FrameTable{
		CompressionType: storage.CompressionZstd,
		StartAt:         storage.FrameOffset{U: 0, C: 0},
		Frames:          []storage.FrameSize{{U: int32(dataSize), C: int32(dataSize / 2)}},
	}
	pending.Add(buildId.String()+"/rootfs.ext4", ft)
	pending.Add(buildId.String()+"/memfile", ft)

	// No mock expectations — UploadCompressedHeaders should be a complete no-op.
	provider := storage.NewMockStorageProvider(t)

	tb := NewTemplateBuild(memfileHeader, rootfsHeader, provider, files)

	ctx := context.Background()
	err := tb.UploadCompressedHeaders(ctx, pending)
	require.NoError(t, err)

	// Verify frame tables were NOT applied (headers remain unmutated).
	for _, m := range memfileHeader.Mapping {
		assert.Nil(t, m.FrameTable, "memfile header should not have frame tables when compression disabled")
	}
	for _, m := range rootfsHeader.Mapping {
		assert.Nil(t, m.FrameTable, "rootfs header should not have frame tables when compression disabled")
	}
}

func TestPendingFrameTables_ApplyToHeader(t *testing.T) {
	t.Parallel()

	buildId := uuid.New()
	dataSize := uint64(storage.MemoryChunkSize)

	h := makeHeader(t, buildId, dataSize)

	// Verify no frame tables initially.
	for _, m := range h.Mapping {
		require.Nil(t, m.FrameTable)
	}

	pending := NewPendingFrameTables()
	ft := &storage.FrameTable{
		CompressionType: storage.CompressionZstd,
		StartAt:         storage.FrameOffset{U: 0, C: 0},
		Frames:          []storage.FrameSize{{U: int32(dataSize), C: int32(dataSize / 2)}},
	}
	pending.Add(buildId.String()+"/memfile", ft)

	err := pending.ApplyToHeader(h, "memfile")
	require.NoError(t, err)

	// Verify frame tables were applied.
	for _, m := range h.Mapping {
		if m.BuildId == buildId {
			assert.NotNil(t, m.FrameTable, "mapping should have frame table after ApplyToHeader")
			assert.Equal(t, storage.CompressionZstd, m.FrameTable.CompressionType)
		}
	}
}

func TestUploadCompressedHeaders_SerializationRoundTrip(t *testing.T) { //nolint:paralleltest // mutates storage.EnableGCSCompression global
	saved := storage.EnableGCSCompression
	t.Cleanup(func() { storage.EnableGCSCompression = saved })
	storage.EnableGCSCompression = true

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	dataSize := uint64(storage.MemoryChunkSize)

	memfileHeader := makeHeader(t, buildId, dataSize)

	pending := NewPendingFrameTables()
	ft := &storage.FrameTable{
		CompressionType: storage.CompressionZstd,
		StartAt:         storage.FrameOffset{U: 0, C: 0},
		Frames:          []storage.FrameSize{{U: int32(dataSize), C: 1234}},
	}
	pending.Add(buildId.String()+"/memfile", ft)

	provider := storage.NewMockStorageProvider(t)

	blobMap := make(map[string][]byte)
	var mu sync.Mutex

	provider.EXPECT().StoreBlob(
		mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, objectPath string, in io.Reader) error {
		data, err := io.ReadAll(in)
		if err != nil {
			return err
		}
		mu.Lock()
		blobMap[objectPath] = data
		mu.Unlock()

		return nil
	})

	// Pass nil rootfsHeader to only get memfile headers.
	tb := NewTemplateBuild(memfileHeader, nil, provider, files)

	ctx := context.Background()
	err := tb.UploadCompressedHeaders(ctx, pending)
	require.NoError(t, err)

	// Verify the compressed header round-trips correctly.
	compressedData, ok := blobMap[files.StorageMemfileHeaderCompressedPath(storage.CompressionZstd)]
	require.True(t, ok)

	h, err := header.Deserialize(compressedData)
	require.NoError(t, err)

	// Find the mapping with our buildId.
	var found *header.BuildMap
	for _, m := range h.Mapping {
		if m.BuildId == buildId {
			found = m

			break
		}
	}
	require.NotNil(t, found, "should find mapping with our buildId")
	require.NotNil(t, found.FrameTable, "mapping should have frame table")
	assert.Equal(t, storage.CompressionZstd, found.FrameTable.CompressionType)
	assert.Len(t, found.FrameTable.Frames, 1)
}
