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

func makeHeader(t *testing.T, buildId uuid.UUID, size uint64) *header.Header {
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

func TestUpload_AllFiles(t *testing.T) {
	t.Parallel()

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

	// Mock StoreFile for rootfs and memfile uploads.
	provider.EXPECT().StoreFile(
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, _ string, objectPath string, opts *storage.FramedUploadOptions) (*storage.FrameTable, error) {
		mu.Lock()
		storeFileCalls = append(storeFileCalls, storeFileCall{objectPath: objectPath, opts: opts})
		mu.Unlock()

		return nil, nil
	})

	// Mock StoreBlob for snapfile, metadata, and headers.
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
	errCh := tb.Upload(ctx, metadataPath, snapfilePath, &memfilePath, &rootfsPath)
	err := <-errCh
	require.NoError(t, err)

	// Verify StoreFile was called for rootfs and memfile.
	var filePaths []string
	for _, c := range storeFileCalls {
		filePaths = append(filePaths, c.objectPath)
	}

	assert.Contains(t, filePaths, files.Path(storage.RootfsName))
	assert.Contains(t, filePaths, files.Path(storage.MemfileName))

	// All StoreFile calls should have nil opts (no compression).
	for _, c := range storeFileCalls {
		assert.Nil(t, c.opts, "StoreFile should be called with nil opts (no compression)")
	}

	// Verify StoreBlob was called for headers, snapfile, and metadata.
	var blobPaths []string
	for _, c := range blobCalls {
		blobPaths = append(blobPaths, c.objectPath)
	}

	assert.Contains(t, blobPaths, files.HeaderPath(storage.RootfsName))
	assert.Contains(t, blobPaths, files.HeaderPath(storage.MemfileName))
	assert.Contains(t, blobPaths, files.Path(storage.SnapfileName))
	assert.Contains(t, blobPaths, files.Path(storage.MetadataName))
}

func TestUpload_NilDiffs(t *testing.T) {
	t.Parallel()

	buildId := uuid.New()
	files := storage.TemplateFiles{BuildID: buildId.String()}
	tmpDir := t.TempDir()

	snapfilePath := createTempFile(t, tmpDir, "snapfile")
	metadataPath := createTempFile(t, tmpDir, "metadata.json")

	provider := storage.NewMockStorageProvider(t)

	// Mock StoreBlob for snapfile, metadata, and headers.
	provider.EXPECT().StoreBlob(
		mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, _ string, in io.Reader) error {
		_, err := io.ReadAll(in)

		return err
	})

	h := makeHeader(t, buildId, uint64(storage.MemoryChunkSize))
	tb := NewTemplateBuild(h, h, provider, files)

	ctx := context.Background()
	// Pass nil for both memfile and rootfs paths
	errCh := tb.Upload(ctx, metadataPath, snapfilePath, nil, nil)
	err := <-errCh
	require.NoError(t, err)
}
