package cmdutil

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// setupLocalBuild creates a temp directory structured like local storage
// and returns the storage path and build ID.
func setupLocalBuild(t *testing.T) (storagePath string, buildID string) {
	t.Helper()

	storagePath = t.TempDir()
	buildID = uuid.New().String()

	buildDir := filepath.Join(storagePath, "templates", buildID)
	require.NoError(t, os.MkdirAll(buildDir, 0o755))

	return storagePath, buildID
}

// writeTestFile writes a file into the build directory.
func writeTestFile(t *testing.T, storagePath, buildID, filename string, data []byte) {
	t.Helper()

	p := filepath.Join(storagePath, "templates", buildID, filename)
	require.NoError(t, os.WriteFile(p, data, 0o644))
}

// serializeTestHeader creates a serialized header with one mapping.
func serializeTestHeader(t *testing.T, buildID uuid.UUID, ft *storage.FrameTable) []byte {
	t.Helper()

	meta := header.NewTemplateMetadata(buildID, 4096, 8192)
	mappings := []*header.BuildMap{
		{
			Offset:             0,
			Length:             8192,
			BuildId:            buildID,
			BuildStorageOffset: 0,
			FrameTable:         ft,
		},
	}

	data, err := header.Serialize(meta, mappings)
	require.NoError(t, err)

	return data
}

func TestReadFileIfExists_FileExists(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	content := []byte("hello world")
	writeTestFile(t, storagePath, buildID, "testfile", content)

	ctx := context.Background()
	data, source, err := ReadFileIfExists(ctx, storagePath, buildID, "testfile")

	require.NoError(t, err)
	assert.Equal(t, content, data)
	assert.NotEmpty(t, source)
}

func TestReadFileIfExists_FileNotExists(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)

	ctx := context.Background()
	data, source, err := ReadFileIfExists(ctx, storagePath, buildID, "nonexistent")

	require.NoError(t, err)
	assert.Nil(t, data)
	assert.Empty(t, source)
}

func TestReadCompressedHeader_Success(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	bid := uuid.MustParse(buildID)

	// Create a header, serialize it, then LZ4-compress it
	headerData := serializeTestHeader(t, bid, nil)
	compressed, err := storage.CompressLZ4(headerData)
	require.NoError(t, err)

	// Write as .compressed.header.lz4
	writeTestFile(t, storagePath, buildID, "memfile"+storage.CompressedHeaderSuffix, compressed)

	ctx := context.Background()
	h, source, err := ReadCompressedHeader(ctx, storagePath, buildID, "memfile")

	require.NoError(t, err)
	require.NotNil(t, h)
	assert.NotEmpty(t, source)
	assert.Equal(t, bid, h.Metadata.BuildId)
	assert.Equal(t, uint64(8192), h.Metadata.Size)
}

func TestReadCompressedHeader_WithFrameTable(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	bid := uuid.MustParse(buildID)

	ft := &storage.FrameTable{
		CompressionType: storage.CompressionZstd,
		StartAt:         storage.FrameOffset{U: 0, C: 0},
		Frames: []storage.FrameSize{
			{U: 4096, C: 2048},
			{U: 4096, C: 1024},
		},
	}

	headerData := serializeTestHeader(t, bid, ft)
	compressed, err := storage.CompressLZ4(headerData)
	require.NoError(t, err)

	writeTestFile(t, storagePath, buildID, "memfile"+storage.CompressedHeaderSuffix, compressed)

	ctx := context.Background()
	h, _, err := ReadCompressedHeader(ctx, storagePath, buildID, "memfile")

	require.NoError(t, err)
	require.NotNil(t, h)
	require.Len(t, h.Mapping, 1)
	require.NotNil(t, h.Mapping[0].FrameTable)
	assert.Equal(t, storage.CompressionZstd, h.Mapping[0].FrameTable.CompressionType)
	assert.Len(t, h.Mapping[0].FrameTable.Frames, 2)
}

func TestReadCompressedHeader_NotExists(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)

	ctx := context.Background()
	h, source, err := ReadCompressedHeader(ctx, storagePath, buildID, "memfile")

	require.NoError(t, err)
	assert.Nil(t, h)
	assert.Empty(t, source)
}

func TestReadCompressedHeader_CorruptData(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)

	// Write garbage data as compressed header
	writeTestFile(t, storagePath, buildID, "memfile"+storage.CompressedHeaderSuffix, []byte("not-lz4-data"))

	ctx := context.Background()
	h, _, err := ReadCompressedHeader(ctx, storagePath, buildID, "memfile")

	require.Error(t, err)
	assert.Nil(t, h)
	assert.Contains(t, err.Error(), "decompress")
}

func TestProbeFile_Exists(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	content := []byte("hello world test data")
	writeTestFile(t, storagePath, buildID, "memfile", content)

	ctx := context.Background()
	info := ProbeFile(ctx, storagePath, buildID, "memfile")

	assert.Equal(t, "memfile", info.Name)
	assert.True(t, info.Exists)
	assert.Equal(t, int64(len(content)), info.Size)
	assert.NotEmpty(t, info.Path)
}

func TestProbeFile_NotExists(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)

	ctx := context.Background()
	info := ProbeFile(ctx, storagePath, buildID, "memfile")

	assert.Equal(t, "memfile", info.Name)
	assert.False(t, info.Exists)
	assert.Equal(t, int64(0), info.Size)
}

func TestProbeFile_AllArtifacts(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)

	// Write only some files
	writeTestFile(t, storagePath, buildID, "memfile", []byte("data"))
	writeTestFile(t, storagePath, buildID, "memfile.header", []byte("hdr"))

	ctx := context.Background()

	for _, a := range MainArtifacts() {
		// Data file
		info := ProbeFile(ctx, storagePath, buildID, a.File)
		if a.File == "memfile" {
			assert.True(t, info.Exists, "memfile should exist")
		} else {
			assert.False(t, info.Exists, "%s should not exist", a.File)
		}

		// Compressed file (not written)
		cInfo := ProbeFile(ctx, storagePath, buildID, a.CompressedFile)
		assert.False(t, cInfo.Exists, "compressed %s should not exist", a.CompressedFile)
	}
}

func TestReadFile_LocalPath(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	content := []byte("file content")
	writeTestFile(t, storagePath, buildID, "testfile", content)

	ctx := context.Background()
	data, source, err := ReadFile(ctx, storagePath, buildID, "testfile")

	require.NoError(t, err)
	assert.Equal(t, content, data)
	assert.Contains(t, source, buildID)
}

func TestReadHeader_LocalPath(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	bid := uuid.MustParse(buildID)

	headerData := serializeTestHeader(t, bid, nil)
	headerPath := buildID + "/memfile.header"
	writeTestFile(t, storagePath, buildID, "memfile.header", headerData)

	ctx := context.Background()
	data, source, err := ReadHeader(ctx, storagePath, headerPath)

	require.NoError(t, err)
	assert.Equal(t, headerData, data)
	assert.Contains(t, source, buildID)

	// Verify it deserializes correctly
	h, err := header.Deserialize(data)
	require.NoError(t, err)
	assert.Equal(t, bid, h.Metadata.BuildId)
}

func TestOpenDataFile_LocalPath(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	content := make([]byte, 8192)
	for i := range content {
		content[i] = byte(i % 256)
	}
	writeTestFile(t, storagePath, buildID, "memfile", content)

	ctx := context.Background()
	reader, size, source, err := OpenDataFile(ctx, storagePath, buildID, "memfile")
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, int64(8192), size)
	assert.Contains(t, source, buildID)

	// Test ReadAt
	buf := make([]byte, 10)
	n, err := reader.ReadAt(buf, 100)
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.Equal(t, content[100:110], buf)
}

func TestIsGCSPath(t *testing.T) {
	t.Parallel()

	assert.True(t, IsGCSPath("gs://bucket"))
	assert.True(t, IsGCSPath("gs:bucket"))
	assert.False(t, IsGCSPath("/local/path"))
	assert.False(t, IsGCSPath(".local-build"))
}

func TestNormalizeGCSPath(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "gs://bucket", NormalizeGCSPath("gs://bucket"))
	assert.Equal(t, "gs://bucket", NormalizeGCSPath("gs:bucket"))
	assert.Equal(t, "/local/path", NormalizeGCSPath("/local/path"))
}

func TestExtractBucketName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "mybucket", ExtractBucketName("gs://mybucket"))
	assert.Equal(t, "mybucket", ExtractBucketName("gs:mybucket"))
}
