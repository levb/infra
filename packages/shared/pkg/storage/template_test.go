package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressionType_Suffix(t *testing.T) {
	t.Parallel()

	assert.Equal(t, ".zst", CompressionZstd.Suffix())
	assert.Equal(t, ".lz4", CompressionLZ4.Suffix())
	assert.Empty(t, CompressionNone.Suffix())
}

func TestFrameTable_CompressionTypeSuffix(t *testing.T) {
	t.Parallel()

	ft := &FrameTable{CompressionType: CompressionZstd}
	assert.Equal(t, ".zst", ft.CompressionTypeSuffix())

	ft = &FrameTable{CompressionType: CompressionLZ4}
	assert.Equal(t, ".lz4", ft.CompressionTypeSuffix())

	ft = &FrameTable{CompressionType: CompressionNone}
	assert.Empty(t, ft.CompressionTypeSuffix())

	// nil frame table returns empty suffix.
	var nilFT *FrameTable
	assert.Empty(t, nilFT.CompressionTypeSuffix())
}

func TestTemplateFiles_Path(t *testing.T) {
	t.Parallel()

	tf := TemplateFiles{BuildID: "test-build-123"}

	assert.Equal(t, "test-build-123/memfile", tf.Path(MemfileName))
	assert.Equal(t, "test-build-123/rootfs.ext4", tf.Path(RootfsName))
	assert.Equal(t, "test-build-123/snapfile", tf.Path(SnapfileName))
	assert.Equal(t, "test-build-123/metadata.json", tf.Path(MetadataName))
}

func TestTemplateFiles_HeaderPath(t *testing.T) {
	t.Parallel()

	tf := TemplateFiles{BuildID: "test-build-123"}

	assert.Equal(t, "test-build-123/memfile.header", tf.HeaderPath(MemfileName))
	assert.Equal(t, "test-build-123/rootfs.ext4.header", tf.HeaderPath(RootfsName))
}

func TestTemplateFiles_CompressedPath(t *testing.T) {
	t.Parallel()

	tf := TemplateFiles{BuildID: "test-build-123"}

	// CompressedPath uses DefaultCompressionOptions (currently zstd).
	assert.Equal(t, "test-build-123/memfile.zst", tf.CompressedPath(MemfileName))
	assert.Equal(t, "test-build-123/rootfs.ext4.zst", tf.CompressedPath(RootfsName))
}

func TestTemplateFiles_CompressedHeaderPath(t *testing.T) {
	t.Parallel()

	tf := TemplateFiles{BuildID: "test-build-123"}

	assert.Equal(t, "test-build-123/memfile.compressed.header.lz4", tf.CompressedHeaderPath(MemfileName))
	assert.Equal(t, "test-build-123/rootfs.ext4.compressed.header.lz4", tf.CompressedHeaderPath(RootfsName))
}
