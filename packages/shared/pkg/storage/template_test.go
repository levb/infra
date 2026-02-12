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

func TestTemplateFiles_CompressedPaths(t *testing.T) {
	t.Parallel()

	tf := TemplateFiles{BuildID: "test-build-123"}

	tests := []struct {
		name     string
		method   func(CompressionType) string
		ct       CompressionType
		expected string
	}{
		{
			name:     "StorageMemfileCompressedPath/zstd",
			method:   tf.StorageMemfileCompressedPath,
			ct:       CompressionZstd,
			expected: "test-build-123/memfile.zst",
		},
		{
			name:     "StorageMemfileHeaderCompressedPath/zstd",
			method:   tf.StorageMemfileHeaderCompressedPath,
			ct:       CompressionZstd,
			expected: "test-build-123/memfile.header.zst",
		},
		{
			name:     "StorageRootfsCompressedPath/zstd",
			method:   tf.StorageRootfsCompressedPath,
			ct:       CompressionZstd,
			expected: "test-build-123/rootfs.ext4.zst",
		},
		{
			name:     "StorageRootfsHeaderCompressedPath/zstd",
			method:   tf.StorageRootfsHeaderCompressedPath,
			ct:       CompressionZstd,
			expected: "test-build-123/rootfs.ext4.header.zst",
		},
		{
			name:     "StorageRootfsCompressedPath/lz4",
			method:   tf.StorageRootfsCompressedPath,
			ct:       CompressionLZ4,
			expected: "test-build-123/rootfs.ext4.lz4",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, tc.method(tc.ct))
		})
	}
}
