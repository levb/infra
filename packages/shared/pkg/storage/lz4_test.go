package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLZ4_RoundTrip(t *testing.T) {
	t.Parallel()

	original := []byte("hello world — this is a test of LZ4 round-trip compression and decompression")

	compressed, err := CompressLZ4(original)
	require.NoError(t, err)
	require.NotEmpty(t, compressed)

	decompressed, err := DecompressLZ4(compressed, len(original))
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestLZ4_RoundTrip_LargeData(t *testing.T) {
	t.Parallel()

	// 1 MB of repetitive data (compresses well).
	original := make([]byte, 1<<20)
	for i := range original {
		original[i] = byte(i % 251)
	}

	compressed, err := CompressLZ4(original)
	require.NoError(t, err)
	assert.Less(t, len(compressed), len(original), "compressed should be smaller for repetitive data")

	decompressed, err := DecompressLZ4(compressed, len(original))
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestLZ4_EmptyInput(t *testing.T) {
	t.Parallel()

	compressed, err := CompressLZ4([]byte{})
	require.NoError(t, err)

	decompressed, err := DecompressLZ4(compressed, 0)
	require.NoError(t, err)
	assert.Empty(t, decompressed)
}
