package cmdutil

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func TestFormatMappingWithCompression_Uncompressed(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	mapping := &header.BuildMap{
		Offset:             0,
		Length:             4096,
		BuildId:            buildID,
		BuildStorageOffset: 0,
	}

	result := FormatMappingWithCompression(mapping, 4096)

	assert.Contains(t, result, "[uncompressed]")
	assert.Contains(t, result, buildID.String()[:8])
}

func TestFormatMappingWithCompression_Compressed(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	mapping := &header.BuildMap{
		Offset:             0,
		Length:             8192,
		BuildId:            buildID,
		BuildStorageOffset: 0,
		FrameTable: &storage.FrameTable{
			CompressionType: storage.CompressionZstd,
			StartAt:         storage.FrameOffset{U: 0, C: 0},
			Frames: []storage.FrameSize{
				{U: 4096, C: 2048},
				{U: 4096, C: 1024},
			},
		},
	}

	result := FormatMappingWithCompression(mapping, 4096)

	assert.Contains(t, result, "zstd")
	assert.Contains(t, result, "2 frames")
	assert.Contains(t, result, "U=8192")
	assert.Contains(t, result, "C=3072")
	assert.Contains(t, result, "ratio=")
	assert.NotContains(t, result, "[uncompressed]")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintCompressionSummary_AllUncompressed(t *testing.T) {
	buildID := uuid.New()
	h := makeTestHeader(t, buildID, nil)

	output := captureStdout(t, func() {
		PrintCompressionSummary(h)
	})

	assert.Contains(t, output, "0 compressed, 1 uncompressed")
	assert.Contains(t, output, "All mappings are uncompressed")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintCompressionSummary_WithCompression(t *testing.T) {
	buildID := uuid.New()
	ft := &storage.FrameTable{
		CompressionType: storage.CompressionZstd,
		StartAt:         storage.FrameOffset{U: 0, C: 0},
		Frames: []storage.FrameSize{
			{U: 4096, C: 2048},
			{U: 4096, C: 1024},
		},
	}
	h := makeTestHeader(t, buildID, ft)

	output := captureStdout(t, func() {
		PrintCompressionSummary(h)
	})

	assert.Contains(t, output, "1 compressed, 0 uncompressed")
	assert.Contains(t, output, "Total frames:")
	assert.Contains(t, output, "Compression ratio:")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintCompressionSummary_AllSparse(t *testing.T) {
	h := &header.Header{
		Metadata: &header.Metadata{
			Version:     4,
			BlockSize:   4096,
			Size:        4096,
			BuildId:     uuid.New(),
			BaseBuildId: uuid.New(),
		},
		Mapping: []*header.BuildMap{
			{
				Offset:  0,
				Length:  4096,
				BuildId: uuid.UUID{}, // nil UUID = sparse
			},
		},
	}

	output := captureStdout(t, func() {
		PrintCompressionSummary(h)
	})

	assert.Contains(t, output, "No data mappings (all sparse)")
}

// makeTestHeader creates a valid header with one mapping covering the full size.
func makeTestHeader(t *testing.T, buildID uuid.UUID, ft *storage.FrameTable) *header.Header {
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

	h, err := header.Deserialize(data)
	require.NoError(t, err)

	return h
}

// captureStdout captures stdout output during fn execution.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, err = buf.ReadFrom(r)
	require.NoError(t, err)

	return buf.String()
}

func TestNilUUID(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "00000000-0000-0000-0000-000000000000", NilUUID)
	assert.Equal(t, NilUUID, uuid.Nil.String())
}

func TestMainArtifacts_IncludesCompressedFields(t *testing.T) {
	t.Parallel()

	artifacts := MainArtifacts()
	require.Len(t, artifacts, 2)

	for _, a := range artifacts {
		assert.NotEmpty(t, a.CompressedFile, "CompressedFile should be set for %s", a.Name)
		assert.NotEmpty(t, a.CompressedHeaderFile, "CompressedHeaderFile should be set for %s", a.Name)
		assert.True(t, strings.HasSuffix(a.CompressedFile, ".zst"), "CompressedFile should end with .zst for %s", a.Name)
		assert.True(t, strings.HasSuffix(a.CompressedHeaderFile, ".compressed.header.lz4"), "CompressedHeaderFile should end with .compressed.header.lz4 for %s", a.Name)
	}
}

func TestSmallArtifacts_IncludesCompressedHeaders(t *testing.T) {
	t.Parallel()

	artifacts := SmallArtifacts()

	var names []string
	for _, a := range artifacts {
		names = append(names, a.Name)
	}

	assert.Contains(t, names, "Rootfs compressed header")
	assert.Contains(t, names, "Memfile compressed header")
	assert.Contains(t, names, "Snapfile")
	assert.Contains(t, names, "Metadata")
}
