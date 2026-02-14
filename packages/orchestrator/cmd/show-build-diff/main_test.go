package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

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

func makeTestHeader(t *testing.T, buildID, baseBuildID uuid.UUID, ft *storage.FrameTable) *header.Header {
	t.Helper()

	meta := header.NewTemplateMetadata(buildID, 4096, 8192)
	meta.BaseBuildId = baseBuildID

	mappings := []*header.BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            baseBuildID,
			BuildStorageOffset: 0,
		},
		{
			Offset:             4096,
			Length:             4096,
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

//nolint:paralleltest // modifies os.Stdout
func TestPrintDiffSummary_Uncompressed(t *testing.T) {
	buildID := uuid.New()
	baseID := uuid.New()
	h := makeTestHeader(t, buildID, baseID, nil)

	output := captureStdout(t, func() {
		printDiffSummary("Base", h)
	})

	assert.Contains(t, output, "Base:")
	assert.Contains(t, output, "2 mappings")
	assert.Contains(t, output, "2 builds")
	assert.Contains(t, output, "(current)")
	assert.Contains(t, output, "(parent)")
	assert.Contains(t, output, "MiB")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintDiffSummary_WithCompression(t *testing.T) {
	buildID := uuid.New()
	baseID := uuid.New()
	ft := &storage.FrameTable{
		CompressionType: storage.CompressionZstd,
		StartAt:         storage.FrameOffset{U: 0, C: 0},
		Frames: []storage.FrameSize{
			{U: 4096, C: 2048},
		},
	}
	h := makeTestHeader(t, buildID, baseID, ft)

	output := captureStdout(t, func() {
		printDiffSummary("Diff", h)
	})

	assert.Contains(t, output, "Diff:")
	assert.Contains(t, output, "(current)")
	// The current build mapping should show compression ratio
	assert.Contains(t, output, "U=")
	assert.Contains(t, output, "C=")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintDiffSummary_SingleBuild(t *testing.T) {
	buildID := uuid.New()
	meta := header.NewTemplateMetadata(buildID, 4096, 4096)
	mappings := []*header.BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 0,
		},
	}

	data, err := header.Serialize(meta, mappings)
	require.NoError(t, err)

	h, err := header.Deserialize(data)
	require.NoError(t, err)

	output := captureStdout(t, func() {
		printDiffSummary("Test", h)
	})

	assert.Contains(t, output, "Test:")
	assert.Contains(t, output, "1 mappings")
	assert.Contains(t, output, "1 builds")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintDiffSummary_WithSparse(t *testing.T) {
	buildID := uuid.New()
	meta := header.NewTemplateMetadata(buildID, 4096, 8192)
	mappings := []*header.BuildMap{
		{
			Offset:  0,
			Length:  4096,
			BuildId: uuid.Nil, // sparse
		},
		{
			Offset:             4096,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 0,
		},
	}

	data, err := header.Serialize(meta, mappings)
	require.NoError(t, err)

	h, err := header.Deserialize(data)
	require.NoError(t, err)

	output := captureStdout(t, func() {
		printDiffSummary("Test", h)
	})

	assert.Contains(t, output, "(sparse)")
	assert.Contains(t, output, "(current)")
}
