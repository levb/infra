package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// --- Test helpers ---

func setupLocalBuild(t *testing.T) (storagePath string, buildID string) {
	t.Helper()

	storagePath = t.TempDir()
	buildID = uuid.New().String()

	buildDir := filepath.Join(storagePath, "templates", buildID)
	require.NoError(t, os.MkdirAll(buildDir, 0o755))

	return storagePath, buildID
}

func writeFile(t *testing.T, storagePath, buildID, filename string, data []byte) {
	t.Helper()

	p := filepath.Join(storagePath, "templates", buildID, filename)
	require.NoError(t, os.WriteFile(p, data, 0o644))
}

func serializeHeader(t *testing.T, buildID uuid.UUID, blockSize, size uint64, ft *storage.FrameTable) []byte {
	t.Helper()

	meta := header.NewTemplateMetadata(buildID, blockSize, size)
	mappings := []*header.BuildMap{
		{
			Offset:             0,
			Length:             size,
			BuildId:            buildID,
			BuildStorageOffset: 0,
			FrameTable:         ft,
		},
	}

	data, err := header.Serialize(meta, mappings)
	require.NoError(t, err)

	return data
}

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

// --- formatSize tests ---

func TestFormatSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		size     int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
		{1073741824, "1.0 GiB"},
		{1610612736, "1.5 GiB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, formatSize(tt.size))
		})
	}
}

// --- printHeader tests ---

//nolint:paralleltest // modifies os.Stdout
func TestPrintHeader_Full(t *testing.T) {
	buildID := uuid.New()
	h := deserializeTestHeader(t, buildID, 4096, 8192, nil)

	output := captureStdout(t, func() {
		printHeader(h, "test-source", false)
	})

	assert.Contains(t, output, "METADATA")
	assert.Contains(t, output, "test-source")
	assert.Contains(t, output, buildID.String())
	assert.Contains(t, output, "MAPPING")
	assert.Contains(t, output, "MAPPING SUMMARY")
	assert.Contains(t, output, "COMPRESSION SUMMARY")
	assert.Contains(t, output, "(current)")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintHeader_SummaryOnly(t *testing.T) {
	buildID := uuid.New()
	h := deserializeTestHeader(t, buildID, 4096, 8192, nil)

	output := captureStdout(t, func() {
		printHeader(h, "test-source", true)
	})

	assert.Contains(t, output, "METADATA")
	assert.Contains(t, output, "MAPPING SUMMARY")
	// Summary mode should NOT contain the per-mapping section with "[uncompressed]"
	assert.NotContains(t, output, "[uncompressed]")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintHeader_WithCompression(t *testing.T) {
	buildID := uuid.New()
	ft := &storage.FrameTable{
		CompressionType: storage.CompressionZstd,
		StartAt:         storage.FrameOffset{U: 0, C: 0},
		Frames: []storage.FrameSize{
			{U: 4096, C: 2048},
			{U: 4096, C: 1024},
		},
	}
	h := deserializeTestHeader(t, buildID, 4096, 8192, ft)

	output := captureStdout(t, func() {
		printHeader(h, "test-source", false)
	})

	assert.Contains(t, output, "zstd")
	assert.Contains(t, output, "compressed")
}

// --- printFileList tests ---

//nolint:paralleltest // modifies os.Stdout
func TestPrintFileList_WithSomeFiles(t *testing.T) {
	storagePath, buildID := setupLocalBuild(t)

	// Write only memfile and its header
	writeFile(t, storagePath, buildID, "memfile", make([]byte, 8192))
	writeFile(t, storagePath, buildID, "memfile.header", make([]byte, 100))
	writeFile(t, storagePath, buildID, "snapfile", make([]byte, 50))

	ctx := context.Background()
	output := captureStdout(t, func() {
		printFileList(ctx, storagePath, buildID)
	})

	assert.Contains(t, output, "FILES for build")
	assert.Contains(t, output, "memfile")
	assert.Contains(t, output, "memfile.header")
	assert.Contains(t, output, "snapfile")

	// Memfile should show "yes", rootfs should show "no"
	assert.Contains(t, output, "yes")
	assert.Contains(t, output, "no")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintFileList_EmptyBuild(t *testing.T) {
	storagePath, buildID := setupLocalBuild(t)

	ctx := context.Background()
	output := captureStdout(t, func() {
		printFileList(ctx, storagePath, buildID)
	})

	// All files should show "no"
	assert.NotContains(t, output, "  yes  ")
}

//nolint:paralleltest // modifies os.Stdout
func TestPrintFileList_AllFiles(t *testing.T) {
	storagePath, buildID := setupLocalBuild(t)

	// Write all expected files
	for _, a := range cmdutil.MainArtifacts() {
		writeFile(t, storagePath, buildID, a.File, make([]byte, 1024))
		writeFile(t, storagePath, buildID, a.HeaderFile, make([]byte, 100))
		writeFile(t, storagePath, buildID, a.CompressedFile, make([]byte, 512))
		writeFile(t, storagePath, buildID, a.CompressedHeaderFile, make([]byte, 80))
	}
	writeFile(t, storagePath, buildID, storage.SnapfileName, make([]byte, 50))
	writeFile(t, storagePath, buildID, storage.MetadataName, []byte(`{}`))

	ctx := context.Background()
	output := captureStdout(t, func() {
		printFileList(ctx, storagePath, buildID)
	})

	// All lines should show "yes"
	assert.NotContains(t, output, "    no    ")
}

// --- validateArtifact tests ---

func TestValidateArtifact_Valid(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	bid := uuid.MustParse(buildID)

	blockSize := uint64(4096)
	size := uint64(8192)

	// Write header
	headerData := serializeHeader(t, bid, blockSize, size, nil)
	writeFile(t, storagePath, buildID, "memfile.header", headerData)

	// Write data file (must match size — uncompressed mapping expects data at BuildStorageOffset)
	writeFile(t, storagePath, buildID, "memfile", make([]byte, size))

	ctx := context.Background()
	err := validateArtifact(ctx, storagePath, buildID, "memfile")
	require.NoError(t, err)
}

func TestValidateArtifact_MissingHeader(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)

	ctx := context.Background()
	err := validateArtifact(ctx, storagePath, buildID, "memfile")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read header")
}

func TestValidateArtifact_MissingData(t *testing.T) {
	t.Parallel()

	storagePath, buildID := setupLocalBuild(t)
	bid := uuid.MustParse(buildID)

	headerData := serializeHeader(t, bid, 4096, 8192, nil)
	writeFile(t, storagePath, buildID, "memfile.header", headerData)
	// Don't write data file

	ctx := context.Background()
	err := validateArtifact(ctx, storagePath, buildID, "memfile")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open data file")
}

//nolint:paralleltest // modifies os.Stdout
func TestValidateArtifact_ValidWithCompressedHeader(t *testing.T) {
	storagePath, buildID := setupLocalBuild(t)
	bid := uuid.MustParse(buildID)

	blockSize := uint64(4096)
	size := uint64(8192)

	// Write default header
	headerData := serializeHeader(t, bid, blockSize, size, nil)
	writeFile(t, storagePath, buildID, "memfile.header", headerData)

	// Write compressed header too
	compressed, err := storage.CompressLZ4(headerData)
	require.NoError(t, err)
	writeFile(t, storagePath, buildID, "memfile"+storage.CompressedHeaderSuffix, compressed)

	// Write data
	writeFile(t, storagePath, buildID, "memfile", make([]byte, size))

	ctx := context.Background()

	output := captureStdout(t, func() {
		err = validateArtifact(ctx, storagePath, buildID, "memfile")
	})

	require.NoError(t, err)
	assert.Contains(t, output, "Compressed header: validated")
}

// --- calculateCOffset tests ---

func TestCalculateCOffset(t *testing.T) {
	t.Parallel()

	ft := &storage.FrameTable{
		StartAt: storage.FrameOffset{C: 100},
		Frames: []storage.FrameSize{
			{U: 4096, C: 2048},
			{U: 4096, C: 1024},
			{U: 4096, C: 512},
		},
	}

	assert.Equal(t, int64(100), calculateCOffset(ft, 0))
	assert.Equal(t, int64(100+2048), calculateCOffset(ft, 1))
	assert.Equal(t, int64(100+2048+1024), calculateCOffset(ft, 2))
}

// --- helper ---

func deserializeTestHeader(t *testing.T, buildID uuid.UUID, blockSize, size uint64, ft *storage.FrameTable) *header.Header {
	t.Helper()

	data := serializeHeader(t, buildID, blockSize, size, ft)
	h, err := header.Deserialize(data)
	require.NoError(t, err)

	return h
}
