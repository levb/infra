package main

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

func makeHeader(t *testing.T, buildID uuid.UUID, baseBuildID uuid.UUID) *header.Header {
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
		},
	}

	data, err := header.Serialize(meta, mappings)
	require.NoError(t, err)

	h, err := header.Deserialize(data)
	require.NoError(t, err)

	return h
}

func TestGetReferencedData_Memfile(t *testing.T) {
	t.Parallel()

	currentBuild := uuid.New()
	baseBuild := uuid.New()
	h := makeHeader(t, currentBuild, baseBuild)

	refs := getReferencedData(h, storage.MemfileHeaderObjectType)

	// Should have references for both builds (excluding nil UUID)
	assert.Len(t, refs, 2)

	// Both should be memfile paths
	for _, ref := range refs {
		assert.Contains(t, ref, storage.MemfileName)
	}

	// Should reference both build IDs
	refStr := refs[0] + " " + refs[1]
	assert.Contains(t, refStr, currentBuild.String())
	assert.Contains(t, refStr, baseBuild.String())
}

func TestGetReferencedData_Rootfs(t *testing.T) {
	t.Parallel()

	currentBuild := uuid.New()
	baseBuild := uuid.New()
	h := makeHeader(t, currentBuild, baseBuild)

	refs := getReferencedData(h, storage.RootFSHeaderObjectType)

	assert.Len(t, refs, 2)

	for _, ref := range refs {
		assert.Contains(t, ref, storage.RootfsName)
	}
}

func TestGetReferencedData_ExcludesNilUUID(t *testing.T) {
	t.Parallel()

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

	refs := getReferencedData(h, storage.MemfileHeaderObjectType)

	// Only current build, not nil UUID
	assert.Len(t, refs, 1)
	assert.Contains(t, refs[0], buildID.String())
}

func TestGetCompressedDataReferences_Memfile(t *testing.T) {
	t.Parallel()

	currentBuild := uuid.New()
	baseBuild := uuid.New()
	h := makeHeader(t, currentBuild, baseBuild)

	refs := getCompressedDataReferences(h, storage.MemfileHeaderObjectType)

	assert.Len(t, refs, 2)

	for _, ref := range refs {
		assert.Contains(t, ref, storage.MemfileName)
		assert.Contains(t, ref, ".zst")
	}
}

func TestGetCompressedDataReferences_Rootfs(t *testing.T) {
	t.Parallel()

	currentBuild := uuid.New()
	baseBuild := uuid.New()
	h := makeHeader(t, currentBuild, baseBuild)

	refs := getCompressedDataReferences(h, storage.RootFSHeaderObjectType)

	assert.Len(t, refs, 2)

	for _, ref := range refs {
		assert.Contains(t, ref, storage.RootfsName)
		assert.Contains(t, ref, ".zst")
	}
}

func TestGetCompressedDataReferences_ExcludesNilUUID(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	meta := header.NewTemplateMetadata(buildID, 4096, 8192)
	mappings := []*header.BuildMap{
		{
			Offset:  0,
			Length:  4096,
			BuildId: uuid.Nil,
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

	refs := getCompressedDataReferences(h, storage.MemfileHeaderObjectType)

	assert.Len(t, refs, 1)
	assert.Contains(t, refs[0], buildID.String())
	assert.Contains(t, refs[0], ".zst")
}

func TestNewHeaderFromPath_Valid(t *testing.T) {
	t.Parallel()

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

	tmpDir := t.TempDir()
	headerPath := filepath.Join("build1", "memfile.header")
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "build1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, headerPath), data, 0o644))

	h, err := NewHeaderFromPath(context.Background(), tmpDir, headerPath)
	require.NoError(t, err)
	assert.Equal(t, buildID, h.Metadata.BuildId)
}

func TestNewHeaderFromPath_MissingFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	_, err := NewHeaderFromPath(context.Background(), tmpDir, "nonexistent/memfile.header")
	require.Error(t, err)
}

func TestNewHeaderFromPath_CorruptData(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	headerPath := filepath.Join("build1", "memfile.header")
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "build1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, headerPath), []byte("garbage"), 0o644))

	_, err := NewHeaderFromPath(context.Background(), tmpDir, headerPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deserialize")
}
