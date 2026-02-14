package header

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	lz4 "github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func compressLZ4(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func TestSerializeDeserialize_V3_RoundTrip(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	baseID := uuid.New()
	metadata := &Metadata{
		Version:     3,
		BlockSize:   4096,
		Size:        8192,
		Generation:  7,
		BuildId:     buildID,
		BaseBuildId: baseID,
	}

	mappings := []*BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 0,
		},
		{
			Offset:             4096,
			Length:             4096,
			BuildId:            baseID,
			BuildStorageOffset: 123,
		},
	}

	data, err := Serialize(metadata, mappings)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	require.Equal(t, metadata.Version, got.Metadata.Version)
	require.Equal(t, metadata.BlockSize, got.Metadata.BlockSize)
	require.Equal(t, metadata.Size, got.Metadata.Size)
	require.Equal(t, metadata.Generation, got.Metadata.Generation)
	require.Equal(t, metadata.BuildId, got.Metadata.BuildId)
	require.Equal(t, metadata.BaseBuildId, got.Metadata.BaseBuildId)

	require.Len(t, got.Mapping, 2)
	assert.Equal(t, uint64(0), got.Mapping[0].Offset)
	assert.Equal(t, uint64(4096), got.Mapping[0].Length)
	assert.Equal(t, buildID, got.Mapping[0].BuildId)
	assert.Equal(t, uint64(0), got.Mapping[0].BuildStorageOffset)

	assert.Equal(t, uint64(4096), got.Mapping[1].Offset)
	assert.Equal(t, uint64(4096), got.Mapping[1].Length)
	assert.Equal(t, baseID, got.Mapping[1].BuildId)
	assert.Equal(t, uint64(123), got.Mapping[1].BuildStorageOffset)
}

func TestDeserialize_TruncatedMetadata(t *testing.T) {
	t.Parallel()

	_, err := DeserializeBytes([]byte{0x01, 0x02, 0x03})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read metadata")
}

func TestSerializeDeserialize_EmptyMappings_Defaults(t *testing.T) {
	t.Parallel()

	metadata := &Metadata{
		Version:     3,
		BlockSize:   4096,
		Size:        8192,
		Generation:  0,
		BuildId:     uuid.New(),
		BaseBuildId: uuid.New(),
	}

	data, err := Serialize(metadata, nil)
	require.NoError(t, err)

	got, err := DeserializeBytes(data)
	require.NoError(t, err)

	// NewHeader creates a default mapping when none provided
	require.Len(t, got.Mapping, 1)
	assert.Equal(t, uint64(0), got.Mapping[0].Offset)
	assert.Equal(t, metadata.Size, got.Mapping[0].Length)
	assert.Equal(t, metadata.BuildId, got.Mapping[0].BuildId)
}

func TestDeserialize_BlockSizeZero(t *testing.T) {
	t.Parallel()

	metadata := &Metadata{
		Version:     3,
		BlockSize:   0,
		Size:        4096,
		Generation:  0,
		BuildId:     uuid.New(),
		BaseBuildId: uuid.New(),
	}

	data, err := Serialize(metadata, nil)
	require.NoError(t, err)

	_, err = DeserializeBytes(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "block size cannot be zero")
}

func TestSerializeDeserialize_V4_WithFrameTable(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	baseID := uuid.New()
	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        8192,
		Generation:  1,
		BuildId:     buildID,
		BaseBuildId: baseID,
	}

	mappings := []*BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 0,
			FrameTable: &storage.FrameTable{
				CompressionType: storage.CompressionLZ4,
				StartAt:         storage.FrameOffset{U: 0, C: 0},
				Frames: []storage.FrameSize{
					{U: 2048, C: 1024},
					{U: 2048, C: 900},
				},
			},
		},
		{
			Offset:             4096,
			Length:             4096,
			BuildId:            baseID,
			BuildStorageOffset: 0,
		},
	}

	data, err := Serialize(metadata, mappings)
	require.NoError(t, err)

	got, err := DeserializeV4(compressLZ4(t, data))
	require.NoError(t, err)

	require.Equal(t, uint64(4), got.Metadata.Version)
	require.Len(t, got.Mapping, 2)

	// First mapping has FrameTable
	m0 := got.Mapping[0]
	assert.Equal(t, uint64(0), m0.Offset)
	assert.Equal(t, uint64(4096), m0.Length)
	assert.Equal(t, buildID, m0.BuildId)
	require.NotNil(t, m0.FrameTable)
	assert.Equal(t, storage.CompressionLZ4, m0.FrameTable.CompressionType)
	assert.Equal(t, int64(0), m0.FrameTable.StartAt.U)
	assert.Equal(t, int64(0), m0.FrameTable.StartAt.C)
	require.Len(t, m0.FrameTable.Frames, 2)
	assert.Equal(t, int32(2048), m0.FrameTable.Frames[0].U)
	assert.Equal(t, int32(1024), m0.FrameTable.Frames[0].C)
	assert.Equal(t, int32(2048), m0.FrameTable.Frames[1].U)
	assert.Equal(t, int32(900), m0.FrameTable.Frames[1].C)

	// Second mapping has no FrameTable
	m1 := got.Mapping[1]
	assert.Equal(t, uint64(4096), m1.Offset)
	assert.Equal(t, uint64(4096), m1.Length)
	assert.Equal(t, baseID, m1.BuildId)
	assert.Nil(t, m1.FrameTable)
}

func TestSerializeDeserialize_V4_NoCompression(t *testing.T) {
	t.Parallel()

	buildID := uuid.New()
	metadata := &Metadata{
		Version:     4,
		BlockSize:   4096,
		Size:        4096,
		Generation:  0,
		BuildId:     buildID,
		BaseBuildId: buildID,
	}

	mappings := []*BuildMap{
		{
			Offset:             0,
			Length:             4096,
			BuildId:            buildID,
			BuildStorageOffset: 0,
			// No FrameTable
		},
	}

	data, err := Serialize(metadata, mappings)
	require.NoError(t, err)

	got, err := DeserializeV4(compressLZ4(t, data))
	require.NoError(t, err)

	require.Len(t, got.Mapping, 1)
	assert.Nil(t, got.Mapping[0].FrameTable)
}
