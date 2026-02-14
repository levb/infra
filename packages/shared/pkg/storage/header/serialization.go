package header

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	lz4 "github.com/pierrec/lz4/v4"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const metadataVersion = 3

type Metadata struct {
	Version    uint64
	BlockSize  uint64
	Size       uint64
	Generation uint64
	BuildId    uuid.UUID
	// TODO: Use the base build id when setting up the snapshot rootfs
	BaseBuildId uuid.UUID
}

type v3SerializableBuildMap struct {
	Offset             uint64
	Length             uint64
	BuildId            uuid.UUID
	BuildStorageOffset uint64
}

type v4SerializableBuildMap struct {
	Offset                   uint64
	Length                   uint64
	BuildId                  uuid.UUID
	BuildStorageOffset       uint64
	CompressionTypeNumFrames uint64 // CompressionType is stored as uint8 in the high byte, the low 24 bits are NumFrames

	// if CompressionType != CompressionNone and there are frames
	// - followed by frames offset (16 bytes)
	// - followed by frames... (16 bytes * NumFrames)
}

func NewTemplateMetadata(buildId uuid.UUID, blockSize, size uint64) *Metadata {
	return &Metadata{
		Version:     metadataVersion,
		Generation:  0,
		BlockSize:   blockSize,
		Size:        size,
		BuildId:     buildId,
		BaseBuildId: buildId,
	}
}

func (m *Metadata) NextGeneration(buildID uuid.UUID) *Metadata {
	return &Metadata{
		Version:     metadataVersion,
		Generation:  m.Generation + 1,
		BlockSize:   m.BlockSize,
		Size:        m.Size,
		BuildId:     buildID,
		BaseBuildId: m.BaseBuildId,
	}
}

func Serialize(metadata *Metadata, mappings []*BuildMap) ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to write metadata: %w", err)
	}

	var v any
	for _, mapping := range mappings {
		var offset *storage.FrameOffset
		var frames []storage.FrameSize
		if metadata.Version <= 3 {
			v = &v3SerializableBuildMap{
				Offset:             mapping.Offset,
				Length:             mapping.Length,
				BuildId:            mapping.BuildId,
				BuildStorageOffset: mapping.BuildStorageOffset,
			}
		} else {
			v4 := &v4SerializableBuildMap{
				Offset:             mapping.Offset,
				Length:             mapping.Length,
				BuildId:            mapping.BuildId,
				BuildStorageOffset: mapping.BuildStorageOffset,
			}
			if mapping.FrameTable != nil {
				v4.CompressionTypeNumFrames = uint64(mapping.FrameTable.CompressionType)<<24 | uint64(len(mapping.FrameTable.Frames))
				offset = &mapping.FrameTable.StartAt
				frames = mapping.FrameTable.Frames
			}
			v = v4
		}

		err := binary.Write(&buf, binary.LittleEndian, v)
		if err != nil {
			return nil, fmt.Errorf("failed to write block mapping: %w", err)
		}
		if offset != nil {
			err := binary.Write(&buf, binary.LittleEndian, offset)
			if err != nil {
				return nil, fmt.Errorf("failed to write compression frames starting offset: %w", err)
			}
		}
		for _, frame := range frames {
			err := binary.Write(&buf, binary.LittleEndian, frame)
			if err != nil {
				return nil, fmt.Errorf("failed to write compression frame: %w", err)
			}
		}
	}

	return buf.Bytes(), nil
}

func Deserialize(ctx context.Context, in storage.Blob) (*Header, error) {
	data, err := storage.GetBlob(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("failed to write to buffer: %w", err)
	}

	return DeserializeBytes(data)
}

func DeserializeBytes(data []byte) (*Header, error) {
	var metadata Metadata
	reader := bytes.NewReader(data)
	err := binary.Read(reader, binary.LittleEndian, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	mappings := make([]*BuildMap, 0)

	for {
		var v3 v3SerializableBuildMap
		err = binary.Read(reader, binary.LittleEndian, &v3)
		if errors.Is(err, io.EOF) {
			break
		}

		mappings = append(mappings, &BuildMap{
			Offset:             v3.Offset,
			Length:             v3.Length,
			BuildId:            v3.BuildId,
			BuildStorageOffset: v3.BuildStorageOffset,
		})
	}

	return newValidatedHeader(&metadata, mappings)
}

// DeserializeV4 decompresses LZ4 data and deserializes a v4 header with frame tables.
func DeserializeV4(data []byte) (*Header, error) {
	decompressed, err := io.ReadAll(lz4.NewReader(bytes.NewReader(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to decompress v4 header: %w", err)
	}

	var metadata Metadata
	reader := bytes.NewReader(decompressed)
	err = binary.Read(reader, binary.LittleEndian, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	mappings := make([]*BuildMap, 0)

	for {
		var v4 v4SerializableBuildMap
		err = binary.Read(reader, binary.LittleEndian, &v4)
		if errors.Is(err, io.EOF) {
			break
		}

		m := BuildMap{
			Offset:             v4.Offset,
			Length:             v4.Length,
			BuildId:            v4.BuildId,
			BuildStorageOffset: v4.BuildStorageOffset,
		}

		if v4.CompressionTypeNumFrames != 0 {
			m.FrameTable = &storage.FrameTable{
				CompressionType: storage.CompressionType((v4.CompressionTypeNumFrames >> 24) & 0xFF),
			}
			numFrames := v4.CompressionTypeNumFrames & 0xFFFFFF

			var startAt storage.FrameOffset
			err = binary.Read(reader, binary.LittleEndian, &startAt)
			if err != nil {
				return nil, fmt.Errorf("failed to read compression frames starting offset: %w", err)
			}
			m.FrameTable.StartAt = startAt

			for range numFrames {
				var frame storage.FrameSize
				err = binary.Read(reader, binary.LittleEndian, &frame)
				if err != nil {
					return nil, fmt.Errorf("failed to read the expected compression frame: %w", err)
				}
				m.FrameTable.Frames = append(m.FrameTable.Frames, frame)
			}
		}

		mappings = append(mappings, &m)
	}

	return newValidatedHeader(&metadata, mappings)
}

func newValidatedHeader(metadata *Metadata, mappings []*BuildMap) (*Header, error) {
	header, err := NewHeader(metadata, mappings)
	if err != nil {
		return nil, err
	}

	if err := ValidateHeader(header); err != nil {
		return nil, fmt.Errorf("header validation failed: %w", err)
	}

	return header, nil
}
