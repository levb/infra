package header

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bits-and-blooms/bitset"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	// metadataVersion is used by template-manager for uncompressed builds (V3 headers).
	metadataVersion = 3
	// MetadataVersionCompressed is used for compressed builds (V4 headers with FrameTables).
	MetadataVersionCompressed = 4
)

type Metadata struct {
	Version    uint64
	BlockSize  uint64
	Size       uint64
	Generation uint64
	BuildId    uuid.UUID
	// TODO: Use the base build id when setting up the snapshot rootfs
	BaseBuildId uuid.UUID
}

func NewTemplateMetadata(buildId uuid.UUID, blockSize, size int) *Metadata {
	return &Metadata{
		Version:     metadataVersion,
		Generation:  0,
		BlockSize:   uint64(blockSize),
		Size:        uint64(size),
		BuildId:     buildId,
		BaseBuildId: buildId,
	}
}

func (m *Metadata) NextGeneration(buildID uuid.UUID) *Metadata {
	return &Metadata{
		Version:     m.Version,
		Generation:  m.Generation + 1,
		BlockSize:   m.BlockSize,
		Size:        m.Size,
		BuildId:     buildID,
		BaseBuildId: m.BaseBuildId,
	}
}

// metadataSize is the binary size of the Metadata struct, computed from the struct layout.
var metadataSize = binary.Size(Metadata{})

func deserializeMetadata(data []byte) (*Metadata, error) {
	var metadata Metadata

	err := binary.Read(bytes.NewReader(data), binary.LittleEndian, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	return &metadata, nil
}

var ignoreBuildID = uuid.Nil

type DiffMetadata struct {
	Dirty *bitset.BitSet
	Empty *bitset.BitSet

	BlockSize int
}

func (d *DiffMetadata) toDiffMapping(
	ctx context.Context,
	buildID uuid.UUID,
) ([]*BuildMap, error) {
	dirtyMappings := CreateMapping(
		&buildID,
		d.Dirty,
		d.BlockSize,
	)
	telemetry.ReportEvent(ctx, "created dirty mapping")

	emptyMappings := CreateMapping(
		// This buildID is intentionally ignored for nil blocks
		&ignoreBuildID,
		d.Empty,
		d.BlockSize,
	)
	telemetry.ReportEvent(ctx, "created empty mapping")

	mappings, err := MergeMappings(dirtyMappings, emptyMappings)
	if err != nil {
		return nil, fmt.Errorf("merge dirty+empty mappings: %w", err)
	}
	telemetry.ReportEvent(ctx, "merge mappings")

	return mappings, nil
}

func (d *DiffMetadata) ToDiffHeader(
	ctx context.Context,
	originalHeader *Header,
	buildID uuid.UUID,
) (h *Header, e error) {
	ctx, span := tracer.Start(ctx, "to diff-header")
	defer span.End()
	defer func() {
		if e != nil {
			span.RecordError(e)
			span.SetStatus(codes.Error, e.Error())
		}
	}()

	diffMapping, err := d.toDiffMapping(ctx, buildID)
	if err != nil {
		return nil, fmt.Errorf("toDiffMapping: %w", err)
	}

	m, err := MergeMappings(
		originalHeader.Mapping,
		diffMapping,
	)
	if err != nil {
		return nil, fmt.Errorf("merge base+diff mappings: %w", err)
	}
	telemetry.ReportEvent(ctx, "merged mappings")

	// TODO: We can run normalization only when empty mappings are not empty for this snapshot
	m = NormalizeMappings(m)
	telemetry.ReportEvent(ctx, "normalized mappings")

	metadata := originalHeader.Metadata.NextGeneration(buildID)

	telemetry.SetAttributes(ctx,
		attribute.Int("snapshot.header.mappings.length", len(m)),
		attribute.Int("snapshot.diff.size", int(d.Dirty.Count()*uint(originalHeader.Metadata.BlockSize))),
		attribute.Int("snapshot.mapped_size", int(metadata.Size)),
		attribute.Int("snapshot.block_size", int(metadata.BlockSize)),
		attribute.Int("snapshot.metadata.version", int(metadata.Version)),
		attribute.Int("snapshot.metadata.generation", int(metadata.Generation)),
		attribute.String("snapshot.metadata.build_id", metadata.BuildId.String()),
		attribute.String("snapshot.metadata.base_build_id", metadata.BaseBuildId.String()),
	)

	header, err := NewHeader(metadata, m)
	if err != nil {
		return nil, fmt.Errorf("failed to create header: %w", err)
	}

	// Copy only BuildFiles referenced by the merged mappings.
	referenced := make(map[uuid.UUID]struct{}, len(m))
	for _, mapping := range m {
		referenced[mapping.BuildId] = struct{}{}
	}
	header.BuildFiles = make(map[uuid.UUID]BuildFileInfo, len(referenced))
	for id := range referenced {
		if info, ok := originalHeader.BuildFiles[id]; ok {
			header.BuildFiles[id] = info
		}
	}

	err = ValidateMappings(header.Mapping, int(header.Metadata.Size), int(header.Metadata.BlockSize))
	if err != nil {
		if header.IsNormalizeFixApplied() {
			return nil, fmt.Errorf("invalid header mappings: %w", err)
		}

		logger.L().Warn(ctx, "header mappings are invalid, but normalize fix is not applied", zap.Error(err), logger.WithBuildID(header.Metadata.BuildId.String()))
	}

	return header, nil
}

type DiffMetadataBuilder struct {
	dirty *bitset.BitSet
	empty *bitset.BitSet

	blockSize int
}

func NewDiffMetadataBuilder(size, blockSize int) *DiffMetadataBuilder {
	return &DiffMetadataBuilder{
		// TODO: We might be able to start with 0 as preallocating here actually takes space.
		dirty: bitset.New(uint(TotalBlocks(size, blockSize))),
		empty: bitset.New(0),

		blockSize: blockSize,
	}
}

func (b *DiffMetadataBuilder) AddDirtyOffset(offset int) {
	b.dirty.Set(uint(BlockIdx(offset, b.blockSize)))
}

func (b *DiffMetadataBuilder) Process(ctx context.Context, block []byte, out io.Writer, offset int) error {
	blockIdx := BlockIdx(offset, b.blockSize)

	isEmpty, err := IsEmptyBlock(block, b.blockSize)
	if err != nil {
		return fmt.Errorf("error checking empty block: %w", err)
	}
	if isEmpty {
		b.empty.Set(uint(blockIdx))

		return nil
	}

	b.dirty.Set(uint(blockIdx))
	n, err := out.Write(block)
	if err != nil {
		logger.L().Error(ctx, "error writing to out", zap.Error(err))

		return err
	}

	if n != b.blockSize {
		return fmt.Errorf("short write: %d != %d", n, b.blockSize)
	}

	return nil
}

func (b *DiffMetadataBuilder) Build() *DiffMetadata {
	return &DiffMetadata{
		Dirty:     b.dirty,
		Empty:     b.empty,
		BlockSize: b.blockSize,
	}
}
