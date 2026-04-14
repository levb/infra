package template

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const (
	oldMemfileHugePageSize = 2 << 20 // 2 MiB
	oldRootfsBlockSize     = 4 << 10 // 4 KiB
)

type Storage struct {
	source *build.File
}

func NewStorage(
	ctx context.Context,
	diffs *build.DiffStore,
	buildId string,
	fileType build.DiffType,
	h *header.Header,
	s storage.Store,
	metrics blockmetrics.Metrics,
) (*Storage, error) {
	paths := storage.Paths{BuildID: buildId}

	if h == nil {
		if fileType != build.Memfile && fileType != build.Rootfs {
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		var hdrPath string
		switch fileType {
		case build.Memfile:
			hdrPath = paths.MemfileHeader()
		case build.Rootfs:
			hdrPath = paths.RootfsHeader()
		default:
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		var err error
		h, err = header.LoadHeader(ctx, s, hdrPath)
		if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			return nil, err
		}
	}

	// If we can't find the diff header in storage, we try to find the "old" style template without a header as a fallback.
	if h == nil {
		if fileType != build.Memfile && fileType != build.Rootfs {
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		var dataPath string
		switch fileType {
		case build.Memfile:
			dataPath = paths.Memfile()
		case build.Rootfs:
			dataPath = paths.Rootfs()
		default:
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		size, err := s.Size(ctx, dataPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get object size: %w", err)
		}

		id, err := uuid.Parse(buildId)
		if err != nil {
			return nil, fmt.Errorf("failed to parse build id: %w", err)
		}

		// TODO: This is a workaround for the old style template without a header.
		// We don't know the block size of the old style template, so we set it manually.
		var blockSize uint64
		switch fileType {
		case build.Memfile:
			blockSize = oldMemfileHugePageSize
		case build.Rootfs:
			blockSize = oldRootfsBlockSize
		default:
			return nil, fmt.Errorf("unsupported file type: %s", fileType)
		}

		h, err = header.NewHeader(&header.Metadata{
			// The version is always 1 for the old style template without a header.
			Version:     1,
			BuildId:     id,
			BaseBuildId: id,
			Size:        uint64(size),
			BlockSize:   blockSize,
			Generation:  1,
		}, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create header for old style template: %w", err)
		}
	}

	b := build.NewFile(h, diffs, fileType, s, metrics)

	return &Storage{
		source: b,
	}, nil
}

func (d *Storage) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	return d.source.ReadAt(ctx, p, off)
}

func (d *Storage) Size(_ context.Context) (int64, error) {
	return int64(d.source.Header().Metadata.Size), nil
}

func (d *Storage) BlockSize() int64 {
	return int64(d.source.Header().Metadata.BlockSize)
}

func (d *Storage) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	return d.source.Slice(ctx, off, length)
}

func (d *Storage) Header() *header.Header {
	return d.source.Header()
}

func (d *Storage) Close() error {
	return nil
}
