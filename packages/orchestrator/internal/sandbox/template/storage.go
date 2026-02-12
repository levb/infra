package template

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const (
	oldMemfileHugePageSize = 2 << 20 // 2 MiB
	oldRootfsBlockSize     = 2 << 11 // 4 KiB
)

type Storage struct {
	header *header.Header
	source *build.File
}

func storageHeaderObjectType(diffType build.DiffType) (storage.ObjectType, bool) {
	switch diffType {
	case build.Memfile:
		return storage.MemfileHeaderObjectType, true
	case build.Rootfs:
		return storage.RootFSHeaderObjectType, true
	default:
		return storage.UnknownObjectType, false
	}
}

func objectType(diffType build.DiffType) (storage.SeekableObjectType, bool) {
	switch diffType {
	case build.Memfile:
		return storage.MemfileObjectType, true
	case build.Rootfs:
		return storage.RootFSObjectType, true
	default:
		return storage.UnknownSeekableObjectType, false
	}
}

func NewStorage(
	ctx context.Context,
	store *build.DiffStore,
	buildId string,
	fileType build.DiffType,
	h *header.Header,
	persistence storage.StorageProvider,
	metrics blockmetrics.Metrics,
) (*Storage, error) {
	if h == nil {
		_, ok := storageHeaderObjectType(fileType)
		if !ok {
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		headerObjectPath := buildId + "/" + string(fileType) + storage.HeaderSuffix

		if storage.UseCompressedAssets {
			// Fetch both default and compressed headers in parallel.
			compressedHeaderPath := headerObjectPath + storage.DefaultCompressionOptions.CompressionType.Suffix()
			var defaultData, compressedData []byte
			var defaultErr, compressedErr error

			eg, egCtx := errgroup.WithContext(ctx)
			eg.Go(func() error {
				defaultData, defaultErr = persistence.GetBlob(egCtx, headerObjectPath)

				return nil // don't fail the group; we handle errors below
			})
			eg.Go(func() error {
				compressedData, compressedErr = persistence.GetBlob(egCtx, compressedHeaderPath)

				return nil
			})
			_ = eg.Wait()

			// Prefer compressed header if available.
			if compressedErr == nil {
				if diffHeader, err := header.Deserialize(compressedData); err == nil {
					h = diffHeader
				}
			}
			// Fall back to default header.
			if h == nil && defaultErr == nil {
				diffHeader, err := header.Deserialize(defaultData)
				if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
					return nil, fmt.Errorf("failed to deserialize header: %w", err)
				}
				if err == nil {
					h = diffHeader
				}
			}
			// If both failed with non-NotExist errors, propagate.
			if h == nil && defaultErr != nil && !errors.Is(defaultErr, storage.ErrObjectNotExist) {
				return nil, defaultErr
			}
		} else {
			headerData, err := persistence.GetBlob(ctx, headerObjectPath)
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				return nil, err
			}

			if err == nil {
				diffHeader, err := header.Deserialize(headerData)
				if err != nil {
					return nil, fmt.Errorf("failed to deserialize header: %w", err)
				}
				h = diffHeader
			}
		}
	}

	// If we can't find the diff header in storage, we try to find the "old" style template without a header as a fallback.
	if h == nil {
		objectPath := buildId + "/" + string(fileType)
		_, ok := objectType(fileType)
		if !ok {
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		size, _, err := persistence.Size(ctx, objectPath)
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

	b := build.NewFile(h, store, fileType, persistence, metrics)

	return &Storage{
		source: b,
		header: h,
	}, nil
}

func (d *Storage) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	return d.source.ReadAt(ctx, p, off)
}

func (d *Storage) Size(_ context.Context) (int64, error) {
	return int64(d.header.Metadata.Size), nil
}

func (d *Storage) BlockSize() int64 {
	return int64(d.header.Metadata.BlockSize)
}

func (d *Storage) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	return d.source.Slice(ctx, off, length)
}

func (d *Storage) Header() *header.Header {
	return d.header
}

func (d *Storage) Close() error {
	return nil
}
