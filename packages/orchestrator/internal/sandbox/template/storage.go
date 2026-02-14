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

// loadBlob loads a blob from storage. Returns (data, error).
// On ErrObjectNotExist, returns (nil, nil).
func loadBlob(ctx context.Context, persistence storage.StorageProvider, path string, objType storage.ObjectType) ([]byte, error) {
	blob, err := persistence.OpenBlob(ctx, path, objType)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil
		}

		return nil, err
	}

	data, err := storage.GetBlob(ctx, blob)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil
		}

		return nil, err
	}

	return data, nil
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
		headerObjectType, ok := storageHeaderObjectType(fileType)
		if !ok {
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}

		files := storage.TemplateFiles{BuildID: buildId}
		headerObjectPath := files.HeaderPath(string(fileType))

		if storage.UseCompressedAssets {
			// Fetch both default and compressed headers in parallel.
			compressedHeaderPath := files.CompressedHeaderPath(string(fileType))
			var defaultData, compressedData []byte
			var defaultErr, compressedErr error

			eg, egCtx := errgroup.WithContext(ctx)
			eg.Go(func() error {
				defaultData, defaultErr = loadBlob(egCtx, persistence, headerObjectPath, headerObjectType)

				return nil // don't fail the group; we handle errors below
			})
			eg.Go(func() error {
				compressedData, compressedErr = loadBlob(egCtx, persistence, compressedHeaderPath, headerObjectType)

				return nil
			})
			_ = eg.Wait()

			// Prefer compressed header if available.
			if compressedErr == nil && compressedData != nil {
				decompressed, lz4Err := storage.DecompressLZ4(compressedData, storage.MaxCompressedHeaderSize)
				if lz4Err == nil {
					if diffHeader, err := header.DeserializeBytes(decompressed); err == nil {
						h = diffHeader
					}
				}
			}
			// Fall back to default header.
			if h == nil && defaultErr == nil && defaultData != nil {
				diffHeader, err := header.DeserializeBytes(defaultData)
				if err != nil {
					return nil, fmt.Errorf("failed to deserialize header: %w", err)
				}
				h = diffHeader
			}
			// If both failed with non-nil errors, propagate.
			if h == nil && defaultErr != nil {
				return nil, defaultErr
			}
		} else {
			headerObject, err := persistence.OpenBlob(ctx, headerObjectPath, headerObjectType)
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				return nil, err
			}

			if err == nil {
				headerData, err := storage.GetBlob(ctx, headerObject)
				if err != nil {
					return nil, fmt.Errorf("failed to read header blob: %w", err)
				}

				diffHeader, err := header.DeserializeBytes(headerData)
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
		objectType, ok := objectType(fileType)
		if !ok {
			return nil, build.UnknownDiffTypeError{DiffType: fileType}
		}
		object, err := persistence.OpenSeekable(ctx, objectPath, objectType)
		if err != nil {
			return nil, err
		}

		size, err := object.Size(ctx)
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
