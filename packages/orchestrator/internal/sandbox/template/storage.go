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

// useCompressedAssets controls whether to try loading v4 (compressed) headers.
// Will be replaced with a feature flag.
var useCompressedAssets = true

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

// loadHeader loads and deserializes a header blob. Returns (nil, nil) if not found.
func loadHeader(ctx context.Context, persistence storage.StorageProvider, path string, objType storage.ObjectType) (*header.Header, error) {
	blob, err := persistence.OpenBlob(ctx, path, objType)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil
		}

		return nil, err
	}

	h, err := header.Deserialize(ctx, blob)
	if err != nil {
		return nil, err
	}

	return h, nil
}

// loadV4Header loads a v4 header (LZ4 compressed), decompresses, and deserializes it.
// Returns (nil, nil) if not found.
func loadV4Header(ctx context.Context, persistence storage.StorageProvider, path string, objType storage.ObjectType) (*header.Header, error) {
	data, err := storage.LoadBlob(ctx, persistence, path, objType)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil
		}

		return nil, err
	}

	return header.DeserializeV4(data)
}

// loadV3Header loads a header from the standard (uncompressed) path.
func loadV3Header(ctx context.Context, persistence storage.StorageProvider, buildId string, fileType build.DiffType, objType storage.ObjectType) (*header.Header, error) {
	files := storage.TemplateFiles{BuildID: buildId}
	path := files.HeaderPath(string(fileType))

	return loadHeader(ctx, persistence, path, objType)
}

// loadV4orV3Header fetches both compressed and default headers in parallel,
// preferring the compressed one if available.
func loadV4orV3Header(ctx context.Context, persistence storage.StorageProvider, buildId string, fileType build.DiffType, objType storage.ObjectType) (*header.Header, error) {
	files := storage.TemplateFiles{BuildID: buildId}
	defaultPath := files.HeaderPath(string(fileType))
	v4Path := files.CompressedHeaderPath(string(fileType))

	var defaultHeader, compressedHeader *header.Header
	var defaultErr, compressedErr error

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defaultHeader, defaultErr = loadHeader(egCtx, persistence, defaultPath, objType)

		return nil
	})
	eg.Go(func() error {
		compressedHeader, compressedErr = loadV4Header(egCtx, persistence, v4Path, objType)

		return nil
	})
	_ = eg.Wait()

	if compressedErr == nil && compressedHeader != nil {
		return compressedHeader, nil
	}
	if defaultErr == nil && defaultHeader != nil {
		return defaultHeader, nil
	}
	if compressedErr != nil {
		return nil, compressedErr
	}
	if defaultErr != nil {
		return nil, defaultErr
	}

	return nil, nil
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

		var err error
		if useCompressedAssets {
			h, err = loadV4orV3Header(ctx, persistence, buildId, fileType, headerObjectType)
		} else {
			h, err = loadV3Header(ctx, persistence, buildId, fileType, headerObjectType)
		}
		if err != nil {
			return nil, err
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
