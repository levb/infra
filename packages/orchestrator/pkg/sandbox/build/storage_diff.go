package build

import (
	"context"
	"fmt"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type StorageDiff struct {
	chunker     *utils.SetOnce[*block.Chunker]
	cachePath   string
	cacheKey    DiffStoreKey
	storagePath string

	blockSize        int64
	metrics          blockmetrics.Metrics
	store      storage.Store
	featureFlags     *featureflags.Client
	uncompressedSize int64 // 0 means unknown (fall back to Size() call)
}

var _ Diff = (*StorageDiff)(nil)

type UnknownDiffTypeError struct {
	DiffType DiffType
}

func (e UnknownDiffTypeError) Error() string {
	return fmt.Sprintf("unknown diff type: %s", e.DiffType)
}

func newStorageDiff(
	basePath string,
	buildId string,
	diffType DiffType,
	blockSize int64,
	metrics blockmetrics.Metrics,
	s storage.Store,
	uncompressedSize int64,
	ct storage.CompressionType,
	ff *featureflags.Client,
) (*StorageDiff, error) {
	if diffType != Memfile && diffType != Rootfs {
		return nil, UnknownDiffTypeError{diffType}
	}

	cachePath := GenerateDiffCachePath(basePath, buildId, diffType)

	return &StorageDiff{
		storagePath:      storage.Paths{BuildID: buildId}.DataFile(string(diffType), ct),
		cachePath:        cachePath,
		chunker:          utils.NewSetOnce[*block.Chunker](),
		blockSize:        blockSize,
		metrics:          metrics,
		store:            s,
		featureFlags:     ff,
		uncompressedSize: uncompressedSize,
		cacheKey:         GetDiffStoreKey(buildId, diffType),
	}, nil
}

func (b *StorageDiff) CacheKey() DiffStoreKey {
	return b.cacheKey
}

func (b *StorageDiff) Init(ctx context.Context) error {
	size := b.uncompressedSize
	if size == 0 {
		var err error
		size, err = b.store.Size(ctx, b.storagePath)
		if err != nil {
			errMsg := fmt.Errorf("failed to get object size: %w", err)
			b.chunker.SetError(errMsg)

			return errMsg
		}
	}

	c, err := block.NewChunker(ctx, b.featureFlags, size, b.blockSize, b.store, b.storagePath, b.cachePath, b.metrics)
	if err != nil {
		errMsg := fmt.Errorf("failed to create chunker: %w", err)
		b.chunker.SetError(errMsg)

		return errMsg
	}

	return b.chunker.SetValue(c)
}

func (b *StorageDiff) Close() error {
	c, err := b.chunker.Wait()
	if err != nil {
		return err
	}

	return c.Close()
}

func (b *StorageDiff) ReadAt(ctx context.Context, p []byte, off int64, ft *storage.FrameTable) (int, error) {
	c, err := b.chunker.Wait()
	if err != nil {
		return 0, err
	}

	return c.ReadAt(ctx, p, off, ft)
}

func (b *StorageDiff) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	c, err := b.chunker.Wait()
	if err != nil {
		return nil, err
	}

	return c.Slice(ctx, off, length, ft)
}

// The local file might not be synced.
func (b *StorageDiff) CachePath() (string, error) {
	return b.cachePath, nil
}

func (b *StorageDiff) FileSize() (int64, error) {
	c, err := b.chunker.Wait()
	if err != nil {
		return 0, err
	}

	return c.FileSize()
}

func (b *StorageDiff) Size(_ context.Context) (int64, error) {
	return b.FileSize()
}

func (b *StorageDiff) BlockSize() int64 {
	return b.blockSize
}
