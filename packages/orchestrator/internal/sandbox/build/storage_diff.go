package build

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func storagePath(buildId string, diffType DiffType) string {
	return fmt.Sprintf("%s/%s", buildId, diffType)
}

type StorageDiff struct {
	chunker           *utils.SetOnce[block.Chunker]
	cachePath         string
	cacheKey          DiffStoreKey
	storagePath       string
	storageObjectType storage.SeekableObjectType

	blockSize   int64
	metrics     blockmetrics.Metrics
	persistence storage.StorageProvider
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
	persistence storage.StorageProvider,
) (*StorageDiff, error) {
	sp := storagePath(buildId, diffType)
	sot, ok := storageObjectType(diffType)
	if !ok {
		return nil, UnknownDiffTypeError{diffType}
	}

	cachePath := GenerateDiffCachePath(basePath, buildId, diffType)

	return &StorageDiff{
		storagePath:       sp,
		storageObjectType: sot,
		cachePath:         cachePath,
		chunker:           utils.NewSetOnce[block.Chunker](),
		blockSize:         blockSize,
		metrics:           metrics,
		persistence:       persistence,
		cacheKey:          GetDiffStoreKey(buildId, diffType),
	}, nil
}

func storageObjectType(diffType DiffType) (storage.SeekableObjectType, bool) {
	switch diffType {
	case Memfile:
		return storage.MemfileObjectType, true
	case Rootfs:
		return storage.RootFSObjectType, true
	default:
		return storage.UnknownSeekableObjectType, false
	}
}

func (b *StorageDiff) CacheKey() DiffStoreKey {
	return b.cacheKey
}

// initChunker lazily creates the chunker on first access.
func (b *StorageDiff) initChunker(ctx context.Context) error {
	// If already initialized, nothing to do.
	if _, err := b.chunker.Result(); err == nil {
		return nil
	}

	c, err := b.createChunker(ctx)
	if err != nil {
		b.chunker.SetError(err)

		return err
	}

	return b.chunker.SetValue(c)
}

// createChunker probes for available assets and creates a DecompressMMapChunker.
func (b *StorageDiff) createChunker(ctx context.Context) (block.Chunker, error) {
	assets := b.probeAssets(ctx)
	if assets.Size == 0 {
		return nil, fmt.Errorf("uncompressed asset not found: %s", b.storagePath)
	}

	b.metrics.ChunkerCreations.Add(ctx, 1, metric.WithAttributes(
		attribute.String("chunker", block.ChunkerTypeDecompressMMap),
	))

	return block.NewDecompressMMapChunker(assets, b.blockSize, b.persistence, b.storageObjectType, b.cachePath, b.metrics)
}

// probeAssets probes for uncompressed and compressed asset variants in parallel.
func (b *StorageDiff) probeAssets(ctx context.Context) block.AssetInfo {
	assets := block.AssetInfo{BasePath: b.storagePath}

	var wg sync.WaitGroup

	// Probe all 3 paths in parallel: uncompressed, .lz4, .zst
	wg.Add(3)

	go func() {
		defer wg.Done()

		obj, err := b.persistence.OpenSeekable(ctx, b.storagePath, b.storageObjectType)
		if err != nil {
			return
		}

		assets.Size, _ = obj.Size(ctx)
	}()

	go func() {
		defer wg.Done()

		obj, err := b.persistence.OpenSeekable(ctx, b.storagePath+storage.CompressionLZ4.Suffix(), b.storageObjectType)
		if err != nil {
			return
		}

		assets.LZ4Size, _ = obj.Size(ctx)
	}()

	go func() {
		defer wg.Done()

		obj, err := b.persistence.OpenSeekable(ctx, b.storagePath+storage.CompressionZstd.Suffix(), b.storageObjectType)
		if err != nil {
			return
		}

		assets.ZstSize, _ = obj.Size(ctx)
	}()

	wg.Wait()

	return assets
}

func (b *StorageDiff) Close() error {
	c, err := b.chunker.Wait()
	if err != nil {
		return err
	}

	return c.Close()
}

func (b *StorageDiff) ReadAt(ctx context.Context, p []byte, off int64, ft *storage.FrameTable) (int, error) {
	if err := b.initChunker(ctx); err != nil {
		return 0, err
	}

	chunker, err := b.chunker.Wait()
	if err != nil {
		return 0, err
	}

	return chunker.ReadAt(ctx, p, off, ft)
}

func (b *StorageDiff) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	if err := b.initChunker(ctx); err != nil {
		return nil, err
	}

	chunker, err := b.chunker.Wait()
	if err != nil {
		return nil, err
	}

	return chunker.Slice(ctx, off, length, ft)
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
