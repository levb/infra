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

func (b *StorageDiff) Init(ctx context.Context) error {
	chunker, err := b.createChunker(ctx)
	if err != nil {
		errMsg := fmt.Errorf("failed to create chunker: %w", err)
		b.chunker.SetError(errMsg)

		return errMsg
	}

	return b.chunker.SetValue(chunker)
}

// createChunker probes for available assets and creates a DecompressMMapChunker.
func (b *StorageDiff) createChunker(ctx context.Context) (block.Chunker, error) {
	assets := b.probeAssets(ctx)
	if assets.Size == 0 {
		return nil, fmt.Errorf("no asset found for %s (no uncompressed or compressed with metadata)", b.storagePath)
	}

	b.metrics.ChunkerCreations.Add(ctx, 1, metric.WithAttributes(
		attribute.String("chunker", block.ChunkerTypeDecompressMMap),
	))

	return block.NewDecompressMMapChunker(assets, b.blockSize, b.persistence, b.storageObjectType, b.cachePath, b.metrics)
}

// probeAssets probes for uncompressed and compressed asset variants in parallel.
// For compressed objects, Size() returns (uncompressedSize, compressedSize, err)
// via GCS/S3 metadata, allowing us to derive the mmap allocation size even
// when the uncompressed object doesn't exist.
func (b *StorageDiff) probeAssets(ctx context.Context) block.AssetInfo {
	assets := block.AssetInfo{BasePath: b.storagePath}

	var (
		wg            sync.WaitGroup
		lz4UncompSize int64
		zstUncompSize int64
	)

	// Probe all 3 paths in parallel: uncompressed, v4.*.lz4, v4.*.zst
	wg.Add(3)

	go func() {
		defer wg.Done()

		obj, err := b.persistence.OpenSeekable(ctx, b.storagePath, b.storageObjectType)
		if err != nil {
			return
		}

		uncompSize, _, err := obj.Size(ctx)
		if err != nil {
			return
		}

		assets.Size = uncompSize
		assets.HasUncompressed = true
	}()

	go func() {
		defer wg.Done()

		lz4Path := storage.V4DataPath(b.storagePath, storage.CompressionLZ4)
		obj, err := b.persistence.OpenSeekable(ctx, lz4Path, b.storageObjectType)
		if err != nil {
			return
		}

		uncompSize, compSize, err := obj.Size(ctx)
		if err != nil {
			return
		}

		assets.LZ4Size = compSize
		lz4UncompSize = uncompSize
	}()

	go func() {
		defer wg.Done()

		zstPath := storage.V4DataPath(b.storagePath, storage.CompressionZstd)
		obj, err := b.persistence.OpenSeekable(ctx, zstPath, b.storageObjectType)
		if err != nil {
			return
		}

		uncompSize, compSize, err := obj.Size(ctx)
		if err != nil {
			return
		}

		assets.ZstSize = compSize
		zstUncompSize = uncompSize
	}()

	wg.Wait()

	// If no uncompressed object exists, derive the mmap allocation size
	// from the compressed object's uncompressed-size metadata.
	if assets.Size == 0 {
		if lz4UncompSize > 0 {
			assets.Size = lz4UncompSize
		} else if zstUncompSize > 0 {
			assets.Size = zstUncompSize
		}
	}

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
	chunker, err := b.chunker.Wait()
	if err != nil {
		return 0, err
	}

	return chunker.ReadAt(ctx, p, off, ft)
}

func (b *StorageDiff) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
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

func (b *StorageDiff) Size(_ context.Context) (int64, int64, error) {
	s, err := b.FileSize()

	return s, 0, err
}

func (b *StorageDiff) BlockSize() int64 {
	return b.blockSize
}
