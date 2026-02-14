package build

import (
	"context"
	"fmt"
	"sync"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type chunkerType byte

const (
	uncompressedMMapChunker chunkerType = iota
	decompressMMapChunker
	compressMMapLRUChunker
)

var (
	compressedChunkerType   = compressMMapLRUChunker
	uncompressedChunkerType = uncompressedMMapChunker
)

type StorageDiff struct {
	// chunker is lazily initialized via chunkerOnce on first ReadAt/Slice call.
	chunker     block.Chunker
	chunkerOnce sync.Once
	chunkerErr  error

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

func storagePath(buildId string, diffType DiffType) string {
	return storage.TemplateFiles{BuildID: buildId}.Path(string(diffType))
}

func newStorageDiff(
	basePath string,
	buildId string,
	diffType DiffType,
	blockSize int64,
	metrics blockmetrics.Metrics,
	persistence storage.StorageProvider,
) (*StorageDiff, error) {
	storagePath := storagePath(buildId, diffType)
	storageObjectType, ok := storageObjectType(diffType)
	if !ok {
		return nil, UnknownDiffTypeError{diffType}
	}

	cachePath := GenerateDiffCachePath(basePath, buildId, diffType)
	cacheKey := GetDiffStoreKey(buildId, diffType)

	return &StorageDiff{
		storagePath:       storagePath,
		storageObjectType: storageObjectType,
		cachePath:         cachePath,
		blockSize:         blockSize,
		metrics:           metrics,
		persistence:       persistence,
		cacheKey:          cacheKey,
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

// getChunker lazily initializes and returns the chunker.
// The frame table determines whether to use compressed or uncompressed chunker.
func (b *StorageDiff) getChunker(ctx context.Context, ft *storage.FrameTable) (block.Chunker, error) {
	b.chunkerOnce.Do(func() {
		b.chunker, b.chunkerErr = b.createChunker(ctx, ft)
	})

	return b.chunker, b.chunkerErr
}

// createChunker creates the appropriate chunker based on the frame table.
func (b *StorageDiff) createChunker(ctx context.Context, ft *storage.FrameTable) (block.Chunker, error) {
	actualPath := b.storagePath
	if storage.IsCompressed(ft) {
		actualPath = b.storagePath + ft.CompressionTypeSuffix()
	}

	// Get actual file size from storage
	obj, err := b.persistence.OpenSeekable(ctx, actualPath, b.storageObjectType)
	if err != nil {
		return nil, fmt.Errorf("failed to open object %s: %w", actualPath, err)
	}
	rawSize, err := obj.Size(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get size of %s: %w", actualPath, err)
	}

	if storage.IsCompressed(ft) {
		// For compressed data, also get the uncompressed size
		uObj, err := b.persistence.OpenSeekable(ctx, b.storagePath, b.storageObjectType)
		if err != nil {
			return nil, fmt.Errorf("failed to open uncompressed object %s: %w", b.storagePath, err)
		}
		uSize, err := uObj.Size(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get uncompressed size of %s: %w", b.storagePath, err)
		}

		const estimatedFrameU = 16 * 1024 * 1024
		estimatedFrames := max(1, int(uSize/estimatedFrameU))
		lruSize := max(4, estimatedFrames/2)

		switch compressedChunkerType {
		case decompressMMapChunker:
			return block.NewDecompressMMapChunker(uSize, rawSize, b.blockSize, b.persistence, actualPath, b.cachePath, b.metrics)

		case compressMMapLRUChunker:
			return block.NewCompressMMapLRUChunker(uSize, rawSize, b.persistence, actualPath, b.cachePath, lruSize, b.metrics)

		default:
			return nil, fmt.Errorf("unsupported chunker type for object %s", actualPath)
		}
	}

	// Uncompressed path
	switch uncompressedChunkerType {
	case decompressMMapChunker:
		return block.NewDecompressMMapChunker(rawSize, rawSize, b.blockSize, b.persistence, actualPath, b.cachePath, b.metrics)

	case uncompressedMMapChunker:
		return block.NewUncompressedMMapChunker(rawSize, b.blockSize, b.persistence, actualPath, b.cachePath, b.metrics)

	default:
		return nil, fmt.Errorf("unsupported chunker type for object %s", actualPath)
	}
}

func (b *StorageDiff) Close() error {
	if b.chunker == nil {
		return nil
	}

	return b.chunker.Close()
}

func (b *StorageDiff) ReadAt(ctx context.Context, p []byte, off int64, ft *storage.FrameTable) (int, error) {
	chunker, err := b.getChunker(ctx, ft)
	if err != nil {
		return 0, err
	}

	slice, err := chunker.Slice(ctx, off, int64(len(p)), ft)
	if err != nil {
		return 0, err
	}

	n := copy(p, slice)

	return n, nil
}

func (b *StorageDiff) Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error) {
	chunker, err := b.getChunker(ctx, ft)
	if err != nil {
		return nil, err
	}

	return chunker.Slice(ctx, off, length, ft)
}

func (b *StorageDiff) CachePath() (string, error) {
	return b.cachePath, nil
}

func (b *StorageDiff) FileSize() (int64, error) {
	if b.chunker == nil {
		return 0, fmt.Errorf("chunker not initialized - call ReadAt or Slice first")
	}

	return b.chunker.FileSize()
}

func (b *StorageDiff) BlockSize() int64 {
	return b.blockSize
}
