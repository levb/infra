package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	lz4 "github.com/pierrec/lz4/v4"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	featureflags "github.com/e2b-dev/infra/packages/shared/pkg/feature-flags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	cacheFilePermissions = 0o600
	cacheDirPermissions  = 0o700
)

type cache struct {
	rootPath  string
	chunkSize int64
	inner     StorageProvider
	flags     *featureflags.Client

	tracer trace.Tracer
}

var _ StorageProvider = (*cache)(nil)

func WrapInNFSCache(
	ctx context.Context,
	rootPath string,
	inner StorageProvider,
	flags *featureflags.Client,
) StorageProvider {
	cacheTracer := tracer

	createCacheSpans := flags.BoolFlag(ctx, featureflags.CreateStorageCacheSpansFlag)
	if !createCacheSpans {
		cacheTracer = noop.NewTracerProvider().Tracer("github.com/e2b-dev/infra/packages/shared/pkg/storage")
	}

	return &cache{
		rootPath:  rootPath,
		inner:     inner,
		chunkSize: MemoryChunkSize,
		flags:     flags,
		tracer:    cacheTracer,
	}
}

func (c cache) DeleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	// no need to wait for cache deletion before returning
	go func(ctx context.Context) {
		c.deleteCachedObjectsWithPrefix(ctx, prefix)
	}(context.WithoutCancel(ctx))

	return c.inner.DeleteObjectsWithPrefix(ctx, prefix)
}

func (c cache) UploadSignedURL(ctx context.Context, path string, ttl time.Duration) (string, error) {
	return c.inner.UploadSignedURL(ctx, path, ttl)
}

func (c cache) OpenBlob(ctx context.Context, path string, objectType ObjectType) (Blob, error) {
	innerObject, err := c.inner.OpenBlob(ctx, path, objectType)
	if err != nil {
		return nil, fmt.Errorf("failed to open object: %w", err)
	}

	localPath := filepath.Join(c.rootPath, path)
	if err = os.MkdirAll(localPath, cacheDirPermissions); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	return &cachedBlob{
		path:      localPath,
		chunkSize: c.chunkSize,
		inner:     innerObject,
		flags:     c.flags,
		tracer:    c.tracer,
	}, nil
}

func (c cache) OpenSeekable(ctx context.Context, path string, objectType SeekableObjectType) (Seekable, error) {
	innerObject, err := c.inner.OpenSeekable(ctx, path, objectType)
	if err != nil {
		return nil, fmt.Errorf("failed to open object: %w", err)
	}

	localPath := filepath.Join(c.rootPath, path)
	if err = os.MkdirAll(localPath, cacheDirPermissions); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	return &cachedSeekable{
		path:      localPath,
		chunkSize: c.chunkSize,
		inner:     innerObject,
		flags:     c.flags,
		tracer:    c.tracer,
	}, nil
}

func (c cache) GetDetails() string {
	return fmt.Sprintf("[Caching file storage, base path set to %s, which wraps %s]",
		c.rootPath, c.inner.GetDetails())
}

func (c cache) StoreFileCompressed(ctx context.Context, localPath, objectPath string, opts *FramedUploadOptions) (*FrameTable, error) {
	if opts != nil && opts.CompressionType != CompressionNone {
		return c.storeCompressed(ctx, localPath, objectPath, opts)
	}

	return c.inner.StoreFileCompressed(ctx, localPath, objectPath, opts)
}

// storeCompressed wraps the inner StoreFileCompressed with an OnFrameReady callback
// that writes each compressed frame to the NFS cache.
func (c cache) storeCompressed(ctx context.Context, localPath, objectPath string, opts *FramedUploadOptions) (*FrameTable, error) {
	// Copy opts so we don't mutate the caller's value
	modifiedOpts := *opts
	modifiedOpts.OnFrameReady = func(offset FrameOffset, size FrameSize, data []byte) error {
		framePath := makeFrameFilename(c.rootPath, objectPath, offset, size)

		dir := filepath.Dir(framePath)
		if err := os.MkdirAll(dir, cacheDirPermissions); err != nil {
			logger.L().Warn(ctx, "failed to create cache directory for compressed frame",
				zap.String("dir", dir),
				zap.Error(err))

			return nil // non-fatal: cache write failures should not block uploads
		}

		if err := os.WriteFile(framePath, data, cacheFilePermissions); err != nil {
			logger.L().Warn(ctx, "failed to write compressed frame to cache",
				zap.String("path", framePath),
				zap.Error(err))

			return nil // non-fatal
		}

		return nil
	}

	// Chain the original callback if present
	if opts.OnFrameReady != nil {
		origCallback := opts.OnFrameReady
		wrappedCallback := modifiedOpts.OnFrameReady
		modifiedOpts.OnFrameReady = func(offset FrameOffset, size FrameSize, data []byte) error {
			if err := origCallback(offset, size, data); err != nil {
				return err
			}

			return wrappedCallback(offset, size, data)
		}
	}

	ft, err := c.inner.StoreFileCompressed(ctx, localPath, objectPath, &modifiedOpts)
	if err != nil {
		return nil, err
	}

	return ft, nil
}

// makeFrameFilename returns the NFS cache path for a compressed frame.
// Format: {rootPath}/{objectPath}/{016xC}-{xC}.frm
func makeFrameFilename(rootPath, objectPath string, offset FrameOffset, size FrameSize) string {
	return fmt.Sprintf("%s/%s/%016x-%x.frm",
		rootPath, objectPath, offset.C, size.C)
}

func (c cache) GetFrame(ctx context.Context, objectPath string, offsetU int64, frameTable *FrameTable, decompress bool, buf []byte, readSize int64, onRead func(totalWritten int64)) (Range, error) {
	if !IsCompressed(frameTable) {
		return c.inner.GetFrame(ctx, objectPath, offsetU, frameTable, decompress, buf, readSize, onRead)
	}

	// Look up frame info for this uncompressed offset
	frameStart, frameSize, err := frameTable.FrameFor(offsetU)
	if err != nil {
		return Range{}, fmt.Errorf("cache GetFrame: frame lookup for offset %#x: %w", offsetU, err)
	}

	framePath := makeFrameFilename(c.rootPath, objectPath, frameStart, frameSize)

	// Try reading from NFS cache
	compressedBuf := make([]byte, frameSize.C)
	n, readErr := readCacheFile(framePath, compressedBuf)

	if readErr != nil {
		// Cache miss: fetch compressed data from inner
		_, err = c.inner.GetFrame(ctx, objectPath, offsetU, frameTable, false, compressedBuf, readSize, nil)
		if err != nil {
			return Range{}, fmt.Errorf("cache GetFrame: inner fetch for offset %#x: %w", offsetU, err)
		}

		n = int(frameSize.C)

		// Write compressed data to cache async
		dataCopy := make([]byte, n)
		copy(dataCopy, compressedBuf[:n])

		go func() {
			bgCtx := context.WithoutCancel(ctx)
			dir := filepath.Dir(framePath)
			if mkErr := os.MkdirAll(dir, cacheDirPermissions); mkErr != nil {
				logger.L().Warn(bgCtx, "failed to create frame cache dir", zap.Error(mkErr))

				return
			}

			if wErr := os.WriteFile(framePath, dataCopy, cacheFilePermissions); wErr != nil {
				logger.L().Warn(bgCtx, "failed to write frame to cache", zap.Error(wErr))
			}
		}()
	}

	if !decompress {
		copy(buf, compressedBuf[:n])

		return Range{Start: frameStart.C, Length: n}, nil
	}

	// Decompress compressed data into buf
	decompN, err := decompressBytes(frameTable.CompressionType, compressedBuf[:n], buf)
	if err != nil {
		return Range{}, fmt.Errorf("cache GetFrame: decompress for offset %#x: %w", offsetU, err)
	}

	return Range{Start: frameStart.C, Length: decompN}, nil
}

// readCacheFile reads a cache file into buf. Returns bytes read and error.
func readCacheFile(path string, buf []byte) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return n, err
	}

	return n, nil
}

// decompressBytes decompresses src into dst using the specified compression type.
// Returns the number of decompressed bytes written to dst.
func decompressBytes(ct CompressionType, src, dst []byte) (int, error) {
	switch ct {
	case CompressionLZ4:
		n, err := lz4.UncompressBlock(src, dst)
		if err != nil {
			return 0, fmt.Errorf("lz4 decompress: %w", err)
		}

		return n, nil

	case CompressionZstd:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return 0, fmt.Errorf("zstd decoder: %w", err)
		}
		defer dec.Close()

		decoded, err := dec.DecodeAll(src, dst[:0])
		if err != nil {
			return 0, fmt.Errorf("zstd decompress: %w", err)
		}

		return len(decoded), nil

	default:
		return 0, fmt.Errorf("unsupported compression type: %s", ct)
	}
}

func (c cache) deleteCachedObjectsWithPrefix(ctx context.Context, prefix string) {
	fullPrefix := filepath.Join(c.rootPath, prefix)
	if err := os.RemoveAll(fullPrefix); err != nil {
		logger.L().Error(ctx, "failed to remove object with prefix",
			zap.String("prefix", prefix),
			zap.String("path", fullPrefix),
			zap.Error(err))
	}
}

func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}
