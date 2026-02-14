package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"
	lz4 "github.com/pierrec/lz4/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	tracer = otel.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/storage")
	meter  = otel.GetMeterProvider().Meter("shared.pkg.storage")
)

var ErrObjectNotExist = errors.New("object does not exist")

type Provider string

const (
	GCPStorageProvider   Provider = "GCPBucket"
	AWSStorageProvider   Provider = "AWSBucket"
	LocalStorageProvider Provider = "Local"

	DefaultStorageProvider Provider = GCPStorageProvider

	storageProviderEnv = "STORAGE_PROVIDER"

	// MemoryChunkSize must always be bigger or equal to the block size.
	MemoryChunkSize = 4 * 1024 * 1024 // 4 MB
)

// rangeReadFunc is a callback for reading a byte range from storage.
type rangeReadFunc func(ctx context.Context, objectPath string, offset int64, length int) (io.ReadCloser, error)

type SeekableObjectType int

const (
	UnknownSeekableObjectType SeekableObjectType = iota
	MemfileObjectType
	RootFSObjectType
)

type ObjectType int

const (
	UnknownObjectType ObjectType = iota
	MemfileHeaderObjectType
	RootFSHeaderObjectType
	SnapfileObjectType
	MetadataObjectType
	BuildLayerFileObjectType
	LayerMetadataObjectType
)

type StorageProvider interface {
	DeleteObjectsWithPrefix(ctx context.Context, prefix string) error
	UploadSignedURL(ctx context.Context, path string, ttl time.Duration) (string, error)
	OpenBlob(ctx context.Context, path string, objectType ObjectType) (Blob, error)
	OpenSeekable(ctx context.Context, path string, seekableObjectType SeekableObjectType) (Seekable, error)
	GetDetails() string
	// GetFrame reads a single frame from storage into buf.
	// When frameTable is nil (uncompressed data), reads directly without frame translation.
	GetFrame(ctx context.Context, objectPath string, offsetU int64, frameTable *FrameTable, decompress bool, buf []byte) (Range, error)
}

type Blob interface {
	WriteTo(ctx context.Context, dst io.Writer) (int64, error)
	Put(ctx context.Context, data []byte) error
	Exists(ctx context.Context) (bool, error)
}

type SeekableReader interface {
	// Random slice access, off and buffer length must be aligned to block size
	ReadAt(ctx context.Context, buffer []byte, off int64) (int, error)
	Size(ctx context.Context) (int64, error)
}

type SeekableWriter interface {
	// Store entire file
	StoreFile(ctx context.Context, path string) error
}

type Seekable interface {
	SeekableReader
	SeekableWriter
}

func GetTemplateStorageProvider(ctx context.Context, limiter *limit.Limiter) (StorageProvider, error) {
	provider := Provider(env.GetEnv(storageProviderEnv, string(DefaultStorageProvider)))

	if provider == LocalStorageProvider {
		basePath := env.GetEnv("LOCAL_TEMPLATE_STORAGE_BASE_PATH", "/tmp/templates")

		return newFileSystemStorage(basePath), nil
	}

	bucketName := utils.RequiredEnv("TEMPLATE_BUCKET_NAME", "Bucket for storing template files")

	// cloud bucket-based storage
	switch provider {
	case AWSStorageProvider:
		return newAWSStorage(ctx, bucketName)
	case GCPStorageProvider:
		return NewGCP(ctx, bucketName, limiter)
	}

	return nil, fmt.Errorf("unknown storage provider: %s", provider)
}

func GetBuildCacheStorageProvider(ctx context.Context, limiter *limit.Limiter) (StorageProvider, error) {
	provider := Provider(env.GetEnv(storageProviderEnv, string(DefaultStorageProvider)))

	if provider == LocalStorageProvider {
		basePath := env.GetEnv("LOCAL_BUILD_CACHE_STORAGE_BASE_PATH", "/tmp/build-cache")

		return newFileSystemStorage(basePath), nil
	}

	bucketName := utils.RequiredEnv("BUILD_CACHE_BUCKET_NAME", "Bucket for storing template files")

	// cloud bucket-based storage
	switch provider {
	case AWSStorageProvider:
		return newAWSStorage(ctx, bucketName)
	case GCPStorageProvider:
		return NewGCP(ctx, bucketName, limiter)
	}

	return nil, fmt.Errorf("unknown storage provider: %s", provider)
}

func recordError(span trace.Span, err error) {
	if ignoreEOF(err) == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// GetBlob is a convenience wrapper that wraps b.WriteTo interface to return a
// byte slice.
func GetBlob(ctx context.Context, b Blob) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := b.WriteTo(ctx, &buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// LoadBlob opens a blob by path and reads its contents.
func LoadBlob(ctx context.Context, s StorageProvider, path string, objType ObjectType) ([]byte, error) {
	blob, err := s.OpenBlob(ctx, path, objType)
	if err != nil {
		return nil, fmt.Errorf("failed to open blob %s: %w", path, err)
	}

	return GetBlob(ctx, blob)
}

// getFrame is the shared implementation for reading a single frame from storage.
// Each backend (GCP, AWS, FS) calls this with their own rangeRead callback.
func getFrame(ctx context.Context, rangeRead rangeReadFunc, storageDetails string, objectPath string, offsetU int64, frameTable *FrameTable, decompress bool, buf []byte) (Range, error) {
	// Handle uncompressed data (nil frameTable) - read directly without frame translation
	if !IsCompressed(frameTable) {
		return getFrameUncompressed(ctx, rangeRead, objectPath, offsetU, buf)
	}

	// Get the frame info: translate U offset -> C offset for fetching
	frameStart, frameSize, err := frameTable.FrameFor(offsetU)
	if err != nil {
		return Range{}, fmt.Errorf("get frame for offset %#x, object %s: %w", offsetU, objectPath, err)
	}

	// Validate buffer size
	expectedSize := int(frameSize.C)
	if decompress {
		expectedSize = int(frameSize.U)
	}
	if len(buf) < expectedSize {
		return Range{}, fmt.Errorf("buffer too small: got %d bytes, need %d bytes for frame", len(buf), expectedSize)
	}

	// Fetch the compressed data from storage
	respBody, err := rangeRead(ctx, objectPath, frameStart.C, int(frameSize.C))
	if err != nil {
		return Range{}, fmt.Errorf("getting frame at %#x from %s in %s: %w", frameStart.C, objectPath, storageDetails, err)
	}
	defer respBody.Close()

	var from io.Reader = respBody
	readSize := int(frameSize.C)

	if decompress {
		readSize = int(frameSize.U)

		switch frameTable.CompressionType {
		case CompressionZstd:
			dec, err := zstd.NewReader(respBody)
			if err != nil {
				return Range{}, fmt.Errorf("failed to create zstd decoder: %w", err)
			}
			defer dec.Close()
			from = dec

		case CompressionLZ4:
			from = lz4.NewReader(respBody)

		default:
			return Range{}, fmt.Errorf("unsupported compression type: %s", frameTable.CompressionType)
		}
	}

	n, err := io.ReadFull(from, buf[:readSize])

	return Range{Start: frameStart.C, Length: n}, err
}

// getFrameUncompressed reads uncompressed data directly from storage.
func getFrameUncompressed(ctx context.Context, rangeRead rangeReadFunc, objectPath string, offset int64, buf []byte) (Range, error) {
	respBody, err := rangeRead(ctx, objectPath, offset, len(buf))
	if err != nil {
		return Range{}, fmt.Errorf("getting uncompressed data at %#x from %s: %w", offset, objectPath, err)
	}
	defer respBody.Close()

	n, err := io.ReadFull(respBody, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return Range{}, fmt.Errorf("reading uncompressed data from %s: %w", objectPath, err)
	}

	return Range{Start: offset, Length: n}, nil
}
