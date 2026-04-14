package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	tracer = otel.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/storage")
	meter  = otel.Meter("github.com/e2b-dev/infra/packages/shared/pkg/storage")
)

var ErrObjectNotExist = errors.New("object does not exist")

// ErrObjectRateLimited means per-object mutation rate limiting ---
// multiple concurrent writers racing to write the same content-addressed object.
var ErrObjectRateLimited = errors.New("object access rate limited")

type Provider string

const (
	GCP   Provider = "GCPBucket"
	AWS   Provider = "AWSBucket"
	Local Provider = "Local"

	DefaultProvider Provider = GCP

	providerEnv = "STORAGE_PROVIDER"

	// MemoryChunkSize must always be bigger or equal to the block size.
	MemoryChunkSize = 4 * 1024 * 1024 // 4 MB

	// MetadataKeyUncompressedSize stores the original size so that Size()
	// returns the uncompressed size for compressed objects.
	MetadataKeyUncompressedSize = "uncompressed-size"
)

// GetProvider returns the configured storage provider type from the
// STORAGE_PROVIDER environment variable, defaulting to GCPBucket.
func GetProvider() Provider {
	return Provider(env.GetEnv(providerEnv, string(DefaultProvider)))
}

// IsLocal reports whether the configured storage provider is the local
// filesystem backend.
func IsLocal() bool {
	return GetProvider() == Local
}

// Fetcher is read-only access to storage. Used by LayerReader, NFS cache,
// peer blob relay, header loading, template resolution.
type Fetcher interface {
	Fetch(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)
	GetBlob(ctx context.Context, path string) ([]byte, error)
	Size(ctx context.Context, path string) (int64, error)
}

// Uploader is write access to storage. Used by build upload, template build.
type Uploader interface {
	Upload(ctx context.Context, remotePath string, src io.ReaderAt, size int64) error
	PutBlob(ctx context.Context, path string, data []byte) error
}

// Store is the full storage interface. Concrete stores (GCS, FS, AWS)
// and decorators (NFS cache, peer routing) implement this.
type Store interface {
	Fetcher
	Uploader
	Delete(ctx context.Context, prefix string) error
	SignedUploadURL(ctx context.Context, path string, ttl time.Duration) (string, error)
	GetDetails() string
}

// CompressableStore is optionally implemented by backends that support
// compressed multipart uploads. The caller creates a PartUploader for a
// specific remote path, then drives the compression pipeline externally.
type CompressableStore interface {
	NewPartUploader(ctx context.Context, remotePath string, metadata map[string]string) (PartUploader, error)
}

// PartUploader writes data in indexed parts. Implementations exist for
// GCS multipart uploads and local file writes.
type PartUploader interface {
	Start(ctx context.Context) error
	UploadPart(ctx context.Context, partIndex int, data ...[]byte) error
	Complete(ctx context.Context) error
	Close() error
}


// PeerTransitionedError is returned by the peer Seekable when the GCS upload
// has completed and serialized V4 headers are available.
type PeerTransitionedError struct {
	MemfileHeader []byte
	RootfsHeader  []byte
}

func (e *PeerTransitionedError) Error() string {
	return "peer upload completed, headers available"
}

// StoreConfig holds the configuration for creating a storage provider.
// Both GetLocalBasePath and GetBucketName are evaluated lazily so that
// callers who set environment variables at runtime (e.g. via os.Setenv
// or t.Setenv in tests) see their overrides respected.
type StoreConfig struct {
	GetLocalBasePath func() string
	GetBucketName    func() string
	limiter          *limit.Limiter
	uploadBaseURL    string
	hmacKey          []byte
}

// WithLimiter returns a copy of the config with the given limiter set.
func (c StoreConfig) WithLimiter(limiter *limit.Limiter) StoreConfig {
	c.limiter = limiter

	return c
}

// WithLocalUpload returns a copy of the config with the given local upload
// parameters set. These are only used when STORAGE_PROVIDER=Local to let the
// filesystem storage provider generate signed URLs for file uploads.
func (c StoreConfig) WithLocalUpload(uploadBaseURL string, hmacKey []byte) StoreConfig {
	c.uploadBaseURL = uploadBaseURL
	c.hmacKey = hmacKey

	return c
}

var TemplateStoreConfig = StoreConfig{
	GetLocalBasePath: func() string {
		return env.GetEnv("LOCAL_TEMPLATE_STORAGE_BASE_PATH", "/tmp/templates")
	},
	GetBucketName: func() string {
		return utils.RequiredEnv("TEMPLATE_BUCKET_NAME", "Bucket for storing template files")
	},
}

var BuildCacheStoreConfig = StoreConfig{
	GetLocalBasePath: func() string {
		return env.GetEnv("LOCAL_BUILD_CACHE_STORAGE_BASE_PATH", "/tmp/build-cache")
	},
	GetBucketName: func() string {
		return utils.RequiredEnv("BUILD_CACHE_BUCKET_NAME", "Bucket for storing build cache files")
	},
}

func GetStore(ctx context.Context, cfg StoreConfig) (Store, error) {
	provider := GetProvider()

	if provider == Local {
		return newFileSystemStore(cfg), nil
	}

	bucketName := cfg.GetBucketName()

	// cloud bucket-based storage
	switch provider {
	case AWS:
		return newAWSStore(ctx, bucketName)
	case GCP:
		return NewGCP(ctx, bucketName, cfg.limiter)
	}

	return nil, fmt.Errorf("unknown storage provider: %s", provider)
}

// PutBlobFromFile reads a local file and writes it as a blob.
func PutBlobFromFile(ctx context.Context, dst Uploader, remotePath, localPath string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", localPath, err)
	}

	return dst.PutBlob(ctx, remotePath, data)
}

// UploadFile opens a local file and uploads it via Upload.
func UploadFile(ctx context.Context, dst Uploader, remotePath, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", localPath, err)
	}

	return dst.Upload(ctx, remotePath, f, fi.Size())
}

// GetBlobToFile reads a blob and writes it to a local file.
func GetBlobToFile(ctx context.Context, src Fetcher, remotePath, localPath string) error {
	data, err := src.GetBlob(ctx, remotePath)
	if err != nil {
		return err
	}

	return os.WriteFile(localPath, data, 0o644)
}

func recordError(span trace.Span, err error) {
	if ignoreEOF(err) == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// timedReadCloser wraps a reader with OTEL timer metrics.
// Close records success (with total bytes read) or failure on the timer.
type timedReadCloser struct {
	inner     io.ReadCloser
	timer     *telemetry.Stopwatch
	ctx       context.Context //nolint:containedctx // needed for timer recording in Close
	bytesRead int64
	closeErr  error
}

func (r *timedReadCloser) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	r.bytesRead += int64(n)

	if err != nil && err != io.EOF {
		r.closeErr = err
	}

	return n, err
}

func (r *timedReadCloser) Close() error {
	err := r.inner.Close()

	if r.closeErr != nil || err != nil {
		r.timer.Failure(r.ctx, r.bytesRead)
	} else {
		r.timer.Success(r.ctx, r.bytesRead)
	}

	return err
}
