package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	googleReadTimeout              = 10 * time.Second
	googleOperationTimeout         = 5 * time.Second
	googleBufferSize               = 4 << 20 // 4 MiB
	googleInitialBackoff           = 10 * time.Millisecond
	googleMaxBackoff               = 10 * time.Second
	googleBackoffMultiplier        = 2
	googleMaxAttempts              = 10
	defaultGRPCConnectionPoolSize  = 4
	defaultGCSEnableDirectPath     = false
	gcloudDefaultUploadConcurrency = 16

	gcsOperationAttr                           = "operation"
	gcsOperationAttrWrite                      = "Write"
	gcsOperationAttrWriteFromFileSystem        = "WriteFromFileSystem"
	gcsOperationAttrWriteFromFileSystemOneShot = "WriteFromFileSystemOneShot"
	gcsOperationAttrWriteTo                    = "WriteTo"
	gcsOperationAttrSize                       = "Size"
	gcsOperationAttrOpenReader                 = "OpenRangeReader"
)

var (
	googleReadTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"orchestrator.storage.gcs.read",
		"Duration of GCS reads",
		"Total GCS bytes read",
		"Total GCS reads",
	))
	googleWriteTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"orchestrator.storage.gcs.write",
		"Duration of GCS writes",
		"Total bytes written to GCS",
		"Total writes to GCS",
	))
)

type gcpStore struct {
	client *storage.Client
	bucket *storage.BucketHandle

	limiter *limit.Limiter
}

var _ Store = (*gcpStore)(nil)

func NewGCP(ctx context.Context, bucketName string, limiter *limit.Limiter) (Store, error) {
	grpcPoolSize, err := env.GetEnvAsInt("GCS_GRPC_CONNECTION_POOL_SIZE", defaultGRPCConnectionPoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GCS_GRPC_CONNECTION_POOL_SIZE: %w", err)
	}

	opts := []option.ClientOption{
		option.WithGRPCConnectionPool(grpcPoolSize),
		option.WithGRPCDialOption(grpc.WithInitialConnWindowSize(32 * megabyte)),
		option.WithGRPCDialOption(grpc.WithInitialWindowSize(4 * megabyte)),
		option.WithGRPCDialOption(grpc.WithStatsHandler(otelgrpc.NewClientHandler())),
		internaloption.EnableDirectPath(defaultGCSEnableDirectPath),
	}

	client, err := storage.NewGRPCClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &gcpStore{
		client:  client,
		bucket:  client.Bucket(bucketName),
		limiter: limiter,
	}, nil
}

func (s *gcpStore) objectHandle(path string) *storage.ObjectHandle {
	return s.bucket.Object(path).Retryer(
		storage.WithMaxAttempts(googleMaxAttempts),
		storage.WithPolicy(storage.RetryAlways),
		storage.WithBackoff(
			gax.Backoff{
				Initial:    googleInitialBackoff,
				Max:        googleMaxBackoff,
				Multiplier: googleBackoffMultiplier,
			},
		),
	)
}

func (s *gcpStore) Fetch(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	timer := googleReadTimerFactory.Begin(attribute.String(gcsOperationAttr, gcsOperationAttrOpenReader))

	handle := s.objectHandle(path)

	ctx, cancel := context.WithTimeout(ctx, googleReadTimeout)
	reader, err := handle.NewRangeReader(ctx, offset, length)
	if err != nil {
		cancel()
		timer.Failure(ctx, 0)

		return nil, fmt.Errorf("failed to create GCS range reader for %q at %d+%d: %w", path, offset, length, err)
	}

	rc := &cancelOnCloseReader{ReadCloser: reader, cancel: cancel}

	return &timedReadCloser{inner: rc, timer: timer, ctx: ctx}, nil
}

func (s *gcpStore) GetBlob(ctx context.Context, path string) ([]byte, error) {
	timer := googleReadTimerFactory.Begin(attribute.String(gcsOperationAttr, gcsOperationAttrWriteTo))

	handle := s.objectHandle(path)

	ctx, cancel := context.WithTimeout(ctx, googleReadTimeout)
	defer cancel()

	reader, err := handle.NewReader(ctx)
	if err != nil {
		timer.Failure(ctx, 0)

		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("failed to create reader for %q: %w", path, ErrObjectNotExist)
		}

		return nil, fmt.Errorf("failed to create reader for %q: %w", path, err)
	}

	defer reader.Close()

	buff := make([]byte, googleBufferSize)
	var buf bytes.Buffer
	n, err := io.CopyBuffer(&buf, reader, buff)
	if err != nil {
		timer.Failure(ctx, n)

		return nil, fmt.Errorf("failed to read %q: %w", path, err)
	}

	timer.Success(ctx, n)

	return buf.Bytes(), nil
}

func (s *gcpStore) Size(ctx context.Context, path string) (int64, error) {
	timer := googleReadTimerFactory.Begin(attribute.String(gcsOperationAttr, gcsOperationAttrSize))

	handle := s.objectHandle(path)

	ctx, cancel := context.WithTimeout(ctx, googleOperationTimeout)
	defer cancel()

	attrs, err := handle.Attrs(ctx)
	if err != nil {
		timer.Failure(ctx, 0)

		if errors.Is(err, storage.ErrObjectNotExist) {
			// use ours instead of theirs
			return 0, fmt.Errorf("failed to get GCS object (%q) attributes: %w", path, ErrObjectNotExist)
		}

		return 0, fmt.Errorf("failed to get GCS object (%q) attributes: %w", path, err)
	}

	timer.Success(ctx, 0)

	if v, ok := attrs.Metadata[MetadataKeyUncompressedSize]; ok {
		parsed, parseErr := strconv.ParseInt(v, 10, 64)
		if parseErr == nil {
			return parsed, nil
		}
	}

	return attrs.Size, nil
}

func (s *gcpStore) PutBlob(ctx context.Context, path string, data []byte) error {
	timer := googleWriteTimerFactory.Begin(attribute.String(gcsOperationAttr, gcsOperationAttrWrite))

	handle := s.objectHandle(path)
	w := handle.NewWriter(ctx)

	c, err := io.Copy(w, bytes.NewReader(data))
	if err != nil && !errors.Is(err, io.EOF) {
		closeErr := w.Close()
		if closeErr != nil {
			logger.L().Warn(ctx, "failed to close GCS writer after copy error",
				zap.String("object", path),
				zap.NamedError("error_copy", err),
				zap.Error(closeErr),
			)
		}

		timer.Failure(ctx, c)

		if isResourceExhausted(err) {
			return ErrObjectRateLimited
		}

		return fmt.Errorf("failed to write to %q: %w", path, err)
	}

	if err := w.Close(); err != nil {
		timer.Failure(ctx, c)

		if isResourceExhausted(err) {
			return ErrObjectRateLimited
		}

		return fmt.Errorf("failed to write to %q: %w", path, err)
	}

	timer.Success(ctx, c)

	return nil
}

func (s *gcpStore) Upload(ctx context.Context, remotePath string, src io.ReaderAt, size int64) (e error) {
	ctx, span := tracer.Start(ctx, "write to gcp from file system")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	bucketName := s.bucket.BucketName()

	timer := googleWriteTimerFactory.Begin(
		attribute.String(gcsOperationAttr, gcsOperationAttrWriteFromFileSystem),
	)

	maxConcurrency := gcloudDefaultUploadConcurrency
	if s.limiter != nil {
		uploadLimiter := s.limiter.GCloudUploadLimiter()
		if uploadLimiter != nil {
			semaphoreErr := uploadLimiter.Acquire(ctx, 1)
			if semaphoreErr != nil {
				timer.Failure(ctx, 0)

				return fmt.Errorf("failed to acquire semaphore: %w", semaphoreErr)
			}
			defer uploadLimiter.Release(1)
		}

		maxConcurrency = s.limiter.GCloudMaxTasks(ctx)
	}

	// Small files: single-shot write
	if size < gcpMultipartUploadChunkSize {
		timer := googleWriteTimerFactory.Begin(
			attribute.String(gcsOperationAttr, gcsOperationAttrWriteFromFileSystemOneShot),
		)

		data := make([]byte, size)
		_, err := src.ReadAt(data, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			timer.Failure(ctx, 0)

			return fmt.Errorf("failed to read file: %w", err)
		}

		err = s.PutBlob(ctx, remotePath, data)
		if err != nil {
			timer.Failure(ctx, size)

			return fmt.Errorf("failed to write file (%d bytes): %w", size, err)
		}

		timer.Success(ctx, size)

		return nil
	}

	uploader, err := NewMultipartUploaderWithRetryConfig(
		ctx,
		bucketName,
		remotePath,
		DefaultRetryConfig(),
		nil,
	)
	if err != nil {
		timer.Failure(ctx, 0)

		return fmt.Errorf("failed to create multipart uploader: %w", err)
	}

	start := time.Now()

	numParts := int(math.Ceil(float64(size) / float64(gcpMultipartUploadChunkSize)))
	if numParts == 0 {
		numParts = 1
	}

	uploadID, err := uploader.initiateUpload(ctx)
	if err != nil {
		timer.Failure(ctx, 0)

		return fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	parts, err := uploader.uploadParts(ctx, maxConcurrency, numParts, size, src, uploadID)
	if err != nil {
		timer.Failure(ctx, 0)

		return fmt.Errorf("failed to upload parts: %w", err)
	}

	if err := uploader.completeUpload(ctx, uploadID, parts); err != nil {
		timer.Failure(ctx, 0)

		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	count := size
	if err != nil {
		timer.Failure(ctx, count)

		return fmt.Errorf("failed to upload file in parallel: %w", err)
	}

	logger.L().Debug(ctx, "Uploaded file in parallel",
		zap.String("bucket", bucketName),
		zap.String("object", remotePath),
		zap.Int("max_concurrency", maxConcurrency),
		zap.Int64("file_size", size),
		zap.Int64("duration", time.Since(start).Milliseconds()),
	)

	timer.Success(ctx, count)

	return nil
}

func (s *gcpStore) NewPartUploader(ctx context.Context, remotePath string, metadata map[string]string) (PartUploader, error) {
	return NewMultipartUploaderWithRetryConfig(ctx, s.bucket.BucketName(), remotePath, DefaultRetryConfig(), metadata)
}

func (s *gcpStore) Delete(ctx context.Context, prefix string) error {
	objects := s.bucket.Objects(ctx, &storage.Query{Prefix: prefix + "/"})

	for {
		object, err := objects.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return fmt.Errorf("error when iterating over template objects: %w", err)
		}

		err = s.bucket.Object(object.Name).Delete(ctx)
		if err != nil {
			return fmt.Errorf("error when deleting template object: %w", err)
		}
	}

	return nil
}

func (s *gcpStore) SignedUploadURL(_ context.Context, path string, ttl time.Duration) (string, error) {
	token, err := parseServiceAccountBase64(consts.GoogleServiceAccountSecret)
	if err != nil {
		return "", fmt.Errorf("failed to parse GCP service account: %w", err)
	}

	opts := &storage.SignedURLOptions{
		GoogleAccessID: token.ClientEmail,
		PrivateKey:     []byte(token.PrivateKey),
		Method:         http.MethodPut,
		Expires:        time.Now().Add(ttl),
	}

	url, err := storage.SignedURL(s.bucket.BucketName(), path, opts)
	if err != nil {
		return "", fmt.Errorf("failed to create signed URL for GCS object (%s): %w", path, err)
	}

	return url, nil
}

func (s *gcpStore) GetDetails() string {
	return fmt.Sprintf("[GCP Storage, bucket set to %s]", s.bucket.BucketName())
}

// cancelOnCloseReader wraps a ReadCloser and calls a CancelFunc on Close,
// ensuring the context used to create the reader is cleaned up.
type cancelOnCloseReader struct {
	io.ReadCloser

	cancel context.CancelFunc
}

func (r *cancelOnCloseReader) Close() error {
	defer r.cancel()

	return r.ReadCloser.Close()
}

type gcpServiceToken struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

func parseServiceAccountBase64(serviceAccount string) (*gcpServiceToken, error) {
	decoded, err := base64.StdEncoding.DecodeString(serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	var sa gcpServiceToken
	if err := json.Unmarshal(decoded, &sa); err != nil {
		return nil, fmt.Errorf("failed to parse service account JSON: %w", err)
	}

	return &sa, nil
}

func isResourceExhausted(err error) bool {
	type grpcStatusProvider interface {
		GRPCStatus() *status.Status
	}

	var se grpcStatusProvider
	if errors.As(err, &se) {
		return se.GRPCStatus().Code() == codes.ResourceExhausted
	}

	return false
}
