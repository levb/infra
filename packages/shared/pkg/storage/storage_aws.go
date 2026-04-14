package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	awsOperationTimeout = 5 * time.Second
	awsWriteTimeout     = 30 * time.Second
	awsReadTimeout      = 15 * time.Second
)

type awsStore struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucketName    string
}

var _ Store = (*awsStore)(nil)

func newAWSStore(ctx context.Context, bucketName string) (*awsStore, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)
	presignClient := s3.NewPresignClient(client)

	return &awsStore{
		client:        client,
		presignClient: presignClient,
		bucketName:    bucketName,
	}, nil
}

func (s *awsStore) Fetch(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	readRange := aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(path),
		Range:  readRange,
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrObjectNotExist
		}

		return nil, fmt.Errorf("failed to create S3 range reader for %q: %w", path, err)
	}

	return resp.Body, nil
}

func (s *awsStore) GetBlob(ctx context.Context, path string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, awsReadTimeout)
	defer cancel()

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucketName, Key: &path})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrObjectNotExist
		}

		return nil, err
	}

	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (s *awsStore) Size(ctx context.Context, path string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, awsOperationTimeout)
	defer cancel()

	resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucketName, Key: &path})
	if err != nil {
		var nsk *types.NoSuchKey
		var nfd *types.NotFound
		if errors.As(err, &nsk) || errors.As(err, &nfd) {
			return 0, ErrObjectNotExist
		}

		return 0, err
	}

	return *resp.ContentLength, nil
}

func (s *awsStore) PutBlob(ctx context.Context, path string, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, awsWriteTimeout)
	defer cancel()

	_, err := s.client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket: &s.bucketName,
			Key:    &path,
			Body:   bytes.NewReader(data),
		},
	)

	return err
}

func (s *awsStore) Upload(ctx context.Context, remotePath string, src io.ReaderAt, size int64) error {
	ctx, cancel := context.WithTimeout(ctx, awsWriteTimeout)
	defer cancel()

	uploader := manager.NewUploader(
		s.client,
		func(u *manager.Uploader) {
			u.PartSize = 10 * 1024 * 1024 // 10 MB
			u.Concurrency = 8             // eight parts in flight
		},
	)

	_, err := uploader.Upload(
		ctx,
		&s3.PutObjectInput{
			Bucket: &s.bucketName,
			Key:    &remotePath,
			Body:   io.NewSectionReader(src, 0, size),
		},
	)

	return err
}

func (s *awsStore) Delete(ctx context.Context, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, awsOperationTimeout)
	defer cancel()

	list, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &s.bucketName, Prefix: &prefix})
	if err != nil {
		return err
	}

	objects := make([]types.ObjectIdentifier, 0, len(list.Contents))
	for _, obj := range list.Contents {
		objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
	}

	// AWS S3 delete operation requires at least one object to delete.
	if len(objects) == 0 {
		logger.L().Warn(ctx, "No objects found to delete with the given prefix", zap.String("prefix", prefix), zap.String("bucket", s.bucketName))

		return nil
	}

	output, err := s.client.DeleteObjects(
		ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucketName),
			Delete: &types.Delete{Objects: objects},
		},
	)
	if err != nil {
		return err
	}

	if len(output.Errors) > 0 {
		var errStr strings.Builder
		for _, delErr := range output.Errors {
			errStr.WriteString(fmt.Sprintf("Key: %s, Code: %s, Message: %s; ", aws.ToString(delErr.Key), aws.ToString(delErr.Code), aws.ToString(delErr.Message)))
		}

		return errors.New("errors occurred during deletion: " + errStr.String())
	}

	if len(output.Deleted) != len(objects) {
		return errors.New("not all objects listed were deleted")
	}

	return nil
}

func (s *awsStore) SignedUploadURL(ctx context.Context, path string, ttl time.Duration) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(path),
	}
	resp, err := s.presignClient.PresignPutObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = ttl
	})
	if err != nil {
		return "", fmt.Errorf("failed to presign PUT URL: %w", err)
	}

	return resp.URL, nil
}

func (s *awsStore) GetDetails() string {
	return fmt.Sprintf("[AWS Storage, bucket set to %s]", s.bucketName)
}

func ignoreNotExists(err error) error {
	if errors.Is(err, ErrObjectNotExist) {
		return nil
	}

	return err
}
