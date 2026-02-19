package sandbox

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	headers "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

type TemplateBuild struct {
	files       storage.TemplateFiles
	persistence storage.StorageProvider

	memfileHeader *headers.Header
	rootfsHeader  *headers.Header
}

func NewTemplateBuild(memfileHeader *headers.Header, rootfsHeader *headers.Header, persistence storage.StorageProvider, files storage.TemplateFiles) *TemplateBuild {
	return &TemplateBuild{
		persistence: persistence,
		files:       files,

		memfileHeader: memfileHeader,
		rootfsHeader:  rootfsHeader,
	}
}

func (t *TemplateBuild) Remove(ctx context.Context) error {
	err := t.persistence.DeleteObjectsWithPrefix(ctx, t.files.StorageDir())
	if err != nil {
		return fmt.Errorf("error when removing template build '%s': %w", t.files.StorageDir(), err)
	}

	return nil
}

func (t *TemplateBuild) uploadMemfileHeader(ctx context.Context, h *headers.Header) error {
	object, err := t.persistence.OpenBlob(ctx, t.files.StorageMemfileHeaderPath(), storage.MemfileHeaderObjectType)
	if err != nil {
		return err
	}

	serialized, err := headers.Serialize(h.Metadata, h.Mapping)
	if err != nil {
		return fmt.Errorf("error when serializing memfile header: %w", err)
	}

	err = object.Put(ctx, serialized)
	if err != nil {
		return fmt.Errorf("error when uploading memfile header: %w", err)
	}

	return nil
}

func (t *TemplateBuild) uploadMemfile(ctx context.Context, memfilePath string) error {
	object, err := t.persistence.OpenSeekable(ctx, t.files.StorageMemfilePath(), storage.MemfileObjectType)
	if err != nil {
		return err
	}

	err = object.StoreFile(ctx, memfilePath)
	if err != nil {
		return fmt.Errorf("error when uploading memfile: %w", err)
	}

	return nil
}

func (t *TemplateBuild) uploadRootfsHeader(ctx context.Context, h *headers.Header) error {
	object, err := t.persistence.OpenBlob(ctx, t.files.StorageRootfsHeaderPath(), storage.RootFSHeaderObjectType)
	if err != nil {
		return err
	}

	serialized, err := headers.Serialize(h.Metadata, h.Mapping)
	if err != nil {
		return fmt.Errorf("error when serializing memfile header: %w", err)
	}

	err = object.Put(ctx, serialized)
	if err != nil {
		return fmt.Errorf("error when uploading memfile header: %w", err)
	}

	return nil
}

func (t *TemplateBuild) uploadRootfs(ctx context.Context, rootfsPath string) error {
	object, err := t.persistence.OpenSeekable(ctx, t.files.StorageRootfsPath(), storage.RootFSObjectType)
	if err != nil {
		return err
	}

	err = object.StoreFile(ctx, rootfsPath)
	if err != nil {
		return fmt.Errorf("error when uploading rootfs: %w", err)
	}

	return nil
}

// Snap-file is small enough so we don't use composite upload.
func (t *TemplateBuild) uploadSnapfile(ctx context.Context, path string) error {
	object, err := t.persistence.OpenBlob(ctx, t.files.StorageSnapfilePath(), storage.SnapfileObjectType)
	if err != nil {
		return err
	}

	if err = uploadFileAsBlob(ctx, object, path); err != nil {
		return fmt.Errorf("error when uploading snapfile: %w", err)
	}

	return nil
}

// Metadata is small enough so we don't use composite upload.
func (t *TemplateBuild) uploadMetadata(ctx context.Context, path string) error {
	object, err := t.persistence.OpenBlob(ctx, t.files.StorageMetadataPath(), storage.MetadataObjectType)
	if err != nil {
		return err
	}

	if err := uploadFileAsBlob(ctx, object, path); err != nil {
		return fmt.Errorf("error when uploading metadata: %w", err)
	}

	return nil
}

func uploadFileAsBlob(ctx context.Context, b storage.Blob, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", path, err)
	}

	err = b.Put(ctx, data)
	if err != nil {
		return fmt.Errorf("failed to write data to object: %w", err)
	}

	return nil
}

// DataUploadResult holds the frame tables from compressed data uploads.
type DataUploadResult struct {
	MemfileFrameTable *storage.FrameTable
	RootfsFrameTable  *storage.FrameTable
}

// UploadData uploads all template build files, optionally including compressed data.
// When compressOpts is non-nil, compressed data is uploaded in parallel with uncompressed
// data (dual-write). Returns the frame tables from compressed uploads for later use
// in header serialization.
func (t *TemplateBuild) UploadData(
	ctx context.Context,
	metadataPath string,
	fcSnapfilePath string,
	memfilePath *string,
	rootfsPath *string,
	compressOpts *storage.FramedUploadOptions,
) (*DataUploadResult, error) {
	eg, ctx := errgroup.WithContext(ctx)
	result := &DataUploadResult{}

	// Uncompressed headers (always)
	eg.Go(func() error {
		if t.rootfsHeader == nil {
			return nil
		}

		return t.uploadRootfsHeader(ctx, t.rootfsHeader)
	})

	eg.Go(func() error {
		if t.memfileHeader == nil {
			return nil
		}

		return t.uploadMemfileHeader(ctx, t.memfileHeader)
	})

	// Uncompressed data (always, for rollback safety)
	eg.Go(func() error {
		if rootfsPath == nil {
			return nil
		}

		return t.uploadRootfs(ctx, *rootfsPath)
	})

	eg.Go(func() error {
		if memfilePath == nil {
			return nil
		}

		return t.uploadMemfile(ctx, *memfilePath)
	})

	// Compressed data (when enabled)
	if compressOpts != nil {
		var memFTMu, rootFTMu sync.Mutex

		if memfilePath != nil {
			eg.Go(func() error {
				ft, err := t.uploadCompressed(ctx, *memfilePath, storage.MemfileName, compressOpts)
				if err != nil {
					return fmt.Errorf("compressed memfile upload: %w", err)
				}

				memFTMu.Lock()
				result.MemfileFrameTable = ft
				memFTMu.Unlock()

				return nil
			})
		}

		if rootfsPath != nil {
			eg.Go(func() error {
				ft, err := t.uploadCompressed(ctx, *rootfsPath, storage.RootfsName, compressOpts)
				if err != nil {
					return fmt.Errorf("compressed rootfs upload: %w", err)
				}

				rootFTMu.Lock()
				result.RootfsFrameTable = ft
				rootFTMu.Unlock()

				return nil
			})
		}
	}

	// Snapfile + metadata
	eg.Go(func() error {
		return t.uploadSnapfile(ctx, fcSnapfilePath)
	})

	eg.Go(func() error {
		return t.uploadMetadata(ctx, metadataPath)
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return result, nil
}

// uploadCompressed compresses and uploads a file to the compressed data path.
func (t *TemplateBuild) uploadCompressed(ctx context.Context, localPath, fileName string, opts *storage.FramedUploadOptions) (*storage.FrameTable, error) {
	objectPath := t.files.CompressedDataPath(fileName, opts.CompressionType)

	ft, err := t.persistence.StoreFileCompressed(ctx, localPath, objectPath, opts)
	if err != nil {
		return nil, fmt.Errorf("error compressing %s to %s: %w", fileName, objectPath, err)
	}

	return ft, nil
}

// UploadCompressedHeaders serializes the v4 compressed headers (with frame tables)
// and uploads them. The pending frame tables must be applied to the headers before calling this.
func (t *TemplateBuild) UploadCompressedHeaders(ctx context.Context, pending *PendingFrameTables) error {
	eg, ctx := errgroup.WithContext(ctx)

	if t.memfileHeader != nil {
		eg.Go(func() error {
			return t.uploadCompressedHeader(ctx, pending, t.memfileHeader, storage.MemfileName)
		})
	}

	if t.rootfsHeader != nil {
		eg.Go(func() error {
			return t.uploadCompressedHeader(ctx, pending, t.rootfsHeader, storage.RootfsName)
		})
	}

	return eg.Wait()
}

func (t *TemplateBuild) uploadCompressedHeader(
	ctx context.Context,
	pending *PendingFrameTables,
	h *headers.Header,
	fileType string,
) error {
	// Apply frame tables to header mappings
	if err := pending.ApplyToHeader(h, fileType); err != nil {
		return fmt.Errorf("apply frames to %s header: %w", fileType, err)
	}

	// Set version to compressed so Serialize writes v4 format
	meta := *h.Metadata
	meta.Version = headers.MetadataVersionCompressed

	serialized, err := headers.Serialize(&meta, h.Mapping)
	if err != nil {
		return fmt.Errorf("serialize compressed %s header: %w", fileType, err)
	}

	compressed, err := storage.CompressLZ4(serialized)
	if err != nil {
		return fmt.Errorf("compress %s header: %w", fileType, err)
	}

	objectPath := t.files.CompressedHeaderPath(fileType)
	blob, err := t.persistence.OpenBlob(ctx, objectPath, storage.MemfileHeaderObjectType)
	if err != nil {
		return fmt.Errorf("open blob for compressed %s header: %w", fileType, err)
	}

	if err := blob.Put(ctx, compressed); err != nil {
		return fmt.Errorf("upload compressed %s header: %w", fileType, err)
	}

	return nil
}

func (t *TemplateBuild) Upload(ctx context.Context, metadataPath string, fcSnapfilePath string, memfilePath *string, rootfsPath *string) chan error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		if t.rootfsHeader == nil {
			return nil
		}

		err := t.uploadRootfsHeader(ctx, t.rootfsHeader)
		if err != nil {
			return err
		}

		return nil
	})

	eg.Go(func() error {
		if rootfsPath == nil {
			return nil
		}

		err := t.uploadRootfs(ctx, *rootfsPath)
		if err != nil {
			return err
		}

		return nil
	})

	eg.Go(func() error {
		if t.memfileHeader == nil {
			return nil
		}

		err := t.uploadMemfileHeader(ctx, t.memfileHeader)
		if err != nil {
			return err
		}

		return nil
	})

	eg.Go(func() error {
		if memfilePath == nil {
			return nil
		}

		err := t.uploadMemfile(ctx, *memfilePath)
		if err != nil {
			return err
		}

		return nil
	})

	eg.Go(func() error {
		if err := t.uploadSnapfile(ctx, fcSnapfilePath); err != nil {
			return fmt.Errorf("error when uploading snapfile: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		return t.uploadMetadata(ctx, metadataPath)
	})

	done := make(chan error)

	go func() {
		done <- eg.Wait()
	}()

	return done
}
