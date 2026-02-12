package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// PendingFrameTables collects frame tables from data uploads, keyed by
// object name (e.g., "buildId/rootfs.ext4"). These are held temporarily
// until headers can be finalized after all data uploads complete.
type PendingFrameTables struct {
	tables sync.Map // key: objectName (string), value: *storage.FrameTable
}

func NewPendingFrameTables() *PendingFrameTables {
	return &PendingFrameTables{}
}

// Add stores a frame table for a specific object name.
func (p *PendingFrameTables) Add(objectName string, ft *storage.FrameTable) {
	if ft == nil {
		return
	}
	p.tables.Store(objectName, ft)
}

// Get retrieves a frame table for a specific object name.
func (p *PendingFrameTables) Get(objectName string) *storage.FrameTable {
	v, ok := p.tables.Load(objectName)
	if !ok {
		return nil
	}

	return v.(*storage.FrameTable)
}

// ApplyToHeader applies frame tables to all mappings in a header based on each mapping's BuildId.
// This should be called after all data uploads are complete so all frame tables are available.
func (p *PendingFrameTables) ApplyToHeader(h *header.Header, fileType string) error {
	if h == nil {
		return nil
	}

	for _, mapping := range h.Mapping {
		if mapping.BuildId == uuid.Nil {
			// Skip hole mappings
			continue
		}

		objectName := mapping.BuildId.String() + "/" + fileType
		ft := p.Get(objectName)
		if ft == nil {
			// No frame table for this build - data might be uncompressed or already has FT
			continue
		}

		if err := mapping.AddFrames(ft); err != nil {
			return fmt.Errorf("failed to add frames to mapping for build %s: %w", mapping.BuildId, err)
		}
	}

	return nil
}

type TemplateBuild struct {
	files       storage.TemplateFiles
	persistence storage.StorageProvider

	memfileHeader *header.Header
	rootfsHeader  *header.Header
}

func NewTemplateBuild(memfileHeader *header.Header, rootfsHeader *header.Header, s storage.StorageProvider, files storage.TemplateFiles) *TemplateBuild {
	return &TemplateBuild{
		persistence: s,
		files:       files,

		memfileHeader: memfileHeader,
		rootfsHeader:  rootfsHeader,
	}
}

func (t *TemplateBuild) Remove(ctx context.Context) error {
	err := t.persistence.DeleteWithPrefix(ctx, t.files.StorageDir())
	if err != nil {
		return fmt.Errorf("error when removing template build '%s': %w", t.files.StorageDir(), err)
	}

	return nil
}

// DataUploadResult contains the frame tables generated from uploading data files.
type DataUploadResult struct {
	MemfileFrameTable *storage.FrameTable
	RootfsFrameTable  *storage.FrameTable
}

// UploadData uploads all data files (rootfs, memfile, snapfile, metadata) in parallel.
// It returns the frame tables generated from compression, which should be added
// to PendingFrameTables for later use when finalizing headers.
func (t *TemplateBuild) UploadData(
	ctx context.Context,
	metadataFilePath string,
	snapFilePath string,
	memfileFilePath *string,
	rootfsFilePath *string,
) (*DataUploadResult, error) {
	eg, ctx := errgroup.WithContext(ctx)

	var rootfsFT, memfileFT *storage.FrameTable

	// Uncompressed uploads to default paths ensure rollback safety:
	// flipping UseCompressedAssets=false makes readers use these paths immediately.
	eg.Go(func() error {
		if rootfsFilePath != nil {
			_, err := t.persistence.StoreFile(
				ctx, *rootfsFilePath, t.files.StorageRootfsPath(), storage.NoCompression)
			if err != nil {
				return fmt.Errorf("error when uploading rootfs data: %w", err)
			}
		}

		return nil
	})

	eg.Go(func() error {
		if memfileFilePath != nil {
			_, err := t.persistence.StoreFile(
				ctx, *memfileFilePath, t.files.StorageMemfilePath(), storage.NoCompression)
			if err != nil {
				return fmt.Errorf("error when uploading memfile data: %w", err)
			}
		}

		return nil
	})

	// TODO LEV consider moving compression to a separate, backgroup process.
	//
	// TODO LEV centralize the logic of choosing the compression options, the
	// only thing that matters here ATM is a bool to compress or not.
	if storage.EnableGCSCompression {
		eg.Go(func() error {
			if rootfsFilePath != nil {
				ft, err := t.persistence.StoreFile(
					ctx, *rootfsFilePath, t.files.StorageRootfsCompressedPath(storage.DefaultCompressionOptions.CompressionType), storage.DefaultCompressionOptions)
				if err != nil {
					return fmt.Errorf("error when uploading compressed rootfs data: %w", err)
				}
				rootfsFT = ft
			}

			return nil
		})

		eg.Go(func() error {
			if memfileFilePath != nil {
				ft, err := t.persistence.StoreFile(
					ctx, *memfileFilePath, t.files.StorageMemfileCompressedPath(storage.DefaultCompressionOptions.CompressionType), storage.DefaultCompressionOptions)
				if err != nil {
					return fmt.Errorf("error when uploading compressed memfile data: %w", err)
				}
				memfileFT = ft
			}

			return nil
		})
	}

	eg.Go(func() error {
		err := storage.StoreBlobFromFile(ctx, t.persistence, snapFilePath, t.files.StorageSnapfilePath())
		if err != nil {
			return fmt.Errorf("error when uploading snapfile: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		err := storage.StoreBlobFromFile(ctx, t.persistence, metadataFilePath, t.files.StorageMetadataPath())
		if err != nil {
			return fmt.Errorf("error when uploading metadata: %w", err)
		}

		return nil
	})

	// Uncompressed headers (like main's Upload) — no frame tables needed.
	if t.rootfsHeader != nil {
		eg.Go(func() error {
			serialized, err := header.Serialize(t.rootfsHeader.Metadata, t.rootfsHeader.Mapping)
			if err != nil {
				return fmt.Errorf("error when serializing rootfs header: %w", err)
			}

			return t.persistence.StoreBlob(ctx, t.files.StorageRootfsHeaderPath(), bytes.NewReader(serialized))
		})
	}

	if t.memfileHeader != nil {
		eg.Go(func() error {
			serialized, err := header.Serialize(t.memfileHeader.Metadata, t.memfileHeader.Mapping)
			if err != nil {
				return fmt.Errorf("error when serializing memfile header: %w", err)
			}

			return t.persistence.StoreBlob(ctx, t.files.StorageMemfileHeaderPath(), bytes.NewReader(serialized))
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return &DataUploadResult{
		RootfsFrameTable:  rootfsFT,
		MemfileFrameTable: memfileFT,
	}, nil
}

// UploadCompressedHeaders applies pending frame tables to headers, serializes them,
// and uploads compressed header variants to storage. This is a no-op when
// EnableGCSCompression is false. Uncompressed headers are already uploaded by UploadData.
//
// This should be called after all data uploads are complete so pending contains
// frame tables from all builds referenced by the headers.
func (t *TemplateBuild) UploadCompressedHeaders(ctx context.Context, pending *PendingFrameTables) error {
	if !storage.EnableGCSCompression {
		return nil
	}

	// Apply frame tables (mutates headers — safe, uncompressed already uploaded by UploadData).
	if err := pending.ApplyToHeader(t.rootfsHeader, "rootfs.ext4"); err != nil {
		return fmt.Errorf("failed to apply frame tables to rootfs header: %w", err)
	}

	if err := pending.ApplyToHeader(t.memfileHeader, "memfile"); err != nil {
		return fmt.Errorf("failed to apply frame tables to memfile header: %w", err)
	}

	// Serialize and upload compressed headers in parallel.
	eg, ctx := errgroup.WithContext(ctx)

	if t.rootfsHeader != nil {
		serialized, err := header.Serialize(t.rootfsHeader.Metadata, t.rootfsHeader.Mapping)
		if err != nil {
			return fmt.Errorf("error when serializing compressed rootfs header: %w", err)
		}

		eg.Go(func() error {
			return t.persistence.StoreBlob(ctx, t.files.StorageRootfsHeaderCompressedPath(storage.DefaultCompressionOptions.CompressionType), bytes.NewReader(serialized))
		})
	}

	if t.memfileHeader != nil {
		serialized, err := header.Serialize(t.memfileHeader.Metadata, t.memfileHeader.Mapping)
		if err != nil {
			return fmt.Errorf("error when serializing compressed memfile header: %w", err)
		}

		eg.Go(func() error {
			return t.persistence.StoreBlob(ctx, t.files.StorageMemfileHeaderCompressedPath(storage.DefaultCompressionOptions.CompressionType), bytes.NewReader(serialized))
		})
	}

	return eg.Wait()
}

// Upload uploads data files and headers for a single build.
// This is appropriate for single-layer uploads (e.g., pausing a sandbox) where
// parent frame tables are already embedded in the header from previous builds.
// For parallel multi-layer builds, use UploadData + UploadCompressedHeaders with a shared
// PendingFrameTables to coordinate frame tables across concurrent uploads.
func (t *TemplateBuild) Upload(ctx context.Context, metadataFilePath string, snapFilePath string, memfileFilePath *string, rootfsFilePath *string) chan error {
	done := make(chan error)

	go func() {
		// Create pending frame tables just for this build
		pending := NewPendingFrameTables()

		// Upload data files
		result, err := t.UploadData(ctx, metadataFilePath, snapFilePath, memfileFilePath, rootfsFilePath)
		if err != nil {
			done <- err

			return
		}

		// Add this build's frame tables to pending
		buildId := t.files.BuildID
		if result.RootfsFrameTable != nil {
			pending.Add(buildId+"/rootfs.ext4", result.RootfsFrameTable)
		}
		if result.MemfileFrameTable != nil {
			pending.Add(buildId+"/memfile", result.MemfileFrameTable)
		}

		// Upload compressed headers (only has this build's frame tables, not parents').
		// For multi-layer builds, use UploadData + UploadCompressedHeaders with a shared
		// PendingFrameTables instead.
		err = t.UploadCompressedHeaders(ctx, pending)
		done <- err
	}()

	return done
}
