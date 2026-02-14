package sandbox

import (
	"bytes"
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

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

func (t *TemplateBuild) Upload(ctx context.Context, metadataPath string, fcSnapfilePath string, memfilePath *string, rootfsPath *string) chan error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		if t.rootfsHeader == nil {
			return nil
		}

		serialized, err := header.Serialize(t.rootfsHeader.Metadata, t.rootfsHeader.Mapping)
		if err != nil {
			return fmt.Errorf("error when serializing rootfs header: %w", err)
		}

		return t.persistence.StoreBlob(ctx, t.files.HeaderPath(storage.RootfsName), bytes.NewReader(serialized))
	})

	eg.Go(func() error {
		if rootfsPath == nil {
			return nil
		}

		_, err := t.persistence.StoreFile(ctx, *rootfsPath, t.files.Path(storage.RootfsName), storage.NoCompression)
		if err != nil {
			return fmt.Errorf("error when uploading rootfs: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		if t.memfileHeader == nil {
			return nil
		}

		serialized, err := header.Serialize(t.memfileHeader.Metadata, t.memfileHeader.Mapping)
		if err != nil {
			return fmt.Errorf("error when serializing memfile header: %w", err)
		}

		return t.persistence.StoreBlob(ctx, t.files.HeaderPath(storage.MemfileName), bytes.NewReader(serialized))
	})

	eg.Go(func() error {
		if memfilePath == nil {
			return nil
		}

		_, err := t.persistence.StoreFile(ctx, *memfilePath, t.files.Path(storage.MemfileName), storage.NoCompression)
		if err != nil {
			return fmt.Errorf("error when uploading memfile: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		err := storage.StoreBlobFromFile(ctx, t.persistence, fcSnapfilePath, t.files.Path(storage.SnapfileName))
		if err != nil {
			return fmt.Errorf("error when uploading snapfile: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		return storage.StoreBlobFromFile(ctx, t.persistence, metadataPath, t.files.Path(storage.MetadataName))
	})

	done := make(chan error)

	go func() {
		done <- eg.Wait()
	}()

	return done
}
