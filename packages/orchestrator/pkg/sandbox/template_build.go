package sandbox

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

type TemplateBuild struct {
	paths storage.Paths
	store storage.Store

	memfileHeader *header.Header
	rootfsHeader  *header.Header
}

func NewTemplateBuild(memfileHeader *header.Header, rootfsHeader *header.Header, s storage.Store, paths storage.Paths) *TemplateBuild {
	return &TemplateBuild{
		store: s,
		paths: paths,

		memfileHeader: memfileHeader,
		rootfsHeader:  rootfsHeader,
	}
}

func (t *TemplateBuild) Remove(ctx context.Context) error {
	err := t.store.Delete(ctx, t.paths.StorageDir())
	if err != nil {
		return fmt.Errorf("error when removing template build '%s': %w", t.paths.StorageDir(), err)
	}

	return nil
}

func (t *TemplateBuild) uploadMemfileHeader(ctx context.Context, h *header.Header) error {
	serialized, err := header.SerializeHeader(h)
	if err != nil {
		return fmt.Errorf("error when serializing memfile header: %w", err)
	}

	if err := t.store.PutBlob(ctx, t.paths.MemfileHeader(), serialized); err != nil {
		return fmt.Errorf("error when uploading memfile header: %w", err)
	}

	return nil
}

func (t *TemplateBuild) uploadRootfsHeader(ctx context.Context, h *header.Header) error {
	serialized, err := header.SerializeHeader(h)
	if err != nil {
		return fmt.Errorf("error when serializing rootfs header: %w", err)
	}

	if err := t.store.PutBlob(ctx, t.paths.RootfsHeader(), serialized); err != nil {
		return fmt.Errorf("error when uploading rootfs header: %w", err)
	}

	return nil
}

func (t *TemplateBuild) Upload(ctx context.Context, metadataPath string, fcSnapfilePath string, memfilePath *string, rootfsPath *string) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		if t.memfileHeader == nil {
			return nil
		}

		return t.uploadMemfileHeader(ctx, t.memfileHeader)
	})

	eg.Go(func() error {
		if t.rootfsHeader == nil {
			return nil
		}

		return t.uploadRootfsHeader(ctx, t.rootfsHeader)
	})

	eg.Go(func() error {
		if rootfsPath == nil {
			return nil
		}

		return storage.UploadFile(ctx, t.store, t.paths.Rootfs(), *rootfsPath)
	})

	eg.Go(func() error {
		if memfilePath == nil {
			return nil
		}

		return storage.UploadFile(ctx, t.store, t.paths.Memfile(), *memfilePath)
	})

	eg.Go(func() error {
		return storage.PutBlobFromFile(ctx, t.store, t.paths.Snapfile(), fcSnapfilePath)
	})

	eg.Go(func() error {
		return storage.PutBlobFromFile(ctx, t.store, t.paths.Metadata(), metadataPath)
	})

	return eg.Wait()
}
