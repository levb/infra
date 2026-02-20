package sandbox

import (
	"context"
	"fmt"

	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/internal/sandbox/template"
	featureflags "github.com/e2b-dev/infra/packages/shared/pkg/feature-flags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

type Snapshot struct {
	MemfileDiff       build.Diff
	MemfileDiffHeader *header.Header
	RootfsDiff        build.Diff
	RootfsDiffHeader  *header.Header
	Snapfile          template.File
	Metafile          template.File

	cleanup *Cleanup
}

func (s *Snapshot) Upload(
	ctx context.Context,
	persistence storage.StorageProvider,
	templateFiles storage.TemplateFiles,
	ff *featureflags.Client,
) error {
	var memfilePath *string
	switch r := s.MemfileDiff.(type) {
	case *build.NoDiff:
	default:
		memfileLocalPath, err := r.CachePath()
		if err != nil {
			return fmt.Errorf("error getting memfile diff path: %w", err)
		}

		memfilePath = &memfileLocalPath
	}

	var rootfsPath *string
	switch r := s.RootfsDiff.(type) {
	case *build.NoDiff:
	default:
		rootfsLocalPath, err := r.CachePath()
		if err != nil {
			return fmt.Errorf("error getting rootfs diff path: %w", err)
		}

		rootfsPath = &rootfsLocalPath
	}

	templateBuild := NewTemplateBuild(
		s.MemfileDiffHeader,
		s.RootfsDiffHeader,
		persistence,
		templateFiles,
		ff,
	)

	uploadErrCh := templateBuild.Upload(
		ctx,
		s.Metafile.Path(),
		s.Snapfile.Path(),
		memfilePath,
		rootfsPath,
	)

	// Wait for the upload to finish
	uploadErr := <-uploadErrCh
	if uploadErr != nil {
		return fmt.Errorf("error uploading template build: %w", uploadErr)
	}

	return nil
}

// UploadDataFilesResult holds the results of a data-file upload phase,
// including the TemplateBuild for header finalization and frame tables
// from compressed uploads.
type UploadDataFilesResult struct {
	TemplateBuild     *TemplateBuild
	MemfileFrameTable *storage.FrameTable
	RootfsFrameTable  *storage.FrameTable
}

// UploadDataFiles uploads data files (uncompressed + optionally compressed).
// When compression is enabled (via feature flag), compressed data is uploaded in
// parallel with uncompressed data (dual-write). It returns the TemplateBuild and
// frame tables so the caller can finalize compressed headers after all layers complete.
func (s *Snapshot) UploadDataFiles(
	ctx context.Context,
	persistence storage.StorageProvider,
	templateFiles storage.TemplateFiles,
	ff *featureflags.Client,
) (*UploadDataFilesResult, error) {
	var memfilePath *string
	switch r := s.MemfileDiff.(type) {
	case *build.NoDiff:
	default:
		memfileLocalPath, err := r.CachePath()
		if err != nil {
			return nil, fmt.Errorf("error getting memfile diff path: %w", err)
		}

		memfilePath = &memfileLocalPath
	}

	var rootfsPath *string
	switch r := s.RootfsDiff.(type) {
	case *build.NoDiff:
	default:
		rootfsLocalPath, err := r.CachePath()
		if err != nil {
			return nil, fmt.Errorf("error getting rootfs diff path: %w", err)
		}

		rootfsPath = &rootfsLocalPath
	}

	templateBuild := NewTemplateBuild(
		s.MemfileDiffHeader,
		s.RootfsDiffHeader,
		persistence,
		templateFiles,
		ff,
	)

	result, err := templateBuild.UploadData(
		ctx,
		s.Metafile.Path(),
		s.Snapfile.Path(),
		memfilePath,
		rootfsPath,
	)
	if err != nil {
		return nil, fmt.Errorf("error uploading template data: %w", err)
	}

	return &UploadDataFilesResult{
		TemplateBuild:     templateBuild,
		MemfileFrameTable: result.MemfileFrameTable,
		RootfsFrameTable:  result.RootfsFrameTable,
	}, nil
}

func (s *Snapshot) Close(ctx context.Context) error {
	err := s.cleanup.Run(ctx)
	if err != nil {
		return fmt.Errorf("error cleaning up snapshot: %w", err)
	}

	return nil
}
