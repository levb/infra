package sandbox

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// BuildUploader uploads a paused snapshot's files to storage.
type BuildUploader interface {
	// UploadData uploads data files, snapfile, and metadata.
	UploadData(ctx context.Context) error
	// FinalizeHeaders uploads final headers after all upstream layers are done.
	// Returns serialized V4 header bytes for peer transition (nil for uncompressed).
	FinalizeHeaders(ctx context.Context) (memfileHeader, rootfsHeader []byte, err error)
}

// NewBuildUploader creates a BuildUploader for the given snapshot.
// If cfg is non-nil, compression is used (V4 headers). Otherwise, uncompressed (V3 headers).
// pending is shared across layers for multi-layer builds; nil is fine for single-layer.
func NewBuildUploader(snapshot *Snapshot, u storage.Uploader, paths storage.Paths, cfg *storage.CompressConfig, pending *PendingBuildInfo) BuildUploader {
	base := buildUploader{
		paths:    paths,
		uploader: u,
		snapshot: snapshot,
	}

	if cfg != nil {
		if pending == nil {
			pending = &PendingBuildInfo{}
		}

		return &compressedUploader{
			buildUploader: base,
			pending:       pending,
			cfg:           cfg,
		}
	}

	return &uncompressedUploader{buildUploader: base}
}

// buildUploader contains fields and helpers shared by both implementations.
type buildUploader struct {
	paths    storage.Paths
	uploader storage.Uploader
	snapshot *Snapshot
}

// diffPath returns the cache path for a diff, or nil if the diff is NoDiff.
func diffPath(d build.Diff) (*string, error) {
	if _, ok := d.(*build.NoDiff); ok {
		return nil, nil
	}

	p, err := d.CachePath()
	if err != nil {
		return nil, err
	}

	return &p, nil
}

func (b *buildUploader) uploadCompressedFile(ctx context.Context, local, remote string, cfg *storage.CompressConfig) (*storage.FrameTable, [32]byte, error) {
	cb, ok := b.uploader.(storage.CompressableStore)
	if !ok {
		return nil, [32]byte{}, fmt.Errorf("backend %T does not support compressed uploads", b.uploader)
	}

	file, err := os.Open(local)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to open local file %s: %w", local, err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to stat local file %s: %w", local, err)
	}

	uploader, err := cb.NewPartUploader(ctx, remote, map[string]string{
		storage.MetadataKeyUncompressedSize: strconv.FormatInt(fi.Size(), 10),
	})
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to create part uploader: %w", err)
	}

	return storage.CompressStream(ctx, file, cfg, uploader, cfg.FrameEncodeWorkers)
}

func (b *buildUploader) scheduleAlwaysUploads(eg *errgroup.Group, ctx context.Context) {
	eg.Go(func() error {
		return storage.PutBlobFromFile(ctx, b.uploader, b.paths.Snapfile(), b.snapshot.Snapfile.Path())
	})

	eg.Go(func() error {
		return storage.PutBlobFromFile(ctx, b.uploader, b.paths.Metadata(), b.snapshot.Metafile.Path())
	})
}

// pendingBuildInfo pairs a FrameTable with the uncompressed file size and
// uncompressed-data checksum so all can be stored in the header after uploads complete.
type pendingBuildInfo struct {
	ft       *storage.FrameTable
	fileSize int64
	checksum [32]byte
}

// PendingBuildInfo collects FrameTables and file sizes from compressed data
// uploads across all layers. After all data files are uploaded, the collected
// tables are applied to headers before the compressed headers are serialized
// and uploaded. Safe for concurrent use from errgroup goroutines.
type PendingBuildInfo struct {
	mu sync.Mutex
	m  map[string]pendingBuildInfo
}

func pendingBuildInfoKey(buildID, fileType string) string {
	return buildID + "/" + fileType
}

func (p *PendingBuildInfo) add(key string, ft *storage.FrameTable, fileSize int64, checksum [32]byte) {
	if ft == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.m == nil {
		p.m = make(map[string]pendingBuildInfo)
	}

	p.m[key] = pendingBuildInfo{ft: ft, fileSize: fileSize, checksum: checksum}
}

func (p *PendingBuildInfo) get(key string) *pendingBuildInfo {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, ok := p.m[key]
	if !ok {
		return nil
	}

	return &info
}

func (p *PendingBuildInfo) applyToHeader(h *header.Header, fileType string) error {
	if h == nil {
		return nil
	}

	// Track frame cursor per build to avoid O(N^2) rescanning.
	cursors := make(map[string]int)

	for _, mapping := range h.Mapping {
		key := pendingBuildInfoKey(mapping.BuildId.String(), fileType)
		info := p.get(key)

		if info == nil {
			continue
		}

		cursor := cursors[key]
		next, err := mapping.SetFrames(info.ft, cursor)
		if err != nil {
			return fmt.Errorf("apply frames to mapping at offset %d for build %s: %w",
				mapping.Offset, mapping.BuildId.String(), err)
		}
		cursors[key] = next

		// Populate BuildFiles with size and checksum for this build.
		if h.BuildFiles == nil {
			h.BuildFiles = make(map[uuid.UUID]header.BuildFileInfo)
		}
		h.BuildFiles[mapping.BuildId] = header.BuildFileInfo{
			Size:     info.fileSize,
			Checksum: info.checksum,
		}
	}

	return nil
}
