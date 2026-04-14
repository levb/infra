package testutils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func TemplateRootfs(ctx context.Context, buildID string) (*BuildDevice, *Cleaner, error) {
	var cleaner Cleaner

	paths := storage.Paths{
		BuildID: buildID,
	}

	s, err := storage.GetStore(ctx, storage.TemplateStoreConfig)
	if err != nil {
		return nil, &cleaner, fmt.Errorf("failed to get storage provider: %w", err)
	}

	h, err := header.LoadHeader(ctx, s, paths.RootfsHeader())
	if err != nil {
		id, err := uuid.Parse(buildID)
		if err != nil {
			return nil, &cleaner, fmt.Errorf("failed to parse build id: %w", err)
		}

		size, err := s.Size(ctx, paths.Rootfs())
		if err != nil {
			return nil, &cleaner, fmt.Errorf("failed to get object size: %w", err)
		}

		h, err = header.NewHeader(&header.Metadata{
			BuildId:     id,
			BaseBuildId: id,
			Size:        uint64(size),
			Version:     1,
			BlockSize:   header.RootfsBlockSize,
			Generation:  1,
		}, nil)
		if err != nil {
			return nil, &cleaner, fmt.Errorf("failed to create header for rootfs without header: %w", err)
		}
	}

	diffCacheDir := filepath.Join(os.TempDir(), fmt.Sprintf("%s-rootfs.diff.cache-%s", buildID, uuid.New().String()))

	err = os.MkdirAll(diffCacheDir, 0o755)
	if err != nil {
		return nil, &cleaner, fmt.Errorf("failed to create diff cache directory: %w", err)
	}

	cleaner.Add(func(context.Context) error {
		return os.RemoveAll(diffCacheDir)
	})

	flags, err := featureflags.NewClient()
	if err != nil {
		return nil, &cleaner, fmt.Errorf("failed to create feature flags client: %w", err)
	}

	diffs, err := build.NewDiffStore(
		cfg.Config{},
		flags,
		diffCacheDir,
		24*time.Hour,
		24*time.Hour,
	)
	if err != nil {
		return nil, &cleaner, fmt.Errorf("failed to create diff store: %w", err)
	}

	diffs.Start(ctx)

	cleaner.Add(func(context.Context) error {
		diffs.RemoveCache()

		return nil
	})

	cleaner.Add(func(context.Context) error {
		diffs.Close()

		return nil
	})

	m, err := metrics.NewMetrics(noop.NewMeterProvider())
	if err != nil {
		return nil, &cleaner, fmt.Errorf("failed to create metrics: %w", err)
	}

	buildDevice := NewBuildDevice(
		build.NewFile(h, diffs, build.Rootfs, s, m),
		h,
		int64(h.Metadata.BlockSize),
	)

	return buildDevice, &cleaner, nil
}
