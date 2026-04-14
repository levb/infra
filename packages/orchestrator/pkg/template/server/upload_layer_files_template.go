package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/paths"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const signedUrlExpiration = time.Minute * 30

func (s *ServerStore) InitLayerFileUpload(ctx context.Context, in *templatemanager.InitLayerFileUploadRequest) (*templatemanager.InitLayerFileUploadResponse, error) {
	ctx, childSpan := tracer.Start(ctx, "template-create")
	defer childSpan.End()

	// default to scope by template ID
	cacheScope := in.GetTemplateID()
	if in.CacheScope != nil {
		cacheScope = in.GetCacheScope()
	}

	path := paths.GetLayerFilesCachePath(cacheScope, in.GetHash())

	signedUrl, err := s.buildStore.SignedUploadURL(ctx, path, signedUrlExpiration)
	if err != nil {
		return nil, fmt.Errorf("failed to get signed url: %w", err)
	}

	// Check if the file already exists by trying to get its size
	_, err = s.buildStore.Size(ctx, path)
	exists := err == nil
	if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return nil, fmt.Errorf("failed to check if layer files exists: %w", err)
	}

	return &templatemanager.InitLayerFileUploadResponse{
		Present: exists,
		Url:     &signedUrl,
	}, nil
}
