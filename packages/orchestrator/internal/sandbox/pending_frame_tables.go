package sandbox

import (
	"fmt"
	"sync"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// PendingFrameTables collects FrameTables from compressed data uploads across
// all layers. After all data files are uploaded, the collected tables are applied
// to headers before the compressed headers are serialized and uploaded.
type PendingFrameTables struct {
	tables sync.Map // key: "buildId/fileType", value: *storage.FrameTable
}

// Key builds the lookup key for a frame table: "buildId/fileType".
func PendingFrameTableKey(buildID, fileType string) string {
	return buildID + "/" + fileType
}

// Add stores a frame table for the given object key.
func (p *PendingFrameTables) Add(key string, ft *storage.FrameTable) {
	if ft == nil {
		return
	}

	p.tables.Store(key, ft)
}

// Get retrieves a stored frame table by key. Returns nil if not found.
func (p *PendingFrameTables) Get(key string) *storage.FrameTable {
	v, ok := p.tables.Load(key)
	if !ok {
		return nil
	}

	return v.(*storage.FrameTable)
}

// ApplyToHeader applies stored frame tables to header mappings for a given file type.
// For each mapping in the header, it looks up the frame table by "mappingBuildId/fileType"
// and calls mapping.AddFrames(ft).
func (p *PendingFrameTables) ApplyToHeader(h *header.Header, fileType string) error {
	if h == nil {
		return nil
	}

	for _, mapping := range h.Mapping {
		key := PendingFrameTableKey(mapping.BuildId.String(), fileType)
		ft := p.Get(key)

		if ft == nil {
			continue
		}

		if err := mapping.AddFrames(ft); err != nil {
			return fmt.Errorf("apply frames to mapping at offset %#x for build %s: %w",
				mapping.Offset, mapping.BuildId.String(), err)
		}
	}

	return nil
}
