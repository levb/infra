package testutils

import (
	"context"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

var _ block.ReadonlyDevice = (*BuildDevice)(nil)

type BuildDevice struct {
	*build.File

	header    *header.Header
	blockSize int
}

func NewBuildDevice(file *build.File, header *header.Header, blockSize int) *BuildDevice {
	return &BuildDevice{
		File:      file,
		header:    header,
		blockSize: blockSize,
	}
}

func (m *BuildDevice) Close() error {
	return nil
}

func (m *BuildDevice) BlockSize() int {
	return m.blockSize
}

func (m *BuildDevice) Header() *header.Header {
	return m.header
}

func (m *BuildDevice) Size(_ context.Context) (int, error) {
	return int(m.header.Metadata.Size), nil
}
