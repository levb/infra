package testutils

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

var _ block.ReadonlyDevice = (*ZeroDevice)(nil)

type ZeroDevice struct {
	blockSize int
	size      int
	header    *header.Header
}

func NewZeroDevice(size int, blockSize int) (*ZeroDevice, error) {
	h, err := header.NewHeader(header.NewTemplateMetadata(
		uuid.Nil,
		blockSize,
		size,
	),
		[]*header.BuildMap{
			{
				Offset:             0,
				Length:             size,
				BuildId:            uuid.Nil,
				BuildStorageOffset: 0,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create header: %w", err)
	}

	return &ZeroDevice{
		size:      size,
		blockSize: blockSize,
		header:    h,
	}, nil
}

func (z *ZeroDevice) ReadAt(_ context.Context, p []byte, _ int) (n int, err error) {
	clear(p)

	return len(p), nil
}

func (z *ZeroDevice) BlockSize() int {
	return z.blockSize
}

func (z *ZeroDevice) Slice(_ context.Context, _, length int) ([]byte, error) {
	return make([]byte, length), nil
}

func (z *ZeroDevice) Header() *header.Header {
	return z.header
}

func (z *ZeroDevice) Close() error {
	return nil
}

func (z *ZeroDevice) Size(_ context.Context) (int, error) {
	return z.size, nil
}
