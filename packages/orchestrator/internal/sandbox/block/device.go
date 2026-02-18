package block

import (
	"context"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// BytesNotAvailableError is returned when the requested bytes are not
// yet available in the mmap cache.
type BytesNotAvailableError struct{}

func (BytesNotAvailableError) Error() string {
	return "The requested bytes are not available on the device"
}

// Reader reads data using an optional FrameTable for compression awareness.
type Reader interface {
	ReadBlock(ctx context.Context, p []byte, off int64, ft *storage.FrameTable) (int, error)
	GetBlock(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error)
}

// Slicer is the block-level slice interface used by ReadonlyDevice.
type Slicer interface {
	Slice(ctx context.Context, off, length int64) ([]byte, error)
	BlockSize() int64
}

type ReadonlyDevice interface {
	storage.SeekableReader
	Close() error
	Slicer
	BlockSize() int64
	Header() *header.Header
}

type Device interface {
	ReadonlyDevice
	WriteAt(p []byte, off int64) (n int, err error)
}
