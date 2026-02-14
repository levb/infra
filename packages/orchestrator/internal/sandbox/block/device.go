package block

import (
	"context"
	"io"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

type BytesNotAvailableError struct{}

func (BytesNotAvailableError) Error() string {
	return "The requested bytes are not available on the device"
}

type Chunker interface {
	Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error)
	Close() error
	FileSize() (int64, error)
}

// Verify that chunker types implement Chunker.
var (
	_ Chunker = (*UncompressedMMapChunker)(nil)
	_ Chunker = (*DecompressMMapChunker)(nil)
	_ Chunker = (*CompressMMapLRUChunker)(nil)
)

// Slicer is the block-level slice interface used by ReadonlyDevice.
type Slicer interface {
	Slice(ctx context.Context, off, length int64) ([]byte, error)
	BlockSize() int64
}

type ReadonlyDevice interface {
	storage.SeekableReader
	io.Closer
	Slicer
	BlockSize() int64
	Header() *header.Header
}

type Device interface {
	ReadonlyDevice
	io.WriterAt
}
