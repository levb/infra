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

// Chunker fetches a single chunk from storage and returns a read-only view.
// Callers must request ranges within a single chunk boundary.
// The returned slice is valid until Close() (or LRU eviction for CompressMMapLRUChunker).
type Chunker interface {
	Chunk(ctx context.Context, off, blockSize int64, ft *storage.FrameTable) ([]byte, error)
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
