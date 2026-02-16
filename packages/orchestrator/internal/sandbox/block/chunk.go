package block

import (
	"context"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// Chunker is the interface satisfied by all chunker implementations.
// The ft parameter is used to look up compressed frames when available;
// if ft is nil or the compressed asset doesn't exist, the uncompressed
// path is used.
type Chunker interface {
	Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error)
	ReadAt(ctx context.Context, b []byte, off int64, ft *storage.FrameTable) (int, error)
	Close() error
	FileSize() (int64, error)
}

// BytesNotAvailableError is returned when the requested bytes are not
// yet available in the mmap cache.
type BytesNotAvailableError struct{}

func (BytesNotAvailableError) Error() string {
	return "The requested bytes are not available on the device"
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

const (
	pullType       = "pull-type"
	pullTypeLocal  = "local"
	pullTypeRemote = "remote"

	failureReason = "failure-reason"

	failureTypeLocalRead      = "local-read"
	failureTypeLocalReadAgain = "local-read-again"
	failureTypeRemoteRead     = "remote-read"
	failureTypeCacheFetch     = "cache-fetch"

	chunkerTypeAttr = "chunker"
	compressedAttr  = "compressed"

	ChunkerTypeDecompressMMap = "decompress-mmap"
)
