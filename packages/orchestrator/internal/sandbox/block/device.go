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

// Chunker is an interface for reading block data from either local cache or remote storage.
//
// Implementations (all store some UNCOMPRESSED data for return, differ in caching strategy):
//
//   - UncompressedMMapChunker: Fetches uncompressed data → stores in mmap (Cache).
//     For uncompressed source files only.
//
//   - DecompressMMapChunker: Fetches compressed frames → decompresses immediately →
//     stores UNCOMPRESSED data in mmap (Cache). Like UncompressedMMapChunker but
//     handles compressed sources.
//
//   - CompressMMapLRUChunker: Two-level cache:
//     L1 = LRU for decompressed frames (in memory)
//     L2 = mmap for COMPRESSED frames (on disk, sized to rawSize/C space)
//     On L1 miss: read compressed from L2 mmap → decompress → add to L1.
//     On L2 miss: fetch from storage → store in L2 → decompress → add to L1.
//
// Contract:
//   - Slice() returns a reference to internal data. Callers MUST NOT modify the returned bytes.
//   - The returned slice is valid until Close() is called or (for LRU-based chunkers) the
//     underlying frame is evicted. UFFD handlers should copy to the faulting page immediately.
type Chunker interface {
	// Chunk fetches a single block-aligned chunk at [off, off+length) and returns
	// a view into the data. Callers MUST request block-aligned ranges that fall
	// within a single storage chunk (MemoryChunkSize for uncompressed, one frame
	// for compressed). The returned slice references internal storage and MUST NOT
	// be modified.
	Chunk(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error)
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
