package block

import (
	"context"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// ChunkerStats holds per-chunker statistics for benchmarking compression modes.
type ChunkerStats struct {
	ObjectPath      string // e.g., "abc123-def/memfile"
	ChunkerType     string // "UncompressedMMap", "CompressMMapLRU", etc.
	CompressionType string // "none", "lz4", "zstd"

	// Slice stats
	Slices     int64 // total Slice() calls
	SliceBytes int64 // total bytes requested via Slice()

	// Fetch stats (remote storage)
	Fetches    int64 // remote storage fetches
	FetchBytes int64 // bytes fetched from remote

	// Decompression stats (compressed chunkers only)
	Decompressions    int64 // frames decompressed
	DecompInputBytes  int64 // compressed bytes fed to decompressor
	DecompOutputBytes int64 // uncompressed bytes produced
	DecompDurationNs  int64 // total decompression wall-clock time (ns)

	// Mmap RSS (chunkers with mmap caches)
	MmapRSSBytes int64 // resident set size of mmap at stats time
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
//   - CompressLRUChunker: Fetches compressed frames → decompresses → stores in LRU.
//     No local mmap; relies on NFS cache for compressed frames. Re-decompresses on
//     LRU miss. Legacy, being phased out.
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
	// Slice returns a view into the data at [off, off+length).
	//
	// Contract:
	//   - For compressed data (ft != nil): cross-frame requests are handled via slow path
	//     (assembling from multiple frames with tracing)
	//   - The returned slice references internal storage and MUST NOT be modified
	//   - For UFFD: use the slice immediately to copy into the faulting page
	Slice(ctx context.Context, off, length int64, ft *storage.FrameTable) ([]byte, error)
	Close() error
	FileSize() (int64, error)
	// Stats returns per-chunker statistics for benchmarking.
	Stats() ChunkerStats
}

// Verify that chunker types implement Chunker.
var (
	_ Chunker = (*UncompressedMMapChunker)(nil)
	_ Chunker = (*DecompressMMapChunker)(nil)
	_ Chunker = (*CompressLRUChunker)(nil)
	_ Chunker = (*CompressMMapLRUChunker)(nil)
)

const (
	pullType       = "pull-type"
	pullTypeLocal  = "local"
	pullTypeRemote = "remote"

	failureReason = "failure-reason"

	failureTypeLocalRead      = "local-read"
	failureTypeLocalReadAgain = "local-read-again"
	failureTypeCacheFetch     = "cache-fetch"
)
