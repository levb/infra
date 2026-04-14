package storage

import (
	"context"
	"fmt"
	"io"
)

// LayerReader binds a Fetcher to a specific storage path and FrameTable,
// providing transparent decompression. It satisfies the block.DataReader
// interface so the chunker never sees FrameTable or compression details.
type LayerReader struct {
	src  Fetcher
	path string
	size int64
	ft   *FrameTable
}

// NewLayerReader creates a LayerReader for a build layer's data file.
// When ft is nil, reads are uncompressed pass-through to Fetcher.Fetch.
func NewLayerReader(src Fetcher, path string, size int64, ft *FrameTable) *LayerReader {
	return &LayerReader{src: src, path: path, size: size, ft: ft}
}

// Read returns decompressed bytes for the given virtual (uncompressed) offset.
func (r *LayerReader) Read(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	if !r.ft.IsCompressed() {
		return r.src.Fetch(ctx, r.path, offset, length)
	}

	frameStart, frameSize, err := r.ft.FrameFor(offset)
	if err != nil {
		return nil, fmt.Errorf("LayerReader: frame lookup for offset %d: %w", offset, err)
	}

	raw, err := r.src.Fetch(ctx, r.path, frameStart.C, int64(frameSize.C))
	if err != nil {
		return nil, fmt.Errorf("LayerReader: fetch compressed frame at C=%d: %w", frameStart.C, err)
	}

	decompressed, err := newDecompressingReadCloser(raw, r.ft.CompressionType())
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("LayerReader: create decompressor: %w", err)
	}

	return decompressed, nil
}

// FetchRegion returns the natural fetch unit containing the given virtual offset.
// Uncompressed: MemoryChunkSize-aligned. Compressed: frame boundary.
func (r *LayerReader) FetchRegion(offset int64) (start, length int64) {
	if !r.ft.IsCompressed() {
		start = (offset / MemoryChunkSize) * MemoryChunkSize
		return start, min(int64(MemoryChunkSize), r.size-start)
	}

	frameStart, frameSize, err := r.ft.FrameFor(offset)
	if err != nil {
		start = (offset / MemoryChunkSize) * MemoryChunkSize
		return start, min(int64(MemoryChunkSize), r.size-start)
	}

	return frameStart.U, int64(frameSize.U)
}

// Size returns the uncompressed size of the layer.
func (r *LayerReader) Size() int64 { return r.size }

// FrameTable returns the frame table, if any. Used only by header serialization.
func (r *LayerReader) FrameTable() *FrameTable { return r.ft }
