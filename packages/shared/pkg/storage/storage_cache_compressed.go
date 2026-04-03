package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
)

// openReaderCompressed handles the compressed cache path for OpenRangeReader.
// NFS stores compressed frames (.frm); on hit we decompress, on miss we fetch
// raw compressed bytes and tee them to NFS on Close.
func (c *cachedSeekable) openReaderCompressed(ctx context.Context, offsetU int64, frameTable *FrameTable) (io.ReadCloser, error) {
	frameStart, frameSize, err := frameTable.FrameFor(offsetU)
	if err != nil {
		return nil, fmt.Errorf("cache OpenRangeReader: frame lookup for offset %d: %w", offsetU, err)
	}

	framePath := makeFrameFilename(c.path, frameStart, frameSize)

	// Cache hit: open compressed frame from NFS and wrap with decompressor.
	f, err := os.Open(framePath)

	switch {
	case err == nil:
		recordCacheRead(ctx, true, int64(frameSize.C), cacheTypeSeekable, cacheOpOpenRangeReader)

		decompressed, err := newDecompressingReadCloser(f, frameTable.CompressionType())
		if err != nil {
			f.Close()

			return nil, fmt.Errorf("cache OpenRangeReader: decompress cached frame: %w", err)
		}

		return decompressed, nil
	case !os.IsNotExist(err):
		recordCacheReadError(ctx, cacheTypeSeekable, cacheOpOpenRangeReader, err)
	}

	// Cache miss: fetch raw compressed bytes via OpenRangeReader(nil frameTable).
	raw, err := c.inner.OpenRangeReader(ctx, frameStart.C, int64(frameSize.C), nil)
	if err != nil {
		return nil, fmt.Errorf("cache OpenRangeReader: raw fetch at C=%d: %w", frameStart.C, err)
	}

	recordCacheRead(ctx, false, int64(frameSize.C), cacheTypeSeekable, cacheOpOpenRangeReader)

	// TeeReader: as the decompressor reads compressed bytes, they are
	// captured in compressedBuf for async NFS write-back on Close.
	var compressedBuf bytes.Buffer
	compressedBuf.Grow(int(frameSize.C))
	tee := io.TeeReader(raw, &compressedBuf)

	dec, err := NewDecompressingReader(tee, frameTable.CompressionType())
	if err != nil {
		raw.Close()

		return nil, fmt.Errorf("cache OpenRangeReader: create decompressor: %w", err)
	}

	return &decompressingCacheReader{
		decompressor:  dec,
		raw:           raw,
		compressedBuf: &compressedBuf,
		expectedSize:  int(frameSize.C),
		cache:         c,
		ctx:           ctx,
		framePath:     framePath,
		offset:        offsetU,
	}, nil
}

// decompressingCacheReader wraps a decompressing reader. On Close, it writes the
// accumulated compressed bytes to the NFS cache asynchronously.
type decompressingCacheReader struct {
	decompressor  io.ReadCloser // decompresses on Read
	raw           io.ReadCloser // underlying compressed stream (must be closed)
	compressedBuf *bytes.Buffer
	expectedSize  int
	cache         *cachedSeekable
	ctx           context.Context //nolint:containedctx // needed for async cache write-back in Close
	framePath     string
	offset        int64
}

func (r *decompressingCacheReader) Read(p []byte) (int, error) {
	return r.decompressor.Read(p)
}

func (r *decompressingCacheReader) Close() error {
	if err := r.decompressor.Close(); err != nil {
		r.raw.Close()

		return err
	}

	if err := r.raw.Close(); err != nil {
		return err
	}

	if !skipCacheWriteback(r.ctx) && isCompleteRead(r.compressedBuf.Len(), r.expectedSize, nil) {
		data := make([]byte, r.compressedBuf.Len())
		copy(data, r.compressedBuf.Bytes())

		r.cache.goCtx(r.ctx, func(ctx context.Context) {
			if err := r.cache.writeToCache(ctx, r.offset, r.framePath, data); err != nil {
				recordCacheWriteError(ctx, cacheTypeSeekable, cacheOpOpenRangeReader, err)
			}
		})
	}

	return nil
}

// makeFrameFilename returns the NFS cache path for a compressed frame.
// Format: {cacheBasePath}/{016xC}-{xC}.frm
func makeFrameFilename(cacheBasePath string, offset FrameOffset, size FrameSize) string {
	return fmt.Sprintf("%s/%016x-%x.frm", cacheBasePath, offset.C, size.C)
}
