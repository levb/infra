package storage

import (
	"io"
)

type offsetReader struct {
	wrapped io.ReaderAt
	offset  int
}

var _ io.Reader = (*offsetReader)(nil)

func (r *offsetReader) Read(p []byte) (n int, err error) {
	n, err = r.wrapped.ReadAt(p, int64(r.offset))
	r.offset += n

	return
}

func newOffsetReader(reader io.ReaderAt, offset int) *offsetReader {
	return &offsetReader{reader, offset}
}
