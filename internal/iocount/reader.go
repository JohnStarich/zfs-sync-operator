package iocount

import (
	"io"
	"sync/atomic"
)

// Reader is a counting [io.Reader]. See [Reader.Count] for more detail.
type Reader struct {
	count  atomic.Int64
	reader io.Reader
}

// NewReader returns a new [Reader], which counts the number of bytes successfully read from r
func NewReader(r io.Reader) *Reader {
	return &Reader{
		reader: r,
	}
}

// Count returns the number of bytes read from the underlying reader
func (r *Reader) Count() int64 {
	return r.count.Load()
}

// Read implements [io.Reader]
func (r *Reader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.count.Add(int64(n))
	return n, err
}
