package iocount

import (
	"io"
	"sync/atomic"
)

type Reader struct {
	count  atomic.Int64
	reader io.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		reader: r,
	}
}

func (r *Reader) Count() int64 {
	return r.count.Load()
}

func (r *Reader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.count.Add(int64(n))
	return n, err
}
