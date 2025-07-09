package iocount

import (
	"io"
	"sync/atomic"
)

type Writer struct {
	count  atomic.Int64
	writer io.WriteCloser
}

func NewWriter(w io.WriteCloser) *Writer {
	return &Writer{
		writer: w,
	}
}

func (w *Writer) Count() int64 {
	return w.count.Load()
}

func (w *Writer) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.count.Add(int64(n))
	return n, err
}

func (w *Writer) Close() error { return w.writer.Close() }
