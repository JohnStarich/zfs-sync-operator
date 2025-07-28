// Package iocount counts bytes written in various common I/O interface implementations.
package iocount

import (
	"io"
	"sync/atomic"
)

// Writer is a counting [io.Writer]. See [Writer.Count] for more detail.
type Writer struct {
	count  atomic.Int64
	writer io.WriteCloser
}

// NewWriter returns a new [Writer], which counts the number of bytes successfully written to w
func NewWriter(w io.WriteCloser) *Writer {
	return &Writer{
		writer: w,
	}
}

// Count returns the number of bytes written to the underlying writer
func (w *Writer) Count() int64 {
	return w.count.Load()
}

// Write implements [io.Writer]
func (w *Writer) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.count.Add(int64(n))
	return n, err
}

// Close implements [io.Closer]
func (w *Writer) Close() error { return w.writer.Close() }
