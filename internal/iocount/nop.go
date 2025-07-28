package iocount

import "io"

// NopWriteCloser wraps an [io.Writer] and implements a no-op [io.Closer]
type NopWriteCloser struct {
	writer io.Writer
}

// NewNopWriteCloser returns w with a no-op Close method
func NewNopWriteCloser(w io.Writer) *NopWriteCloser {
	return &NopWriteCloser{
		writer: w,
	}
}

// Write implements [io.Writer]
func (n *NopWriteCloser) Write(b []byte) (int, error) { return n.writer.Write(b) }

// Close is a no-op
func (n *NopWriteCloser) Close() error { return nil }
