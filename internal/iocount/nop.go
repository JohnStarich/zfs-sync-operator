package iocount

import "io"

type NopWriteCloser struct {
	writer io.Writer
}

func NewNopWriteCloser(w io.Writer) *NopWriteCloser {
	return &NopWriteCloser{
		writer: w,
	}
}

func (n *NopWriteCloser) Write(b []byte) (int, error) { return n.writer.Write(b) }
func (n *NopWriteCloser) Close() error                { return nil }
