package backup

import (
	"io"
	"sync/atomic"
)

type countWriter struct {
	count  atomic.Int64
	writer io.WriteCloser
}

func wrapAsCountWriter(w io.WriteCloser) *countWriter {
	return &countWriter{
		writer: w,
	}
}

func (c *countWriter) Count() int64 {
	return c.count.Load()
}

func (c *countWriter) Write(b []byte) (int, error) {
	n, err := c.writer.Write(b)
	c.count.Add(int64(n))
	return n, err
}

func (c *countWriter) Close() error { return c.writer.Close() }
