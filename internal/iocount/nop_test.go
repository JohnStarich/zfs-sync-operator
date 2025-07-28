package iocount

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNopWriteCloser(t *testing.T) {
	t.Parallel()
	w := &mockWriter{Writer: io.Discard}
	nopCloser := NewNopWriteCloser(w)
	const contents = "foo"

	n, err := nopCloser.Write([]byte(contents))
	assert.NoError(t, err)
	assert.Equal(t, len(contents), n)
	assert.NoError(t, nopCloser.Close())
	assert.False(t, w.closed)
}
