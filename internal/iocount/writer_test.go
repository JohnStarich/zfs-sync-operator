package iocount

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockWriter struct {
	closed bool
	io.Writer
}

func (m *mockWriter) Close() error {
	m.closed = true
	return nil
}

func TestWriter(t *testing.T) {
	t.Parallel()
	var buffer bytes.Buffer
	w := &mockWriter{Writer: &buffer}
	countWriter := NewWriter(w)
	const (
		contents1 = "foo bar baz"
		contents2 = "biff boo"
	)
	n1, err := countWriter.Write([]byte(contents1))
	assert.NoError(t, err)
	assert.Equal(t, n1, len(contents1))
	n2, err := countWriter.Write([]byte(contents2))
	assert.NoError(t, err)
	assert.Equal(t, n2, len(contents2))

	assert.NoError(t, countWriter.Close())

	assert.True(t, w.closed)
	assert.Equal(t, contents1+contents2, buffer.String())
	assert.Equal(t, int64(n1+n2), countWriter.Count())
}

func TestWriterWriteConcurrently(t *testing.T) {
	t.Parallel()
	w := &mockWriter{Writer: io.Discard}
	countWriter := NewWriter(w)

	var (
		writes      = 10
		writeLength = len(fmt.Sprintf("%d", writes-1))
	)
	formatString := fmt.Sprintf("%%0%dd", writeLength) // e.g. %02d -> 01, 02, ...

	var waitGroup sync.WaitGroup
	for index := range writes {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			_, err := fmt.Fprintf(countWriter, formatString, index)
			assert.NoError(t, err)
		}()
	}
	waitGroup.Wait()
	assert.Equal(t, int64(writes*writeLength), countWriter.Count())
}
