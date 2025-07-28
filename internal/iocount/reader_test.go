package iocount

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	t.Parallel()
	const (
		contents1 = "foo"
		contents2 = "bar"
	)
	countReader := NewReader(strings.NewReader(contents1 + contents2))

	buffer1 := make([]byte, 3)
	n1, err := countReader.Read(buffer1)
	assert.NoError(t, err)
	assert.Equal(t, n1, len(contents1))
	assert.Equal(t, contents1, string(buffer1))

	buffer2 := make([]byte, 3)
	n2, err := countReader.Read(buffer2)
	assert.NoError(t, err)
	assert.Equal(t, n2, len(contents2))
	assert.Equal(t, contents2, string(buffer2))

	assert.Equal(t, int64(n1+n2), countReader.Count())
}

type mockReader struct {
	reads chan []byte
}

func (m *mockReader) Read(b []byte) (int, error) {
	n := copy(b, <-m.reads)
	return n, nil
}

func TestReaderReadeConcurrently(t *testing.T) {
	t.Parallel()

	var (
		reads      = 10
		readLength = len(fmt.Sprintf("%d", reads-1))
	)
	formatString := fmt.Sprintf("%%0%dd", readLength) // e.g. %02d -> 01, 02, ...

	r := &mockReader{reads: make(chan []byte, reads)}
	for index := range reads {
		r.reads <- fmt.Appendf(nil, formatString, index)
	}
	countReader := NewReader(r)

	var waitGroup sync.WaitGroup
	for range reads {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			buf := make([]byte, readLength)
			n, err := countReader.Read(buf)
			assert.NoError(t, err)
			assert.Equal(t, readLength, n)
		}()
	}
	waitGroup.Wait()
	assert.Equal(t, int64(reads*readLength), countReader.Count())
	assert.Len(t, r.reads, 0)
}
