package backup

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var _ interface {
	io.Reader
	io.Writer
	io.Closer
} = &pipe{}

type fullReadResult struct {
	Data []byte
	N    int
	Err  error
}

func TestPipeReadWrite(t *testing.T) {
	const idleTimeout = 1 * time.Second
	pipe := newPipe(idleTimeout)
	const someData = "some data"

	wait, done := context.WithTimeout(context.Background(), idleTimeout*2)
	t.Cleanup(func() { <-wait.Done() })
	go func() {
		n, err := pipe.Write([]byte(someData))
		assert.NoError(t, err)
		assert.Equal(t, len(someData), n)
		done()
	}()

	b := make([]byte, len(someData))
	n, err := pipe.Read(b)
	assert.NoError(t, err)
	assert.Equal(t, len(someData), n)
	assert.Equal(t, someData, string(b))
}

func TestPipeIdleTimeout(t *testing.T) {
	t.Parallel()
	const idleTimeout = 1 * time.Second
	pipe := newPipe(idleTimeout)
	readResults := make(chan fullReadResult)

	makeByteArray := func() []byte {
		return make([]byte, 1)
	}

	go func() {
		b := makeByteArray()
		n, err := pipe.Read(b)
		readResults <- fullReadResult{
			Data: b,
			N:    n,
			Err:  err,
		}
	}()
	select {
	case <-time.After(idleTimeout * 2):
		t.Error("read should time out at", idleTimeout)
	case result := <-readResults:
		assert.Equal(t, io.ErrUnexpectedEOF, result.Err)
		assert.Equal(t, makeByteArray(), result.Data)
		assert.Zero(t, result.N)
	}
}
