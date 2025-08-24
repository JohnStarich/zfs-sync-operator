package backup

import (
	"io"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/iocount"
)

// pipe is a readable/writable/closable unbuffered pipe that fails with [io.ErrUnexpectedEOF]
// when idleTimeout elapses during a Read or Write.
//
// The pipe and all connected parts must be thrown away immediately when this error is returned
// to avoid undefined behavior from retaining the read/write buffer.
type pipe struct {
	io.Reader
	*iocount.Writer
	idleTimeout time.Duration
}

// newPipe returns a new [pipe]
func newPipe(idleTimeout time.Duration) *pipe {
	r, w := io.Pipe()
	countWriter := iocount.NewWriter(w)
	return &pipe{
		Reader:      r,
		Writer:      countWriter,
		idleTimeout: idleTimeout,
	}
}

type ioResult struct {
	N   int
	Err error
}

func (p *pipe) Read(b []byte) (int, error) {
	return p.doWithIdleTimeout(p.Reader.Read, b)
}

func (p *pipe) Write(b []byte) (int, error) {
	return p.doWithIdleTimeout(p.Writer.Write, b)
}

func (p *pipe) doWithIdleTimeout(doIO func([]byte) (int, error), b []byte) (int, error) {
	results := make(chan ioResult)
	go func() {
		// NOTE: This is not ideal. Operating on 'b' after the caller returns violates the io.Reader/io.Writer interface contract.
		//
		// We're making assumptions here to keep client code as simple as possible.
		// Specifically, we're constraining this to "stuck" scenarios and expecting the buffers to all be thrown away immediately when we time out.
		n, err := doIO(b)
		results <- ioResult{N: n, Err: err}
	}()
	select {
	case result := <-results:
		return result.N, result.Err
	case <-time.After(p.idleTimeout):
		return 0, io.ErrUnexpectedEOF
	}
}
