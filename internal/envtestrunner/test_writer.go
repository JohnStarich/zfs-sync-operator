package envtestrunner

import (
	"io"
	"sync/atomic"
)

// Writer is a thread-safe writer that sends writes to calls to testing.T.Log()
type Writer struct {
	writer atomic.Pointer[io.Writer]
}

type testingLogger interface {
	Log(args ...any)
	Cleanup(func())
}

// NewWriter returns a new io.Writer that sends all writes to the current test t's log
func NewWriter(t testingLogger) *Writer {
	w := &Writer{}
	t.Cleanup(w.cleanUp)
	var testW io.Writer = testWriter{testingTB: t}
	w.writer.Store(&testW)
	return w
}

func (w *Writer) cleanUp() {
	w.writer.Store(&io.Discard)
}

func (w *Writer) Write(b []byte) (int, error) {
	return (*w.writer.Load()).Write(b)
}

type testWriter struct {
	testingTB testingLogger
}

func (w testWriter) Write(b []byte) (int, error) {
	w.testingTB.Log(string(b))
	return len(b), nil
}
