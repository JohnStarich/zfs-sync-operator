package wireguard

import (
	goerrors "errors"
	"sync/atomic"

	"github.com/pkg/errors"
)

type lastErrorsLogger struct {
	maxErrs   int
	errPrefix string
	errs      atomic.Pointer[[]error]
}

// newLastErrorsLogger returns a new logger and its last N errors.
// Keeps the last N error logs as error types. Safe for concurrent use.
func newLastErrorsLogger(n int, errPrefix string) *lastErrorsLogger {
	logger := &lastErrorsLogger{
		maxErrs:   n,
		errPrefix: errPrefix,
	}
	logger.errs.Store(new([]error))
	return logger
}

// LogError appends an error creates with errors.Errorf(format, args...)
func (l *lastErrorsLogger) LogError(format string, args ...any) {
	newErr := errors.Errorf(format, args...)
	for {
		oldErrs := l.errs.Load()
		newErrs := append(*oldErrs, newErr)
		if len(newErrs) > l.maxErrs {
			newErrs = newErrs[len(newErrs)-l.maxErrs:]
		}
		if l.errs.CompareAndSwap(oldErrs, &newErrs) {
			return
		}
	}
}

// LastErrors returns the last N logged errors joined together
func (l *lastErrorsLogger) LastErrors() error {
	err := goerrors.Join(*l.errs.Load()...)
	return errors.WithMessage(err, l.errPrefix)
}
