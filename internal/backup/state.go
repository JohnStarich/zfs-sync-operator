package backup

import (
	"fmt"
	"iter"

	"github.com/pkg/errors"
)

// State represents all backup lifecycle states
type State int

// Backup lifecycle states
const (
	UnexpectedError State = iota // The operator failed unexpectedly

	Error    // The operator failed to send recent snapshots
	NotReady // One or both Pools are not ready to send/receive snapshots
	Ready    // Backup is ready to send/receive snapshots
	Sending  // Backup is sending a snapshot

	maxState // marker to select all possible states
)

// AllStates returns an iterator over all valid values of [State]
func AllStates() iter.Seq[State] {
	return func(yield func(state State) bool) {
		for state := range maxState {
			if !yield(state) {
				return
			}
		}
	}
}

// UnmarshalText implements [encoding.TextUnmarshaler]
func (s *State) UnmarshalText(b []byte) error {
	str := string(b)
	for state := range AllStates() {
		if str == state.String() {
			*s = state
			return nil
		}
	}
	return errors.Errorf("invalid backup state name: %s", b)
}

// MarshalText implements [encoding.TextMarshaler]
func (s State) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s State) String() string {
	switch s {
	case Error:
		return "Error"
	case NotReady:
		return "NotReady"
	case Ready:
		return "Ready"
	case Sending:
		return "Sending"
	case UnexpectedError:
		return "UnexpectedError"
	case maxState:
	default:
	}
	panic(fmt.Sprintf("unrecognized backup state: %d", s))
}
