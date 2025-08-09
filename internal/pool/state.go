package pool

import (
	"fmt"
	"iter"
	"strings"

	"github.com/pkg/errors"
)

// State represents all pool lifecycle states
type State int

// Pool lifecycle states
const (
	Error State = iota // The operator failed to determine pool state

	Degraded // ZFS reported a pool state of DEGRADED
	Faulted  // ZFS reported a pool state of FAULTED
	NotFound // ZFS could not a pool with the configured name
	Online   // ZFS reported a pool state of ONLINE
	Unknown  // ZFS reported an unexpected pool state

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

func stateFromStateField(stateField string) State {
	// A pool's health status is described by one of three states: online, degraded, or faulted.
	// - https://openzfs.github.io/openzfs-docs/man/v0.8/8/zpool.8.html#Device_Failure_and_Recovery
	state := toState(normalizeStateToPossibleEnumName(stateField))
	switch state {
	case Online, Degraded, Faulted:
		return state
	case Error, NotFound, Unknown, maxState:
		// Not real zpool states
		return Unknown
	default:
		return Unknown
	}
}

func toState(possibleStateName string) State {
	for s := range AllStates() {
		if possibleStateName == s.String() {
			return s
		}
	}
	return Unknown
}

func normalizeStateToPossibleEnumName(state string) string {
	if len(state) <= 1 {
		return strings.ToUpper(state)
	}
	return strings.ToUpper(state[0:1]) + strings.ToLower(state[1:])
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
	return errors.Errorf("invalid pool state name: %s", b)
}

// MarshalText implements [encoding.TextMarshaler]
func (s State) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s State) String() string {
	switch s {
	case Degraded:
		return "Degraded"
	case Error:
		return "Error"
	case Faulted:
		return "Faulted"
	case NotFound:
		return "NotFound"
	case Online:
		return "Online"
	case Unknown:
		return "Unknown"
	case maxState:
	default:
	}
	panic(fmt.Sprintf("unrecognized pool state: %d", s))
}
