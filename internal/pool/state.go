package pool

import "strings"

// State represents all pool lifecycle states
type State string

// Pool lifecycle states
const (
	Degraded = State("Degraded") // ZFS reported a pool state of DEGRADED
	Error    = State("Error")    // The operator failed to determine pool state
	Faulted  = State("Faulted")  // ZFS reported a pool state of FAULTED
	NotFound = State("NotFound") // ZFS could not a pool with the configured name
	Online   = State("Online")   // ZFS reported a pool state of ONLINE
	Unknown  = State("Unknown")  // ZFS reported an unexpected pool state
)

func stateFromStateField(stateField string) State {
	// A pool's health status is described by one of three states: online, degraded, or faulted.
	// - https://openzfs.github.io/openzfs-docs/man/v0.8/8/zpool.8.html#Device_Failure_and_Recovery
	state := toState(stateField)
	switch state {
	case Online, Degraded, Faulted:
		return state
	case Error, NotFound, Unknown:
		// Not real zpool states
		return Unknown
	default:
		return Unknown
	}
}

func toState(state string) State {
	if len(state) <= 1 {
		return State(strings.ToUpper(state))
	}
	return State(strings.ToUpper(state[0:1]) + strings.ToLower(state[1:]))
}
