package backup

// State represents all backup lifecycle states
type State string

// Backup lifecycle states
const (
	Ready    = State("Ready")
	NotReady = State("NotReady")
)
