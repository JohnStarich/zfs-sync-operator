package backup

// State represents all backup lifecycle states
type State string

// Backup lifecycle states
const (
	Error    = State("Error")    // The operator failed to send recent snapshots
	NotReady = State("NotReady") // One or both Pools are not ready to send/receive snapshots
	Ready    = State("Ready")    // Backup is ready to send/receive snapshots
	Sending  = State("Sending")  // Backup is sending a snapshot
)
