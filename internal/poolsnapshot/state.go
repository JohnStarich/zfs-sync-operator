package poolsnapshot

type State string

const (
	Completed = State("Completed")
	Error     = State("Error")
	Pending   = State("Pending")
	Running   = State("Running")
)
