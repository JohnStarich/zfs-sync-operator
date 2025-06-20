package pool

type SnapshotState string

const (
	SnapshotCompleted = SnapshotState("Completed")
	SnapshotError     = SnapshotState("Error")
	SnapshotPending   = SnapshotState("Pending")
)
