package pool

type SnapshotState string

const (
	SnapshotCompleted = SnapshotState("Completed") // Pool snapshot across datasets completed successfully.
	SnapshotError     = SnapshotState("Error")     // Pool snapshot encountered an error.
	SnapshotFailed    = SnapshotState("Failed")    // Pool snapshot irrecoverably failed. Usually means the snapshot did not succeed before '.spec.notAfter'.
	SnapshotPending   = SnapshotState("Pending")   // Pool snapshot is in progress.
)
