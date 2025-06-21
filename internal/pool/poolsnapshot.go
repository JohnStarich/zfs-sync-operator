package pool

import (
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlruntime "k8s.io/apimachinery/pkg/runtime"
)

// PoolSnapshot represents a set of ZFS dataset snapshots in a Pool.
// Pools create new PoolSnapshots on a schedule, and delete old ones as they age out.
type PoolSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotSpec    `json:"spec,omitempty"`
	Status *SnapshotStatus `json:"status,omitempty"`
}

// DeepCopyObject implements [ctrlruntime.Object]
func (s *PoolSnapshot) DeepCopyObject() ctrlruntime.Object { return baddeepcopy.DeepCopy(s) }

// SnapshotSpec defines the Pool to snapshot and the set of datasets snapshot
type SnapshotSpec struct {
	Pool                 corev1.LocalObjectReference `json:"pool"`     // The Pool to snapshot
	Deadline             metav1.Time                 `json:"deadline"` // Do not attempt to snapshot after this time. Typically used alongside a snapshot interval, where the next snapshot will take over. Failing to complete the snapshot by this time results in a Failed state.
	SnapshotSpecTemplate `json:",inline"`
}

type SnapshotSpecTemplate struct {
	Datasets []DatasetSelector `json:"datasets"` // The ZFS datasets to snapshot
}

type DatasetSelector struct {
	Name      string                `json:"name"`
	Recursive *RecursiveDatasetSpec `json:"recursive,omitempty"`
}

type RecursiveDatasetSpec struct {
	// Skips the given child dataset names. Names are the fully-qualified names, including their parent dataset names.
	SkipChildren []string `json:"skipChildren,omitempty"`
}

// SnapshotStatus holds status information for a [PoolSnapshot]
type SnapshotStatus struct {
	State  SnapshotState `json:"state"`
	Reason string        `json:"reason"`
}

// PoolSnapshotList is a list of [PoolSnapshot]. Required to perform a Watch.
//
//nolint:revive // The naming scheme XXXList is required to perform a Watch.
type PoolSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []*PoolSnapshot `json:"items"`
}

// DeepCopyObject implements [ctrlruntime.Object]
func (l *PoolSnapshotList) DeepCopyObject() ctrlruntime.Object { return baddeepcopy.DeepCopy(l) }
