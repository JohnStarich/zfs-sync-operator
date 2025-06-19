package poolsnapshot

import (
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// MustAddToScheme adds the Pool scheme to s
func MustAddToScheme(s *ctrlruntime.Scheme) {
	schemeBuilder := &scheme.Builder{
		GroupVersion: schema.GroupVersion{
			Group:   name.Domain,
			Version: "v1alpha1",
		},
	}
	schemeBuilder.Register(&PoolSnapshot{}, &PoolSnapshotList{})
	err := schemeBuilder.AddToScheme(s)
	if err != nil {
		panic(err)
	}
}

// PoolSnapshot represents a set of ZFS dataset snapshots in a Pool.
// Pools create new PoolSnapshots on a schedule, and delete old ones as they age out.
type PoolSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Spec    `json:"spec,omitempty"`
	Status *Status `json:"status,omitempty"`
}

// DeepCopyObject implements [ctrlruntime.Object]
func (p *PoolSnapshot) DeepCopyObject() ctrlruntime.Object { return baddeepcopy.DeepCopy(p) }

// Spec defines the Pool to snapshot and the set of datasets snapshot
type Spec struct {
	Pool     corev1.LocalObjectReference `json:"pool"`     // The Pool to snapshot
	Datasets []DatasetSelector           `json:"datasets"` // The ZFS datasets to snapshot
	NotAfter metav1.Time                 `json:"notAfter"` // Do not attempt to snapshot after this time. Typically used alongside a snapshot interval, where the next snapshot will take over.
}

type DatasetSelector struct {
	Name      string                `json:"name"`
	Recursive *RecursiveDatasetSpec `json:"recursive,omitempty"`
}

type RecursiveDatasetSpec struct {
	// Skips the given child dataset names. Names are the fully-qualified names, including their parent dataset names.
	//
	// NOTE: To ensure snapshots are taken at the same instant, a fully recusrive snapshot is taken first, then
	// destroys the new snapshot on each child in skipChildren.
	SkipChildren []string `json:"skipChildren,omitempty"`
}

// Status holds status information for a [PoolSnapshot]
type Status struct {
	State  State  `json:"state"`
	Reason string `json:"reason"`
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
