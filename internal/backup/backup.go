// Package backup defines and reconciles the [Backup] custom resource
package backup

import (
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// MustAddToScheme adds the Pool scheme to s
func MustAddToScheme(s *runtime.Scheme) {
	schemeBuilder := &scheme.Builder{
		GroupVersion: schema.GroupVersion{
			Group:   name.Domain,
			Version: "v1alpha1",
		},
	}
	schemeBuilder.Register(&Backup{}, &BackupList{})
	err := schemeBuilder.AddToScheme(s)
	if err != nil {
		panic(err)
	}
}

// Backup represents a template to execute new backups, to send ZFS snapshots between hosts
type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Spec    `json:"spec,omitempty"`
	Status *Status `json:"status,omitempty"`
}

// DeepCopyObject implements [runtime.Object]
func (b *Backup) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(b) }

// Spec defines the desired offsite [Backup] source and destination
type Spec struct {
	Destination corev1.LocalObjectReference `json:"destination"` // The destination pool to "zfs receive" snapshots.
	Source      corev1.LocalObjectReference `json:"source"`      // The source pool to "zfs send" snapshots from.
}

// Status holds status information for a [Backup]
type Status struct {
	State            State                        `json:"state"`
	Reason           string                       `json:"reason"`
	LastSentSnapshot *corev1.LocalObjectReference `json:"lastSentSnapshot,omitempty"`
}

// BackupList is a list of [Backup]. Required to perform a Watch.
//
//nolint:revive // The naming scheme XXXList is required to perform a Watch.
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}

// DeepCopyObject implements [runtime.Object]
func (l *BackupList) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(l) }
