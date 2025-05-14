package backup

import (
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	schemeBuilder.Register(&Backup{}, &BackupList{})
}

type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupSpec   `json:"spec,omitempty"`
	Status BackupStatus `json:"status,omitempty"`
}

func (b *Backup) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(b) }

type BackupSpec struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

type BackupStatus struct {
	State string `json:"state,omitempty"`
}

type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}

func (l *BackupList) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(l) }
