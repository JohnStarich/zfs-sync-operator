package backup

import (
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	schemeBuilder.Register(&Backup{})
}

type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupSpec   `json:"spec,omitempty"`
	Status BackupStatus `json:"status,omitempty"`
}

func (z *Backup) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(z) }

type BackupSpec struct{}

type BackupStatus struct{}
