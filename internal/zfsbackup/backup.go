package zfsbackup

import (
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	schemeBuilder.Register(&ZFSBackup{})
}

type ZFSBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ZFSBackupSpec   `json:"spec,omitempty"`
	Status ZFSBackupStatus `json:"status,omitempty"`
}

func (z *ZFSBackup) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(z) }

type ZFSBackupSpec struct{}

type ZFSBackupStatus struct{}
