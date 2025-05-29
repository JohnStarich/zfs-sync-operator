// Package pool defines and reconciles the [Pool] custom resource
package pool

import (
	"net/netip"

	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
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
			Group:   "zfs-sync-operator.johnstarich.com",
			Version: "v1alpha1",
		},
	}
	schemeBuilder.Register(&Pool{}, &PoolList{})
	err := schemeBuilder.AddToScheme(s)
	if err != nil {
		panic(err)
	}
}

// Pool represents a ZFS pool and its connection details, including WireGuard and SSH
type Pool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   Spec   `json:"spec,omitempty"`
	Status Status `json:"status,omitempty"`
}

// DeepCopyObject implements [runtime.Object]
func (p *Pool) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(p) }

// Spec defines the connection details for a [Pool], including WireGuard and SSH
type Spec struct {
	Name      string         `json:"name"`
	SSH       *SSHSpec       `json:"ssh"`
	WireGuard *WireGuardSpec `json:"wireguard,omitempty"`
}

type SSHSpec struct {
	User       string                   `json:"user"`
	Address    netip.AddrPort           `json:"address"`
	PrivateKey corev1.SecretKeySelector `json:"privateKey"`
}

type WireGuardSpec struct {
	DNSAddresses  []netip.Addr              `json:"dnsAddresses,omitempty"`
	PeerAddress   netip.AddrPort            `json:"peerAddress"`
	PeerPublicKey corev1.SecretKeySelector  `json:"peerPublicKey"`
	PresharedKey  *corev1.SecretKeySelector `json:"presharedKey,omitempty"`
	PrivateKey    corev1.SecretKeySelector  `json:"privateKey"`
}

// Status holds status information for a [Pool]
type Status struct {
	State  string `json:"state,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// PoolList is a list of [Pool]. Required to perform a Watch.
//
//nolint:revive // The naming scheme XXXList is required to perform a Watch.
type PoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pool `json:"items"`
}

// DeepCopyObject implements [runtime.Object]
func (l *PoolList) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(l) }
