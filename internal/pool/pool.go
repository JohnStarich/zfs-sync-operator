// Package pool defines and reconciles the [Pool] custom resource
package pool

import (
	"net/netip"
	"strings"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/pkg/errors"
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
	schemeBuilder.Register(
		&PoolList{},
		&Pool{},
		&PoolSnapshotList{},
		&PoolSnapshot{},
	)
	err := schemeBuilder.AddToScheme(s)
	if err != nil {
		panic(err)
	}
}

// Pool represents a ZFS pool and its connection details, including WireGuard and SSH
type Pool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   *Spec   `json:"spec,omitempty"`
	Status *Status `json:"status,omitempty"`
}

// DeepCopyObject implements [ctrlruntime.Object]
func (p *Pool) DeepCopyObject() ctrlruntime.Object { return baddeepcopy.DeepCopy(p) }

func (p *Pool) validDatasetName(name string) bool {
	if name == p.Spec.Name {
		return true
	}
	const datasetSeparator = "/"
	prefix := p.Spec.Name + datasetSeparator
	return name != prefix && strings.HasPrefix(name, prefix)
}

// Spec defines the connection details for a [Pool], including WireGuard and SSH
type Spec struct {
	Name      string         `json:"name"`                // The name of the zpool
	SSH       *SSHSpec       `json:"ssh"`                 // How to connect over SSH.
	Snapshots *SnapshotsSpec `json:"snapshots,omitempty"` // How often to create snapshots on the source pool, and how many to keep.
	WireGuard *WireGuardSpec `json:"wireguard,omitempty"` // How to connect with SSH over WireGuard.
}

// SSHSpec defines the SSH connection details for a [Pool]
type SSHSpec struct {
	Address    string                   `json:"address"`
	HostKey    *[]byte                  `json:"hostKey,omitempty"` // The SSH host's public key. Used for verification after the pool's first connection.
	PrivateKey corev1.SecretKeySelector `json:"privateKey"`
	User       string                   `json:"user"`
}

// WireGuardSpec defines the WireGuard connection details for a [Pool]
type WireGuardSpec struct {
	LocalAddress    netip.Addr                `json:"localAddress"`
	LocalPrivateKey corev1.SecretKeySelector  `json:"localPrivateKey"`
	PeerAddress     string                    `json:"peerAddress"`
	PeerPublicKey   corev1.SecretKeySelector  `json:"peerPublicKey"`
	PresharedKey    *corev1.SecretKeySelector `json:"presharedKey,omitempty"`
}

// SnapshotsSpec defines the desired snapshot schedule and history to maintain
type SnapshotsSpec struct {
	Intervals []SnapshotIntervalSpec `json:"intervals,omitempty"`
	Template  SnapshotSpecTemplate   `json:"template"`
}

// SnapshotIntervalSpec configures a Pool snapshot interval, or schedule
type SnapshotIntervalSpec struct {
	Name         string          `json:"name"`         // The name of this interval. e.g. hourly, daily, weekly, monthly, yearly
	HistoryLimit uint            `json:"historyLimit"` // Keep N snapshots for this snapshot interval. Discard old snapshots when new ones become available.
	Interval     metav1.Duration `json:"interval"`     // Take snapshots every interval
}

func (i *SnapshotIntervalSpec) validate() (returnedErr error) {
	defer func() { returnedErr = errors.WithMessage(returnedErr, "snapshot interval") }()
	if i.Name == "" {
		return errors.New("name is required")
	}
	if i.HistoryLimit == 0 {
		return errors.New("history is required, must be a positive number of snapshots to keep")
	}
	if i.Interval.Duration == 0 {
		return errors.New("interval is required")
	}
	return nil
}

func (s *SnapshotsSpec) validateIntervals() ([]SnapshotIntervalSpec, error) {
	intervals := s.Intervals
	if len(intervals) == 0 {
		const (
			hour          = time.Hour
			day           = 24 * hour
			weekEstimate  = 7 * day
			yearEstimate  = 365 * day
			monthEstimate = yearEstimate / 12
			keepYears     = 2
		)
		intervals = []SnapshotIntervalSpec{
			// DO NOT MODIFY these lightly.
			// This default set of intervals determine a healthy set of snapshots, especially their names and history.
			// Modifying them can change the perceived safety of the operator.
			{
				Name:         "hourly",
				HistoryLimit: uint(day/hour) + 1,
				Interval:     metav1.Duration{Duration: hour},
			},
			{
				Name:         "daily",
				HistoryLimit: uint(weekEstimate/day) + 1,
				Interval:     metav1.Duration{Duration: day},
			},
			{
				Name:         "weekly",
				HistoryLimit: uint(monthEstimate/weekEstimate) + 1,
				Interval:     metav1.Duration{Duration: weekEstimate},
			},
			{
				Name:         "monthly",
				HistoryLimit: uint(yearEstimate/monthEstimate) + 1,
				Interval:     metav1.Duration{Duration: monthEstimate},
			},
			{
				Name:         "yearly",
				HistoryLimit: 2,
				Interval:     metav1.Duration{Duration: yearEstimate},
			},
		}
	}

	keys := make(map[string]struct{}, len(intervals))
	for _, interval := range intervals {
		if err := interval.validate(); err != nil {
			return nil, err
		}
		_, exists := keys[interval.Name]
		if exists {
			return nil, errors.Errorf("duplicate snapshot interval: %s", interval.Name)
		}
		keys[interval.Name] = struct{}{}
	}
	return intervals, nil
}

// Status holds status information for a [Pool]
type Status struct {
	State  State  `json:"state"`
	Reason string `json:"reason"`
}

// PoolList is a list of [Pool]. Required to perform a Watch.
//
//nolint:revive // The naming scheme XXXList is required to perform a Watch.
type PoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []*Pool `json:"items"`
}

// DeepCopyObject implements [ctrlruntime.Object]
func (l *PoolList) DeepCopyObject() ctrlruntime.Object { return baddeepcopy.DeepCopy(l) }
