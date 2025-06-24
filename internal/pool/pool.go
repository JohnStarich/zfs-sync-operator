// Package pool defines and reconciles the [Pool] custom resource
package pool

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"runtime"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/johnstarich/zfs-sync-operator/internal/pointer"
	"github.com/johnstarich/zfs-sync-operator/internal/wireguard"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
	DNSAddresses    []netip.Addr              `json:"dnsAddresses,omitempty"`
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

// WithConnection starts an SSH session (optionally over WireGuard) using p's Spec, runs do with the session, then tears everything down
func (p *Pool) WithConnection(ctx context.Context, client ctrlclient.Client, do func(*ssh.Client) error) (returnedErr error) {
	logger := log.FromContext(ctx)

	if p.Spec == nil || p.Spec.SSH == nil {
		return errors.New("ssh is required")
	}

	conn, err := p.dialSSHConnection(ctx, client)
	if err != nil {
		return err
	}
	defer tryNonCriticalCleanup(ctx, currentLine(), conn.Close)
	logger.Info("SSH TCP connection established")

	sshPrivateKey, err := getSecretKey(ctx, client, p.Namespace, p.Spec.SSH.PrivateKey)
	if err != nil {
		return err
	}
	signer, err := ssh.ParsePrivateKey(sshPrivateKey)
	if err != nil {
		return err
	}

	sshAddress := p.Spec.SSH.Address

	var hostKeyCallback ssh.HostKeyCallback
	if p.Spec.SSH.HostKey != nil {
		key, err := ssh.ParsePublicKey(*p.Spec.SSH.HostKey)
		if err != nil {
			return errors.WithMessage(err, "parse spec.ssh.hostKey")
		}
		hostKeyCallback = ssh.FixedHostKey(key)
	} else {
		hostKeyCallback = ssh.HostKeyCallback(func(_ string, _ net.Addr, key ssh.PublicKey) error {
			// Save host key for validation in next reconcile
			poolWithHostKey := p
			poolWithHostKey.Spec.SSH.HostKey = pointer.Of(key.Marshal())
			return client.Update(ctx, poolWithHostKey)
		})
	}

	config := ssh.ClientConfig{
		User:            p.Spec.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeyCallback,
	}
	logger.Info("connecting via SSH", "address", sshAddress)
	sshConn, channels, requests, err := ssh.NewClientConn(conn, sshAddress, &config)
	if err != nil {
		return err
	}
	defer tryCleanup(currentLine(), &returnedErr, sshConn.Close)
	sshClient := ssh.NewClient(sshConn, channels, requests)
	return do(sshClient)
}

func currentLine() string {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s:%d", file, line)
}

func tryCleanup(message string, storeErr *error, do func() error) {
	err := do()
	if err != nil && storeErr != nil && *storeErr == nil {
		*storeErr = errors.Wrapf(err, "try cleanup: %s", message)
	}
}

func tryNonCriticalCleanup(ctx context.Context, message string, do func() error) {
	logger := log.FromContext(ctx)
	err := do()
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		logger.Info("Non-critical clean up failed:", message, err)
	}
}

func getSecretKey(ctx context.Context, client ctrlclient.Client, currentNamespace string, selector corev1.SecretKeySelector) ([]byte, error) {
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      selector.Name,
			Namespace: currentNamespace,
		},
	}
	if err := client.Get(ctx, ctrlclient.ObjectKeyFromObject(&secret), &secret); err != nil {
		return nil, errors.Wrapf(err, "failed to fetch secret %q from same namespace", secret.Name)
	}
	data, isSet := secret.Data[selector.Key]
	if !isSet {
		return nil, errors.Errorf("secret %q does not contain key %q", selector.Name, selector.Key)
	}
	return data, nil
}

type contextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

func (p Pool) dialSSHConnection(ctx context.Context, client ctrlclient.Client) (net.Conn, error) {
	logger := log.FromContext(ctx)
	sshAddress := p.Spec.SSH.Address

	var ipNet contextDialer = &net.Dialer{}
	if wireGuardSpec := p.Spec.WireGuard; wireGuardSpec == nil {
		logger.Info("Using direct SSH connection")
	} else {
		logger.Info("Using SSH over WireGuard connection")
		peerPublicKey, err := getSecretKey(ctx, client, p.Namespace, wireGuardSpec.PeerPublicKey)
		if err != nil {
			return nil, errors.WithMessage(err, "wireguard peer public key")
		}
		localPrivateKey, err := getSecretKey(ctx, client, p.Namespace, wireGuardSpec.LocalPrivateKey)
		if err != nil {
			return nil, errors.WithMessage(err, "wireguard private key")
		}
		var presharedKey []byte
		if wireGuardSpec.PresharedKey != nil {
			presharedKey, err = getSecretKey(ctx, client, p.Namespace, *wireGuardSpec.PresharedKey)
			if err != nil {
				return nil, errors.WithMessage(err, "wireguard preshared key")
			}
		}

		wireGuardNet, err := wireguard.Start(ctx, wireguard.Config{
			DNSAddresses:    wireGuardSpec.DNSAddresses,
			LocalAddress:    wireGuardSpec.LocalAddress,
			LocalPrivateKey: localPrivateKey,
			LogHandler:      logr.ToSlogHandler(logger),
			PeerAddress:     &wireGuardSpec.PeerAddress,
			PeerPublicKey:   peerPublicKey,
			PresharedKey:    presharedKey,
		})
		if err != nil {
			return nil, err
		}
		logger.Info("Connected to WireGuard peer", "peer", wireGuardSpec.PeerAddress, "local ip", wireGuardSpec.LocalAddress)
		ipNet = wireGuardNet
	}

	conn, err := ipNet.DialContext(ctx, "tcp", sshAddress)
	return conn, errors.WithMessagef(err, "dial SSH server %s", sshAddress)
}

func (p Pool) validDatasetName(name string) bool {
	if name == p.Spec.Name {
		return true
	}
	const datasetSeparator = "/"
	prefix := p.Spec.Name + datasetSeparator
	return name != prefix && strings.HasPrefix(name, prefix)
}
