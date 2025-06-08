// Package pool defines and reconciles the [Pool] custom resource
package pool

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"runtime"

	"github.com/go-logr/logr"
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
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

const (
	group      = name.Operator + ".johnstarich.com"
	apiVersion = "v1alpha1"
)

// MustAddToScheme adds the Pool scheme to s
func MustAddToScheme(s *ctrlruntime.Scheme) {
	schemeBuilder := &scheme.Builder{
		GroupVersion: schema.GroupVersion{
			Group:   group,
			Version: apiVersion,
		},
	}
	schemeBuilder.Register(&Pool{}, &PoolList{})
	err := schemeBuilder.AddToScheme(s)
	if err != nil {
		panic(err)
	}
}

func typeMeta() metav1.TypeMeta {
	return metav1.TypeMeta{
		Kind:       "Pool",
		APIVersion: group + "/" + apiVersion,
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
	Name      string         `json:"name"`
	SSH       *SSHSpec       `json:"ssh"`
	WireGuard *WireGuardSpec `json:"wireguard,omitempty"`
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

// Status holds status information for a [Pool]
type Status struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
}

// PoolList is a list of [Pool]. Required to perform a Watch.
//
//nolint:revive // The naming scheme XXXList is required to perform a Watch.
type PoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pool `json:"items"`
}

// DeepCopyObject implements [ctrlruntime.Object]
func (l *PoolList) DeepCopyObject() ctrlruntime.Object { return baddeepcopy.DeepCopy(l) }

// WithSession starts an SSH session (optionally over WireGuard) using p's Spec, runs do with the session, then tears everything down
func (p Pool) WithSession(ctx context.Context, client ctrlclient.Client, do func(*ssh.Session) error) (resourceVersion string, returnedErr error) {
	logger := log.FromContext(ctx)
	resourceVersion = p.ResourceVersion

	if p.Spec == nil || p.Spec.SSH == nil {
		return resourceVersion, errors.New("ssh is required")
	}

	conn, err := p.dialSSHConnection(ctx, client)
	if err != nil {
		return resourceVersion, err
	}
	defer tryNonCriticalCleanup(ctx, currentLine(), conn.Close)
	logger.Info("SSH TCP connection established")

	sshPrivateKey, err := getSecretKey(ctx, client, p.Namespace, p.Spec.SSH.PrivateKey)
	if err != nil {
		return resourceVersion, err
	}
	signer, err := ssh.ParsePrivateKey(sshPrivateKey)
	if err != nil {
		return resourceVersion, err
	}

	sshAddress := p.Spec.SSH.Address

	var hostKeyCallback ssh.HostKeyCallback
	if p.Spec.SSH.HostKey != nil {
		key, err := ssh.ParsePublicKey(*p.Spec.SSH.HostKey)
		if err != nil {
			return resourceVersion, errors.WithMessage(err, "parse spec.ssh.hostKey")
		}
		hostKeyCallback = ssh.FixedHostKey(key)
	} else {
		hostKeyCallback = ssh.HostKeyCallback(func(_ string, _ net.Addr, key ssh.PublicKey) error {
			// Save host key for validation in next reconcile
			poolWithHostKey := p
			poolWithHostKey.Spec.SSH.HostKey = toPointer(key.Marshal())
			err := client.Update(ctx, &poolWithHostKey)
			resourceVersion = poolWithHostKey.ResourceVersion
			return err
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
		return resourceVersion, err
	}
	defer tryCleanup(currentLine(), &returnedErr, sshConn.Close)
	sshClient := ssh.NewClient(sshConn, channels, requests)
	session, err := sshClient.NewSession()
	if err != nil {
		return resourceVersion, err
	}
	defer tryNonCriticalCleanup(ctx, currentLine(), session.Close)
	return resourceVersion, do(session)
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
	if err != nil {
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
