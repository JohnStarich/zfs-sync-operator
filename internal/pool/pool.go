// Package pool defines and reconciles the [Pool] custom resource
package pool

import (
	"context"
	"net"
	"net/netip"
	"time"

	"github.com/go-logr/logr"
	"github.com/johnstarich/zfs-sync-operator/internal/baddeepcopy"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/johnstarich/zfs-sync-operator/internal/wireguard"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
func MustAddToScheme(s *runtime.Scheme) {
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

// DeepCopyObject implements [runtime.Object]
func (l *PoolList) DeepCopyObject() runtime.Object { return baddeepcopy.DeepCopy(l) }

// WithSession starts an SSH session (optionally over WireGuard) using p's Spec, runs do with the session, then tears everything down
func (p Pool) WithSession(ctx context.Context, client ctrlclient.Client, do func(*ssh.Session) error) error {
	const maxSessionWait = 1 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, maxSessionWait)
	defer cancel()
	logger := log.FromContext(ctx)

	if p.Spec == nil || p.Spec.SSH == nil {
		return errors.New("ssh is required")
	}

	conn, err := p.dialSSHConnection(ctx, client)
	if err != nil {
		return err
	}
	defer conn.Close()

	sshPrivateKey, err := getSecretKey(ctx, client, p.Namespace, p.Spec.SSH.PrivateKey)
	if err != nil {
		return err
	}
	signer, err := ssh.ParsePrivateKey(sshPrivateKey)
	if err != nil {
		return err
	}
	config := ssh.ClientConfig{
		User:            p.Spec.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // TODO Remember last host key. Put in status?
	}
	logger.Info("connecting via SSH", "address", p.Spec.SSH.Address)
	sshConn, channels, requests, err := ssh.NewClientConn(conn, p.Spec.SSH.Address.String(), &config)
	if err != nil {
		return err
	}
	defer sshConn.Close()
	sshClient := ssh.NewClient(sshConn, channels, requests)
	session, err := sshClient.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	return do(session)
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
		privateKey, err := getSecretKey(ctx, client, p.Namespace, wireGuardSpec.PrivateKey)
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

		localAddr := arbitraryUnusedIP(wireGuardSpec.PeerAddress.Addr())
		wireGuardNet, err := wireguard.Connect(ctx, localAddr, wireguard.Config{
			DNSAddresses:  wireGuardSpec.DNSAddresses,
			LogHandler:    logr.ToSlogHandler(logger),
			PeerAddress:   &wireGuardSpec.PeerAddress,
			PeerPublicKey: peerPublicKey,
			PresharedKey:  presharedKey,
			PrivateKey:    privateKey,
		})
		if err != nil {
			return nil, err
		}
		logger.Info("Connected to WireGuard peer", "peer", wireGuardSpec.PeerAddress, "local ip", localAddr)
		ipNet = wireGuardNet
	}

	conn, err := ipNet.DialContext(ctx, "tcp", sshAddress.String())
	return conn, errors.WithMessagef(err, "dial SSH server %s", sshAddress)
}

func arbitraryUnusedIP(usedIP netip.Addr) netip.Addr {
	arbitraryIP := netip.MustParseAddr("10.3.0.1")
	if usedIP == arbitraryIP {
		return netip.MustParseAddr("10.3.0.2")
	}
	return arbitraryIP
}
