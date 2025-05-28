package pool

import (
	"bytes"
	"context"
	"net"
	"net/netip"

	"github.com/go-logr/logr"
	"github.com/johnstarich/zfs-sync-operator/internal/wireguard"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Reconciler reconciles Pool resources to validate their Pools and associated connections
type Reconciler struct {
	client client.Client
}

// NewReconciler returns a new pool reconciler
func NewReconciler(client client.Client) *Reconciler {
	return &Reconciler{client: client}
}

// Reconcile implements [reconcile.Reconciler]
func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking request", "request", request)
	var pool Pool
	if err := r.client.Get(ctx, request.NamespacedName, &pool); err != nil {
		return reconcile.Result{}, err
	}
	logger.Info("got pool", "pool", pool)

	if pool.Spec.SSH == nil {
		return reconcile.Result{}, errors.New("ssh is required")
	}

	var conn net.Conn
	if wireGuardSpec := pool.Spec.WireGuard; wireGuardSpec != nil {
		// TODO report status on error
		peerPublicKey, err := r.getSecretKey(ctx, request.Namespace, wireGuardSpec.PeerPublicKey)
		if err != nil {
			return reconcile.Result{}, errors.WithMessage(err, "wireguard peer public key")
		}
		privateKey, err := r.getSecretKey(ctx, request.Namespace, wireGuardSpec.PrivateKey)
		if err != nil {
			return reconcile.Result{}, errors.WithMessage(err, "wireguard private key")
		}
		var presharedKey []byte
		if wireGuardSpec.PresharedKey != nil {
			presharedKey, err = r.getSecretKey(ctx, request.Namespace, *wireGuardSpec.PresharedKey)
			if err != nil {
				return reconcile.Result{}, errors.WithMessage(err, "wireguard preshared key")
			}
		}

		localAddr := arbitraryUnusedIP(wireGuardSpec.PeerAddress.Addr())
		net, err := wireguard.Connect(ctx, localAddr, wireguard.Config{
			DNSAddresses:  wireGuardSpec.DNSAddresses,
			LogHandler:    logr.ToSlogHandler(logger),
			PeerAddress:   &wireGuardSpec.PeerAddress,
			PeerPublicKey: peerPublicKey,
			PresharedKey:  presharedKey,
			PrivateKey:    privateKey,
		})
		if err != nil {
			return reconcile.Result{}, err
		}
		conn, err = net.DialContextTCPAddrPort(ctx, pool.Spec.SSH.Address)
		if err != nil {
			return reconcile.Result{}, err
		}
	} else {
		var err error
		conn, err = net.Dial("tcp", pool.Spec.SSH.Address.String())
		if err != nil {
			return reconcile.Result{}, err
		}
	}
	defer conn.Close()

	sshPrivateKey, err := r.getSecretKey(ctx, request.Namespace, pool.Spec.SSH.PrivateKey)
	if err != nil {
		return reconcile.Result{}, err
	}
	signer, err := ssh.ParsePrivateKey(sshPrivateKey)
	if err != nil {
		return reconcile.Result{}, err
	}
	// An SSH client is represented with a ClientConn.
	//
	// To authenticate with the remote server you must pass at least one
	// implementation of AuthMethod via the Auth field in ClientConfig,
	// and provide a HostKeyCallback.
	config := ssh.ClientConfig{
		User:            pool.Spec.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // TODO Remember last host key. Put in status?
	}
	sshConn, channels, requests, err := ssh.NewClientConn(conn, pool.Spec.SSH.Address.String(), &config)
	if err != nil {
		return reconcile.Result{}, err
	}
	defer sshConn.Close()
	sshClient := ssh.NewClient(sshConn, channels, requests)
	session, err := sshClient.NewSession()
	if err != nil {
		return reconcile.Result{}, err
	}
	defer session.Close()
	var commandOutput bytes.Buffer
	session.Stdout = &commandOutput
	if err := session.Run("/usr/bin/echo Hello, World!"); err != nil {
		return reconcile.Result{}, errors.WithMessagef(err, "command failed with output '%s'", commandOutput.String())
	}

	pool.Status.State = commandOutput.String()
	if err := r.client.Update(ctx, &pool); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func arbitraryUnusedIP(usedIP netip.Addr) netip.Addr {
	arbitraryIP := netip.MustParseAddr("10.3.0.1")
	if usedIP == arbitraryIP {
		return netip.MustParseAddr("10.3.0.2")
	}
	return arbitraryIP
}

func (r *Reconciler) getSecretKey(ctx context.Context, currentNamespace string, selector corev1.SecretKeySelector) ([]byte, error) {
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      selector.Name,
			Namespace: currentNamespace,
		},
	}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(&secret), &secret); err != nil {
		return nil, errors.Wrapf(err, "failed to fetch secret %q from same namespace", secret.Name)
	}
	data, isSet := secret.Data[selector.Key]
	if !isSet {
		return nil, errors.Errorf("secret %q does not contain key %q", selector.Name, selector.Key)
	}
	return data, nil
}
