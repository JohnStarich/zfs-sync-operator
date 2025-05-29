package pool

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"

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
	command := safelyFormatCommand("/usr/sbin/zpool", "status", pool.Spec.Name)
	zpoolStatus, err := session.CombinedOutput(command)
	if err != nil {
		zpoolStatusStr := strings.TrimSpace(string(zpoolStatus))
		logger.Error(err, "command failed", "output", zpoolStatusStr)
		pool.Status.State = "Error"
		pool.Status.Reason = fmt.Sprintf(`failed to run '%s': %s`, command, zpoolStatusStr)
		return reconcile.Result{}, r.client.Update(ctx, &pool)
	}
	stateField := stateFieldFromZpoolStatus(zpoolStatus)
	logger.Info("pool connection was successful", "state", stateField)
	pool.Status.State = stateFromStateField(stateField)

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

func safelyFormatCommand(name string, args ...string) string {
	// TODO avoid shell evaluation, like $
	var b strings.Builder
	b.WriteString(strconv.Quote(name))
	for _, arg := range args {
		b.WriteRune(' ')
		b.WriteString(strconv.Quote(arg))
	}
	return b.String()
}

func stateFieldFromZpoolStatus(status []byte) string {
	status = bytes.TrimSpace(status) // remove leading blank lines, if any
	scanner := bufio.NewScanner(bytes.NewReader(status))
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Fields(line)
		if len(tokens) != 2 { // stop at break point between fields and vdev list
			break
		}
		field, value := tokens[0], tokens[1]
		if field == "state:" {
			return value
		}
	}
	return ""
}

func stateFromStateField(state string) string {
	switch state {
	case "ONLINE":
		return "Online"
	default:
		// TODO handle all known zpool states
		return "Unknown"
	}
}
