package pool

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"net/netip"
	"strings"
	"time"
	"unicode"

	"github.com/go-logr/logr"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
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
	logger.Info("got pool", "status", pool.Status, "pool", pool.Spec)
	if pool.Status != nil && pool.Status.State == "Online" { // TODO verify nothing has changed
		// TODO can we ignore status field changes? only ack spec or secret updates?
		return reconcile.Result{}, nil
	}

	state, reconcileErr := r.reconcile(ctx, pool)

	poolStatusPatch := Pool{
		TypeMeta:   typeMeta(),
		ObjectMeta: metav1.ObjectMeta{Name: request.Name, Namespace: request.Namespace},
	}
	if reconcileErr == nil {
		logger.Info("pool detected successfully", "state", state)
		poolStatusPatch.Status = &Status{
			State: stateFromStateField(state),
		}
	} else {
		logger.Error(reconcileErr, "reconcile failed")
		poolStatusPatch.Status = &Status{
			State:  "Error",
			Reason: reconcileErr.Error(),
		}
	}
	statusErr := r.client.Patch(ctx, &poolStatusPatch, client.Apply, &client.PatchOptions{
		FieldManager: name.Operator,
	})
	return reconcile.Result{}, errors.Wrap(statusErr, "failed to update status")
}

func (r *Reconciler) reconcile(ctx context.Context, pool Pool) (state string, returnedErr error) {
	defer func() { returnedErr = errors.WithStack(returnedErr) }()
	const maxReconcileWait = 1 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, maxReconcileWait)
	defer cancel()
	logger := log.FromContext(ctx)

	if pool.Spec == nil || pool.Spec.SSH == nil {
		return "", errors.New("ssh is required")
	}

	conn, err := r.dialSSHConnection(ctx, pool)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	sshPrivateKey, err := r.getSecretKey(ctx, pool.Namespace, pool.Spec.SSH.PrivateKey)
	if err != nil {
		return "", err
	}
	signer, err := ssh.ParsePrivateKey(sshPrivateKey)
	if err != nil {
		return "", err
	}
	config := ssh.ClientConfig{
		User:            pool.Spec.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // TODO Remember last host key. Put in status?
	}
	sshAddress := pool.Spec.SSH.Address
	logger.Info("connecting via SSH", "address", sshAddress)
	sshConn, channels, requests, err := ssh.NewClientConn(conn, sshAddress.String(), &config)
	if err != nil {
		return "", err
	}
	defer sshConn.Close()
	sshClient := ssh.NewClient(sshConn, channels, requests)
	session, err := sshClient.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()
	command := safelyFormatCommand("/usr/sbin/zpool", "status", pool.Spec.Name)
	zpoolStatus, err := session.CombinedOutput(command)
	if err != nil {
		zpoolStatusStr := strings.TrimSpace(string(zpoolStatus))
		return "", errors.Errorf(`failed to run '%s': %s`, command, zpoolStatusStr)
	}
	return stateFieldFromZpoolStatus(zpoolStatus), nil
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
	var b strings.Builder
	b.WriteString(shellQuote(name))
	for _, arg := range args {
		b.WriteRune(' ')
		b.WriteString(shellQuote(arg))
	}
	return b.String()
}

func shellQuote(str string) string {
	var builder strings.Builder
	for _, r := range str {
		if shouldQuote(r) {
			const escapeBackslash = '\\'
			builder.WriteRune(escapeBackslash)
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

// shouldQuote conservatively quotes most runes, save some basic readability of ASCII compatible runes
func shouldQuote(r rune) bool {
	if r > unicode.MaxASCII {
		return true
	}
	return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '/'
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

type contextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

func (r *Reconciler) dialSSHConnection(ctx context.Context, pool Pool) (net.Conn, error) {
	logger := log.FromContext(ctx)
	sshAddress := pool.Spec.SSH.Address

	var ipNet contextDialer = &net.Dialer{}
	if wireGuardSpec := pool.Spec.WireGuard; wireGuardSpec == nil {
		logger.Info("Using direct SSH connection")
	} else {
		logger.Info("Using SSH over WireGuard connection")
		peerPublicKey, err := r.getSecretKey(ctx, pool.Namespace, wireGuardSpec.PeerPublicKey)
		if err != nil {
			return nil, errors.WithMessage(err, "wireguard peer public key")
		}
		privateKey, err := r.getSecretKey(ctx, pool.Namespace, wireGuardSpec.PrivateKey)
		if err != nil {
			return nil, errors.WithMessage(err, "wireguard private key")
		}
		var presharedKey []byte
		if wireGuardSpec.PresharedKey != nil {
			presharedKey, err = r.getSecretKey(ctx, pool.Namespace, *wireGuardSpec.PresharedKey)
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

	const dialRetries = 10
	dialTimeout := 500 * time.Millisecond
	return retryWithTimeout(ctx, dialTimeout, dialRetries, func(ctx context.Context) (net.Conn, error) {
		conn, err := ipNet.DialContext(ctx, "tcp", sshAddress.String())
		return conn, errors.WithMessagef(err, "dial SSH server %s", sshAddress)
	})
}

func retryWithTimeout[Value any](ctx context.Context, timeout time.Duration, retries uint, do func(context.Context) (Value, error)) (value Value, doErr error) {
	logger := log.FromContext(ctx)
	if retries == 0 {
		retries = 1
	}
	for attempt := range retries {
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		value, doErr = do(timeoutCtx)
		if doErr == nil {
			return value, nil
		}
		logger.Error(doErr, "failed retry attempt", "waited", timeout, "attempt", attempt)
		select {
		case <-ctx.Done():
			return value, doErr
		default:
		}
	}
	return value, doErr
}
