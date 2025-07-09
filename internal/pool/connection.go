package pool

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"

	"github.com/go-logr/logr"
	"github.com/johnstarich/zfs-sync-operator/internal/pointer"
	"github.com/johnstarich/zfs-sync-operator/internal/wireguard"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Connection executes commands over SSH on a ZFS Pool.
// If WireGuard is required, the SSH connection is tunneled through the VPN.
type Connection struct {
	client *ssh.Client
}

// WithConnection starts an SSH session (optionally over WireGuard) using p's Spec, runs do with the session, then tears everything down
func (p *Pool) WithConnection(ctx context.Context, client ctrlclient.Client, do func(*Connection) error) (returnedErr error) {
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
	return do(&Connection{
		client: sshClient,
	})
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

func (p *Pool) dialSSHConnection(ctx context.Context, client ctrlclient.Client) (net.Conn, error) {
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

// ExecCombinedOutput executes the named command with its arguments, captures the output, and returns the results
func (c *Connection) ExecCombinedOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	session, err := c.client.NewSession()
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to start session for exec (%s %s)", name, strings.Join(args, " "))
	}
	defer tryNonCriticalCleanup(ctx, currentLine(), session.Close)
	output, err := session.CombinedOutput(safelyFormatCommand(name, args...))
	output = bytes.TrimSpace(output)
	return output, wrapExecError(name, args, output, err)
}

// ExecWriteStdout is like [ExecCombinedOutput] but writes stdout to 'out' and closes when execution completes
func (c *Connection) ExecWriteStdout(ctx context.Context, out io.WriteCloser, name string, args ...string) error {
	session, err := c.client.NewSession()
	if err != nil {
		return errors.WithMessagef(err, "failed to start session for exec (%s %s)", name, strings.Join(args, " "))
	}
	defer tryNonCriticalCleanup(ctx, currentLine(), session.Close)
	defer tryNonCriticalCleanup(ctx, currentLine(), out.Close)
	session.Stdout = out
	var stderr bytes.Buffer
	session.Stderr = &stderr
	err = session.Run(safelyFormatCommand(name, args...))
	return wrapExecError(name, args, stderr.Bytes(), err)
}

// ExecReadStdin is like [ExecCombinedOutput] but reads stdin from 'in'
func (c *Connection) ExecReadStdin(ctx context.Context, in io.Reader, name string, args ...string) error {
	session, err := c.client.NewSession()
	if err != nil {
		return errors.WithMessagef(err, "failed to start session for exec (%s %s)", name, strings.Join(args, " "))
	}
	defer tryNonCriticalCleanup(ctx, currentLine(), session.Close)
	session.Stdin = in
	output, err := session.CombinedOutput(safelyFormatCommand(name, args...))
	return wrapExecError(name, args, output, err)
}

func wrapExecError(name string, args []string, output []byte, sshExecError error) error {
	if sshExecError == nil {
		return nil
	}
	return &ExecError{
		Args:   append([]string{name}, args...),
		Output: output,
		Err:    sshExecError,
	}
}

// ExecError is returned from executing a command with [Connection].
// Contains the command metadata and the command's relevant output. If stdout is streamed, then output is stderr.
type ExecError struct {
	Args   []string
	Output []byte
	Err    error
}

func (e *ExecError) Error() string {
	if len(e.Output) > 0 {
		return fmt.Sprintf("command '%s' failed with output: %s: %s", strings.Join(e.Args, " "), string(e.Output), e.Err)
	}
	return fmt.Sprintf("command '%s' failed: %s", strings.Join(e.Args, " "), e.Err)
}

func (e *ExecError) Unwrap() error {
	return e.Err
}
