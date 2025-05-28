package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	zfspool "github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPool(t *testing.T) {
	t.Parallel()
	run := RunTest(t)
	sshPrivateKey, sshAddr := startSSHServer(t)
	const (
		sshSecretName         = "ssh"
		sshPrivateKeySelector = "private-key"
	)
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: sshSecretName, Namespace: run.Namespace},
		StringData: map[string]string{
			sshPrivateKeySelector: sshPrivateKey,
		},
	}))
	pool := zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: run.Namespace},
		Spec: zfspool.Spec{
			SSH: &zfspool.SSHSpec{
				Address: sshAddr,
				PrivateKey: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: sshSecretName,
					},
					Key: sshPrivateKeySelector,
				},
			},
		},
	}
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &pool))
	pool = zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: run.Namespace},
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&pool), &pool))
		assert.Equal(collect, "Hello, World!\n", pool.Status.State)
	}, 1*time.Second, 10*time.Millisecond)
}

func startSSHServer(t *testing.T) (string, netip.AddrPort) {
	const keyBits = 2048
	privateKey, err := rsa.GenerateKey(rand.Reader, keyBits)
	require.NoError(t, err)
	publicKey, err := ssh.NewPublicKey(privateKey.Public())
	require.NoError(t, err)
	publicKeyString := string(publicKey.Marshal())

	config := ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			if publicKeyString == string(pubKey.Marshal()) {
				return &ssh.Permissions{
					// Record the public key used for authentication.
					Extensions: map[string]string{
						"pubkey-fp": ssh.FingerprintSHA256(pubKey),
					},
				}, nil
			}
			return nil, fmt.Errorf("unknown public key for %q", c.User())
		},
	}

	privateKeyBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	private, err := ssh.ParsePrivateKey(privateKeyBytes)
	require.NoError(t, err)
	config.AddHostKey(private)

	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
	})
	go runSSHServer(t, listener, config)
	return string(privateKeyBytes), listener.Addr().(*net.TCPAddr).AddrPort()
}

func runSSHServer(t *testing.T, listener net.Listener, config ssh.ServerConfig) {
	for {
		nConn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		require.NoError(t, err)
		go func() {
			defer nConn.Close()
			handleSSHConn(t, nConn, &config)
		}()
	}
}

func handleSSHConn(t *testing.T, nConn net.Conn, config *ssh.ServerConfig) {
	// Before use, a handshake must be performed on the incoming
	// net.Conn.
	conn, chans, reqs, err := ssh.NewServerConn(nConn, config)
	require.NoError(t, err)
	t.Logf("logged in with key %s", conn.Permissions.Extensions["pubkey-fp"])

	var wg sync.WaitGroup
	defer wg.Wait()

	// The incoming Request channel must be serviced.
	wg.Add(1)
	go func() {
		ssh.DiscardRequests(reqs)
		wg.Done()
	}()

	// Service the incoming Channel channel.
	for newChannel := range chans {
		// Channels have a type, depending on the application level
		// protocol intended. In the case of a shell, the type is
		// "session" and ServerShell may be used to present a simple
		// terminal interface.
		if newChannel.ChannelType() != "session" {
			t.Log("Rejecting unexpected channel type:", newChannel.ChannelType())
			newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}
		channel, requests, err := newChannel.Accept()
		require.NoError(t, err)

		// Sessions have out-of-band requests such as "shell",
		// "pty-req" and "env".  Here we handle only the
		// "shell" request.
		wg.Add(1)
		go func(in <-chan *ssh.Request) {
			for req := range in {
				if !assert.Equal(t, "exec", req.Type, "Unexpected SSH request type") {
					return
				}
				t.Log("Handling SSH request", string(req.Payload))

				wg.Add(1)
				go func(req *ssh.Request) {
					defer func() {
						channel.Close()
						wg.Done()
					}()
					var command struct {
						Command string
					}
					err := ssh.Unmarshal(req.Payload, &command)
					if err != nil {
						req.Reply(false, nil)
						return
					}
					t.Log("Terminal line read:", command.Command)
					err = handleExecRequest(command.Command, channel, channel, channel.Stderr())
					if err != nil {
						fmt.Fprintln(channel, err)
					}
					var exitStatusRequest struct {
						Status uint32
					}
					var exitErr interface{ ExitCode() int }
					if errors.As(err, &exitErr) {
						exitStatusRequest.Status = uint32(exitErr.ExitCode())
					}
					_, err = channel.SendRequest("exit-status", false, ssh.Marshal(exitStatusRequest))
					assert.NoError(t, err)
				}(req)
				req.Reply(true, nil)
			}
			wg.Done()
		}(requests)
	}
}

func handleExecRequest(command string, stdin io.Reader, stdout, stderr io.Writer) error {
	args := strings.Split(command, " ")
	if len(args) == 0 {
		return errors.Errorf("command must have at least 1 argument, got 0: %q", command)
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}
