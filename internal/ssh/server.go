package ssh

import (
	"crypto"
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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

const rsaKeyBits = 2048

func TestServer(tb testing.TB) (user, privateKey string, address netip.AddrPort) {
	tb.Helper()
	rsaPrivateKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	require.NoError(tb, err)
	privateKey = string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaPrivateKey),
	}))

	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, listener.Close())
	})
	user = "some-user"
	go run(tb, listener, user, rsaPrivateKey.Public())
	return user, privateKey, listener.Addr().(*net.TCPAddr).AddrPort()
}

const publicKeyFingerprintExtension = "pubkey-fp"

func run(tb testing.TB, listener net.Listener, clientUser string, clientPublicKey crypto.PublicKey) {
	tb.Helper()
	publicKey, err := ssh.NewPublicKey(clientPublicKey)
	require.NoError(tb, err)
	publicKeyString := string(publicKey.Marshal())

	serverConfig := ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			if clientUser == c.User() && publicKeyString == string(pubKey.Marshal()) {
				return &ssh.Permissions{
					Extensions: map[string]string{
						publicKeyFingerprintExtension: ssh.FingerprintSHA256(pubKey),
					},
				}, nil
			}
			return nil, errors.Errorf("unknown public key for %q", c.User())
		},
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	require.NoError(tb, err)
	privateKeyBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	sshPrivateKey, err := ssh.ParsePrivateKey(privateKeyBytes)
	require.NoError(tb, err)
	serverConfig.AddHostKey(sshPrivateKey)
	for {
		netConn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		require.NoError(tb, err)
		go func() {
			defer netConn.Close()
			handleConn(tb, netConn, &serverConfig)
		}()
	}
}

const execRequestName = "exec"

type execRequest struct {
	Command string
}

const exitStatusRequestName = "exit-status"

type exitStatusRequest struct {
	Status uint32
}

type exitErrorCoder interface {
	ExitCode() int
}

func handleConn(tb testing.TB, netConn net.Conn, serverConfig *ssh.ServerConfig) {
	conn, chans, reqs, err := ssh.NewServerConn(netConn, serverConfig)
	require.NoError(tb, err)
	tb.Logf("Logged in with key fingerprint: %s", conn.Permissions.Extensions[publicKeyFingerprintExtension])

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		ssh.DiscardRequests(reqs) // We don't need to service these for our tests, so receive and discard.
		wg.Done()
	}()

	for newChannel := range chans {
		require.Equal(tb, "session", newChannel.ChannelType())
		channel, requests, err := newChannel.Accept()
		require.NoError(tb, err)

		wg.Add(1)
		go func(in <-chan *ssh.Request) {
			for req := range in {
				require.Equal(tb, execRequestName, req.Type, "Unexpected SSH request type")
				var command execRequest
				require.NoError(tb, ssh.Unmarshal(req.Payload, &command))
				tb.Logf("Handling SSH exec request: %q", command.Command)

				wg.Add(1)
				go func(req *ssh.Request) {
					defer func() {
						channel.Close()
						wg.Done()
					}()
					execErr := handleExecRequest(command.Command, channel, channel, channel.Stderr())
					if execErr != nil {
						fmt.Fprintln(channel.Stderr(), execErr)
					}
					var exitStatus exitStatusRequest
					if exitErr := exitErrorCoder(nil); errors.As(execErr, &exitErr) {
						exitStatus.Status = uint32(exitErr.ExitCode())
					}
					_, err := channel.SendRequest(exitStatusRequestName, false, ssh.Marshal(exitStatus))
					require.NoError(tb, err)
				}(req)
				require.NoError(tb, req.Reply(true, nil))
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
