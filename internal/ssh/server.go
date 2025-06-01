package ssh

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

const rsaKeyBits = 2048

type TestConfig struct {
	Listener    net.Listener // Defaults to a TCP listener on an unused port.
	ExecResults map[string]TestExecResult
}

type TestExecResult struct {
	ExpectStdin []byte
	Stdout      []byte
	Stderr      []byte
	ExitCode    int
}

// TestServer starts an SSH server and returns the user and private key to use
func TestServer(tb testing.TB, config TestConfig) (user, privateKey string, address netip.AddrPort) {
	tb.Helper()
	rsaPrivateKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	require.NoError(tb, err)
	privateKey = string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaPrivateKey),
	}))

	if config.Listener == nil {
		listener, err := net.Listen("tcp", "0.0.0.0:0")
		require.NoError(tb, err)
		tb.Cleanup(func() {
			require.NoError(tb, listener.Close())
		})
		config.Listener = listener
	}
	tcpAddress, isTCPAddress := config.Listener.Addr().(*net.TCPAddr)
	require.True(tb, isTCPAddress, "Listener address must have a port")
	address = tcpAddress.AddrPort()
	user = "some-user"
	go run(tb, config.Listener, user, rsaPrivateKey.Public(), config)
	return user, privateKey, address
}

const publicKeyFingerprintExtension = "pubkey-fp"

func run(tb testing.TB, listener net.Listener, clientUser string, clientPublicKey crypto.PublicKey, config TestConfig) {
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
			handleConn(tb, netConn, &serverConfig, config)
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

func handleConn(tb testing.TB, netConn net.Conn, serverConfig *ssh.ServerConfig, config TestConfig) {
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
					exitCode := handleExecRequest(tb, command.Command, channel, channel, channel.Stderr(), config)
					exitStatus := exitStatusRequest{Status: uint32(exitCode)}
					_, err := channel.SendRequest(exitStatusRequestName, false, ssh.Marshal(exitStatus))
					require.NoError(tb, err)
				}(req)
				require.NoError(tb, req.Reply(true, nil))
			}
			wg.Done()
		}(requests)
	}
}

func handleExecRequest(tb testing.TB, command string, stdin io.Reader, stdout, stderr io.Writer, config TestConfig) int {
	result, hasResult := config.ExecResults[command]
	if !hasResult {
		tb.Fatal("Unexpected exec command:", command)
	}
	if len(result.ExpectStdin) > 0 {
		stdinBytes, err := io.ReadAll(stdin)
		require.NoError(tb, err)
		assert.EqualValues(tb, result.ExpectStdin, stdinBytes)
	}
	_, err := stdout.Write(result.Stdout)
	require.NoError(tb, err)
	_, err = stderr.Write(result.Stderr)
	require.NoError(tb, err)
	return result.ExitCode
}
