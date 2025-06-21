// Package ssh stands up and manages the lifecycle of SSH servers, for testing purposes.
package ssh

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"

	"github.com/johnstarich/zfs-sync-operator/internal/pointer"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

const rsaKeyBits = 2048

// TestConfig configures a test SSH server
type TestConfig struct {
	Listener          net.Listener              // Defaults to a TCP listener on an unused port.
	ExecResults       map[string]TestExecResult // Results for specific commands. Prefer this for the controller under test.
	ExecPrefixResults map[string]TestExecResult // Results for categories of commands. Prefer this for controllers outside the test's scope.
}

// TestExecResult describes the behavior of a command executed via SSH
type TestExecResult struct {
	ExpectStdin []byte
	Stdout      []byte
	Stderr      []byte
	ExitCode    int
	WaitContext context.Context
}

// TestServer starts an SSH server and returns the user and private key to use
func TestServer(tb testing.TB, config TestConfig) (user, clientPrivateKey string, serverPublicKey *[]byte, address netip.AddrPort) {
	tb.Helper()
	clientRSAPrivateKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	require.NoError(tb, err)
	clientPrivateKey = string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(clientRSAPrivateKey),
	}))

	serverRSAPrivateKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	require.NoError(tb, err)
	serverPrivateKeyBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(serverRSAPrivateKey),
	})
	serverPrivateKey, err := ssh.ParsePrivateKey(serverPrivateKeyBytes)
	require.NoError(tb, err)
	serverPublicKey = pointer.Of(serverPrivateKey.PublicKey().Marshal())

	if config.Listener == nil {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(tb, err)
		tb.Cleanup(func() {
			mustClose(tb, listener)
		})
		config.Listener = listener
	}
	tcpAddress, isTCPAddress := config.Listener.Addr().(*net.TCPAddr)
	require.True(tb, isTCPAddress, "Listener address must have a port")
	address = tcpAddress.AddrPort()

	user = "some-user"
	go run(tb, config.Listener, user, clientRSAPrivateKey.Public(), serverPrivateKey, config)
	return user, clientPrivateKey, serverPublicKey, address
}

const publicKeyFingerprintExtension = "pubkey-fp"

func run(tb testing.TB, listener net.Listener, clientUser string, clientPublicKey crypto.PublicKey, serverPrivateKey ssh.Signer, config TestConfig) {
	tb.Helper()
	sshClientPublicKey, err := ssh.NewPublicKey(clientPublicKey)
	require.NoError(tb, err)
	sshClientPublicKeyString := string(sshClientPublicKey.Marshal())

	serverConfig := ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			if clientUser == c.User() && sshClientPublicKeyString == string(pubKey.Marshal()) {
				return &ssh.Permissions{
					Extensions: map[string]string{
						publicKeyFingerprintExtension: ssh.FingerprintSHA256(pubKey),
					},
				}, nil
			}
			return nil, errors.Errorf("unknown public key for %q", c.User())
		},
	}
	serverConfig.AddHostKey(serverPrivateKey)

	for {
		netConn, err := listener.Accept()
		if err != nil {
			tb.Log("Error accepting connection:", err)
			return
		}
		go func() {
			defer tryClose(tb, netConn)
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
	if err != nil {
		tb.Log("Error handling connection:", err)
		return
	}
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
				go func() {
					defer func() {
						tryClose(tb, channel)
						wg.Done()
					}()
					exitCode := handleExecRequest(tb, command.Command, channel, channel, channel.Stderr(), config)
					exitStatus := exitStatusRequest{Status: safelyConvertExitCode(exitCode)}
					_, err := channel.SendRequest(exitStatusRequestName, false, ssh.Marshal(exitStatus))
					require.NoError(tb, err)
				}()
				if err := req.Reply(true, nil); err != nil && !errors.Is(err, io.EOF) {
					require.NoError(tb, err)
				}
			}
			wg.Done()
		}(requests)
	}
}

func handleExecRequest(tb testing.TB, command string, stdin io.Reader, stdout, stderr io.Writer, config TestConfig) int {
	result, hasResult := config.ExecResults[command]
	if !hasResult {
		for prefix, candidateResult := range config.ExecPrefixResults {
			hasResult = strings.HasPrefix(command, prefix)
			if hasResult {
				result = candidateResult
				break
			}
		}
	}
	if !hasResult {
		tb.Fatalf("Unexpected exec command: %s", command)
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
	if result.WaitContext != nil {
		<-result.WaitContext.Done()
	}
	return result.ExitCode
}

func safelyConvertExitCode(i int) uint32 {
	const maxExitCode = 256
	if i >= 0 && i < maxExitCode {
		return uint32(i)
	}
	return 1
}

func mustClose(tb testing.TB, closer io.Closer) {
	require.NoError(tb, closer.Close())
}

func tryClose(tb testing.TB, closer io.Closer) {
	tb.Helper()
	err := closer.Close()
	if err != nil {
		tb.Log("Non-critical clean up failed:", err)
	}
}
