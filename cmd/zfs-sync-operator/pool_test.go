package main

import (
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	zfspool "github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/johnstarich/zfs-sync-operator/internal/ssh"
	"github.com/johnstarich/zfs-sync-operator/internal/wireguard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	maxWaitForPool = 10 * time.Second
	tickForPool    = max(10*time.Millisecond, maxWaitForPool/10)
)

func TestPoolWithOnlySSH(t *testing.T) {
	t.Parallel()
	const somePoolName = "somepool"
	for _, tc := range []struct {
		description  string
		execResults  map[string]ssh.TestExecResult
		expectStatus *zfspool.Status
	}{
		{
			description: "happy path",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout: fmt.Appendf(nil, `
  pool: %[1]s
 state: ONLINE
config:

        NAME                        STATE     READ WRITE CKSUM
        %[1]s                       ONLINE       0     0     0
          raidz2-0                  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0

 errors: No known data errors
`, somePoolName),
					ExitCode: 0,
				},
			},
			expectStatus: &zfspool.Status{
				State:  "Online",
				Reason: "",
			},
		},
		{
			description: "pool not found",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout:   fmt.Appendf(nil, "cannot open '%[1]s': no such pool\n", somePoolName),
					ExitCode: 1,
				},
			},
			expectStatus: &zfspool.Status{
				State:  "NotFound",
				Reason: fmt.Sprintf(`cannot open '%[1]s': no such pool`, somePoolName),
			},
		},
		{
			description: "unexpected command error",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout:   []byte("nope!\n"),
					ExitCode: 1,
				},
			},
			expectStatus: &zfspool.Status{
				State:  "Error",
				Reason: fmt.Sprintf(`failed to run '/usr/sbin/zpool status %[1]s': nope!: Process exited with status 1`, somePoolName),
			},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			run := RunTest(t)
			sshUser, sshPrivateKey, sshAddr := ssh.TestServer(t, ssh.TestConfig{ExecResults: tc.execResults})
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
			require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &zfspool.Pool{
				ObjectMeta: metav1.ObjectMeta{Name: somePoolName, Namespace: run.Namespace},
				Spec: &zfspool.Spec{
					Name: somePoolName,
					SSH: &zfspool.SSHSpec{
						User:    sshUser,
						Address: sshAddr,
						PrivateKey: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: sshSecretName,
							},
							Key: sshPrivateKeySelector,
						},
					},
				},
			}))
			require.EventuallyWithTf(t, func(collect *assert.CollectT) {
				pool := zfspool.Pool{
					ObjectMeta: metav1.ObjectMeta{Name: somePoolName, Namespace: run.Namespace},
				}
				assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&pool), &pool))
				assert.Equal(collect, tc.expectStatus, pool.Status)
			}, maxWaitForPool, tickForPool, "namespace = %s", run.Namespace)
		})
	}
}

func TestPoolWithSSHOverWireGuard(t *testing.T) {
	t.Parallel()
	const somePoolName = "somepool"
	const (
		wireguardSecretKeyPeerPublicKey = "peer-public-key"
		wireguardSecretKeyPresharedKey  = "preshared-key"
		wireguardSecretKeyPrivateKey    = "private-key"
	)
	for _, tc := range []struct {
		description           string
		execResults           map[string]ssh.TestExecResult
		mutateWireGuardSecret func(validSecretData map[string][]byte)
		expectStatus          *zfspool.Status
	}{
		{
			description: "happy path",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout: fmt.Appendf(nil, `
  pool: %[1]s
 state: ONLINE
config:

        NAME                        STATE     READ WRITE CKSUM
        %[1]s                       ONLINE       0     0     0
          raidz2-0                  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0

 errors: No known data errors
`, somePoolName),
					ExitCode: 0,
				},
			},
			expectStatus: &zfspool.Status{
				State:  "Online",
				Reason: "",
			},
		},
		{
			description: "pool not found",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout:   fmt.Appendf(nil, "cannot open '%[1]s': no such pool\n", somePoolName),
					ExitCode: 1,
				},
			},
			expectStatus: &zfspool.Status{
				State:  "NotFound",
				Reason: fmt.Sprintf(`cannot open '%[1]s': no such pool`, somePoolName),
			},
		},
		{
			description: "invalid wireguard configuration",
			mutateWireGuardSecret: func(validSecretData map[string][]byte) {
				bustedPrivateKey := validSecretData[wireguardSecretKeyPrivateKey]
				bustedPrivateKey[2] = 'f'
				bustedPrivateKey[3] = 'o'
				bustedPrivateKey[4] = 'o'
				validSecretData[wireguardSecretKeyPrivateKey] = bustedPrivateKey
			},
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout: fmt.Appendf(nil, `
  pool: %[1]s
 state: ONLINE
config:

        NAME                        STATE     READ WRITE CKSUM
        %[1]s                       ONLINE       0     0     0
          raidz2-0                  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0
            wwn-0x0000000000000000  ONLINE       0     0     0

 errors: No known data errors
`, somePoolName),
					ExitCode: 0,
				},
			},
			expectStatus: &zfspool.Status{
				State:  "Error",
				Reason: `dial SSH server %s: context deadline exceeded`,
			},
		},
		{
			description: "unexpected command error",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout:   []byte("nope!\n"),
					ExitCode: 1,
				},
			},
			expectStatus: &zfspool.Status{
				State:  "Error",
				Reason: fmt.Sprintf(`failed to run '/usr/sbin/zpool status %[1]s': nope!: Process exited with status 1`, somePoolName),
			},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			run := RunTest(t)
			servers := startSSHOverWireGuard(t, tc.execResults)
			const (
				sshSecretName         = "ssh"
				sshPrivateKeySelector = "private-key"
			)
			require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: sshSecretName, Namespace: run.Namespace},
				StringData: map[string]string{
					sshPrivateKeySelector: servers.SSH.PrivateKey,
				},
			}))

			wireguardSecretData := map[string][]byte{
				wireguardSecretKeyPeerPublicKey: servers.WireGuard.ServerPublicKey,
				wireguardSecretKeyPresharedKey:  servers.WireGuard.PresharedKey,
				wireguardSecretKeyPrivateKey:    servers.WireGuard.ClientPrivateKey,
			}
			if tc.mutateWireGuardSecret != nil {
				tc.mutateWireGuardSecret(wireguardSecretData)
			}
			const wireguardSecretName = "wireguard"
			require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: wireguardSecretName, Namespace: run.Namespace},
				Data:       wireguardSecretData,
			}))
			require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &zfspool.Pool{
				ObjectMeta: metav1.ObjectMeta{Name: somePoolName, Namespace: run.Namespace},
				Spec: &zfspool.Spec{
					Name: somePoolName,
					SSH: &zfspool.SSHSpec{
						User:    servers.SSH.User,
						Address: servers.SSH.Address,
						PrivateKey: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: sshSecretName,
							},
							Key: sshPrivateKeySelector,
						},
					},
					WireGuard: &zfspool.WireGuardSpec{
						LocalAddress: servers.WireGuard.LocalAddress,
						PeerAddress:  servers.WireGuard.PeerAddress,
						PeerPublicKey: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: wireguardSecretName},
							Key:                  wireguardSecretKeyPeerPublicKey,
						},
						PresharedKey: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: wireguardSecretName},
							Key:                  wireguardSecretKeyPresharedKey,
						},
						PrivateKey: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: wireguardSecretName},
							Key:                  wireguardSecretKeyPrivateKey,
						},
					},
				},
			}))
			if strings.ContainsRune(tc.expectStatus.Reason, '%') {
				tc.expectStatus.Reason = fmt.Sprintf(tc.expectStatus.Reason, servers.SSH.Address.String())
			}
			require.EventuallyWithTf(t, func(collect *assert.CollectT) {
				pool := zfspool.Pool{
					ObjectMeta: metav1.ObjectMeta{Name: somePoolName, Namespace: run.Namespace},
				}
				assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&pool), &pool))
				assert.Equal(collect, tc.expectStatus, pool.Status)
			}, maxWaitForPool, tickForPool, "namespace = %s", run.Namespace)
		})
	}
}

type TestSSHOverWireGuard struct {
	SSH       TestSSH
	WireGuard TestWireGuard
}

type TestSSH struct {
	Address    netip.AddrPort
	PrivateKey string
	User       string
}

type TestWireGuard struct {
	ClientPrivateKey []byte
	LocalAddress     netip.Addr
	PeerAddress      netip.AddrPort
	PresharedKey     []byte
	ServerPublicKey  []byte // TODO ban wireguard "server" and "client". find a different, more idiomatic way to describe these
}

func startSSHOverWireGuard(tb testing.TB, execResults map[string]ssh.TestExecResult) TestSSHOverWireGuard {
	presharedKey, err := wgtypes.GenerateKey()
	require.NoError(tb, err)

	serverPrivateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(tb, err)
	serverPublicKey := serverPrivateKey.PublicKey()

	clientPrivateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(tb, err)
	clientPublicKey := clientPrivateKey.PublicKey()

	remoteWireGuardInterfaceAddr := netip.MustParseAddr("10.3.0.2")
	localWireGuardInterfaceAddr := netip.MustParseAddr("10.3.0.3")
	wireguardNet, wireguardAddr := wireguard.StartTest(tb, remoteWireGuardInterfaceAddr, presharedKey, serverPrivateKey, clientPublicKey)
	sshListener, err := wireguardNet.ListenTCPAddrPort(netip.AddrPortFrom(remoteWireGuardInterfaceAddr, 0))
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, sshListener.Close())
	})

	sshUser, sshPrivateKey, sshAddr := ssh.TestServer(tb, ssh.TestConfig{
		Listener:    sshListener,
		ExecResults: execResults,
	})

	return TestSSHOverWireGuard{
		SSH: TestSSH{
			Address:    sshAddr,
			PrivateKey: sshPrivateKey,
			User:       sshUser,
		},
		WireGuard: TestWireGuard{
			ClientPrivateKey: clientPrivateKey[:],
			LocalAddress:     localWireGuardInterfaceAddr,
			PeerAddress:      wireguardAddr,
			PresharedKey:     presharedKey[:],
			ServerPublicKey:  serverPublicKey[:],
		},
	}
}
