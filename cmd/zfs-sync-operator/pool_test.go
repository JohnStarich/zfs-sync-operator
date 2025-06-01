package main

import (
	"fmt"
	"net/netip"
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

func TestPoolWithOnlySSH(t *testing.T) {
	t.Parallel()
	run := RunTest(t)
	const (
		foundPool    = "mypool"
		notFoundPool = "mynotfoundpool"
	)
	sshUser, sshPrivateKey, sshAddr := ssh.TestServer(t, ssh.TestConfig{
		ExecResults: map[string]ssh.TestExecResult{
			fmt.Sprintf(`/usr/sbin/zpool status %s`, foundPool): {
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
`, foundPool),
				ExitCode: 0,
			},
			fmt.Sprintf(`/usr/sbin/zpool status %s`, notFoundPool): {
				Stdout:   fmt.Appendf(nil, "cannot open '%[1]s': no such pool\n", notFoundPool),
				ExitCode: 1,
			},
		},
	})
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
		ObjectMeta: metav1.ObjectMeta{Name: foundPool, Namespace: run.Namespace},
		Spec: zfspool.Spec{
			Name: foundPool,
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
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pool := zfspool.Pool{
			ObjectMeta: metav1.ObjectMeta{Name: foundPool, Namespace: run.Namespace},
		}
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&pool), &pool))
		assert.Equal(collect, "Online", pool.Status.State)
		assert.Equal(collect, "", pool.Status.Reason)
	}, 1*time.Second, 10*time.Millisecond)

	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: notFoundPool, Namespace: run.Namespace},
		Spec: zfspool.Spec{
			Name: notFoundPool,
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
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pool := zfspool.Pool{
			ObjectMeta: metav1.ObjectMeta{Name: notFoundPool, Namespace: run.Namespace},
		}
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&pool), &pool))
		assert.Equal(collect, "Error", pool.Status.State)
		assert.Equal(collect, fmt.Sprintf(`failed to run '/usr/sbin/zpool status %[1]s': cannot open '%[1]s': no such pool`, notFoundPool), pool.Status.Reason)
	}, 1*time.Second, 10*time.Millisecond)
}

func TestPoolWithSSHOverWireGuard(t *testing.T) {
	t.Parallel()
	run := RunTest(t)
	presharedKey, err := wgtypes.GenerateKey()
	require.NoError(t, err)

	serverPrivateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	serverPublicKey := serverPrivateKey.PublicKey()

	clientPrivateKey, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)
	clientPublicKey := clientPrivateKey.PublicKey()

	remoteWireGuardInternalAddr := netip.MustParseAddr("10.3.0.2")
	wireguardNet, wireguardAddr := wireguard.StartTestServer(t, remoteWireGuardInternalAddr, presharedKey, serverPrivateKey, clientPublicKey)
	sshListener, err := wireguardNet.ListenTCPAddrPort(netip.AddrPortFrom(remoteWireGuardInternalAddr, 0))
	require.NoError(t, err)

	const foundPool = "mypool"
	sshUser, sshPrivateKey, sshAddr := ssh.TestServer(t, ssh.TestConfig{
		Listener: sshListener,
		ExecResults: map[string]ssh.TestExecResult{
			fmt.Sprintf(`/usr/sbin/zpool status %s`, foundPool): {
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
`, foundPool),
				ExitCode: 0,
			},
		},
	})
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
	const (
		wireguardSecretName             = "wireguard"
		wireguardSecretKeyPeerPublicKey = "peer-public-key"
		wireguardSecretKeyPresharedKey  = "preshared-key"
		wireguardSecretKeyPrivateKey    = "private-key"
	)
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: wireguardSecretName, Namespace: run.Namespace},
		Data: map[string][]byte{
			wireguardSecretKeyPeerPublicKey: serverPublicKey[:],
			wireguardSecretKeyPresharedKey:  presharedKey[:],
			wireguardSecretKeyPrivateKey:    clientPrivateKey[:],
		},
	}))
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: foundPool, Namespace: run.Namespace},
		Spec: zfspool.Spec{
			Name: foundPool,
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
			WireGuard: &zfspool.WireGuardSpec{
				PeerAddress: wireguardAddr,
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
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pool := zfspool.Pool{
			ObjectMeta: metav1.ObjectMeta{Name: foundPool, Namespace: run.Namespace},
		}
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&pool), &pool))
		assert.Equal(collect, "Online", pool.Status.State)
		assert.Equal(collect, "", pool.Status.Reason)
	}, zfspool.MaxReconcileWait*120/100, 10*time.Millisecond)

	// TODO not found check too
}
