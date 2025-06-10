package backup_test

import (
	"fmt"
	"testing"
	"time"

	zfsbackup "github.com/johnstarich/zfs-sync-operator/internal/backup"
	"github.com/johnstarich/zfs-sync-operator/internal/envtestrunner"
	"github.com/johnstarich/zfs-sync-operator/internal/operator"
	zfspool "github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/johnstarich/zfs-sync-operator/internal/ssh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var TestEnv *envtestrunner.Runner //nolint:gochecknoglobals // The test environment is very expensive to set up, so this performance optimization is required for fast test execution.

func TestMain(m *testing.M) {
	operator.RunTestMain(m, &TestEnv)
}

const (
	maxWait = 10 * time.Second
	tick    = max(10*time.Millisecond, maxWait/10)
)

func TestBackupReady(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		description    string
		sourceErr      bool
		destinationErr bool
		expectStatus   *zfsbackup.Status
	}{
		{
			description:    "Ready when source and destination healthy",
			sourceErr:      false,
			destinationErr: false,
			expectStatus:   &zfsbackup.Status{State: "Ready"},
		},
		{
			description:    "NotReady when source unhealthy",
			sourceErr:      true,
			destinationErr: false,
			expectStatus:   &zfsbackup.Status{State: "NotReady", Reason: `source pool "source" is unhealthy`},
		},
		{
			description:    "NotReady when destination unhealthy",
			sourceErr:      false,
			destinationErr: true,
			expectStatus:   &zfsbackup.Status{State: "NotReady", Reason: `destination pool "destination" is unhealthy`},
		},
		{
			description:    "NotReady when both pools unhealthy",
			sourceErr:      true,
			destinationErr: true,
			expectStatus:   &zfsbackup.Status{State: "NotReady", Reason: `source pool "source" is unhealthy`},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			run := operator.RunTest(t, TestEnv)
			source := makePool(t, "source", run, tc.sourceErr)
			destination := makePool(t, "destination", run, tc.destinationErr)
			backup := zfsbackup.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: "mybackup", Namespace: run.Namespace},
				Spec: zfsbackup.Spec{
					Source:      corev1.LocalObjectReference{Name: source.Name},
					Destination: corev1.LocalObjectReference{Name: destination.Name},
				},
			}
			require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &backup))

			backup = zfsbackup.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: "mybackup", Namespace: run.Namespace},
			}
			require.EventuallyWithTf(t, func(collect *assert.CollectT) {
				assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&backup), &backup))
				assert.Equal(collect, tc.expectStatus, backup.Status)
			}, maxWait, tick, "namespace = %s", run.Namespace)
		})
	}
}

func makePool(tb testing.TB, name string, run operator.TestRunConfig, shouldErr bool) zfspool.Pool {
	tb.Helper()
	result := ssh.TestExecResult{
		Stdout: []byte(`state: ONLINE`),
	}
	if shouldErr {
		result = ssh.TestExecResult{
			Stdout:   []byte(`error!`),
			ExitCode: 1,
		}
	}
	sshUser, sshClientPrivateKey, sshServerPublicKey, sshAddr := ssh.TestServer(tb, ssh.TestConfig{ExecResults: map[string]ssh.TestExecResult{
		fmt.Sprintf(`/usr/sbin/zpool status %s`, name): result,
	}})
	const (
		sshSecretBaseName     = "ssh"
		sshPrivateKeySelector = "private-key"
	)
	sshSecretName := fmt.Sprintf("%s-%s", name, sshSecretBaseName)
	require.NoError(tb, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: sshSecretName, Namespace: run.Namespace},
		StringData: map[string]string{
			sshPrivateKeySelector: sshClientPrivateKey,
		},
	}))
	pool := zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace},
		Spec: &zfspool.Spec{
			Name: name,
			SSH: &zfspool.SSHSpec{
				User:    sshUser,
				Address: sshAddr.String(),
				HostKey: sshServerPublicKey,
				PrivateKey: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: sshSecretName,
					},
					Key: sshPrivateKeySelector,
				},
			},
		},
	}
	require.NoError(tb, TestEnv.Client().Create(TestEnv.Context(), &pool))
	return pool
}
