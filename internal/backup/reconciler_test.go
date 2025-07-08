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
		description          string
		sourceErr            bool
		destinationErr       bool
		skipSnapshotSchedule bool
		expectStatus         *zfsbackup.Status
	}{
		{
			description:    "Ready when source and destination healthy",
			sourceErr:      false,
			destinationErr: false,
			expectStatus:   &zfsbackup.Status{State: zfsbackup.Ready},
		},
		{
			description:    "NotReady when source unhealthy",
			sourceErr:      true,
			destinationErr: false,
			expectStatus:   &zfsbackup.Status{State: zfsbackup.NotReady, Reason: `source pool "source" is unhealthy: Error: command '/usr/sbin/zpool status source' failed with output: error!: Process exited with status 1`},
		},
		{
			description:    "NotReady when destination unhealthy",
			sourceErr:      false,
			destinationErr: true,
			expectStatus:   &zfsbackup.Status{State: zfsbackup.NotReady, Reason: `destination pool "destination" is unhealthy: Error: command '/usr/sbin/zpool status destination' failed with output: error!: Process exited with status 1`},
		},
		{
			description:    "NotReady when both pools unhealthy",
			sourceErr:      true,
			destinationErr: true,
			expectStatus:   &zfsbackup.Status{State: zfsbackup.NotReady, Reason: `source pool "source" is unhealthy: Error: command '/usr/sbin/zpool status source' failed with output: error!: Process exited with status 1`},
		},
		{
			description:          "NotReady when source does not have a snapshots schedule",
			skipSnapshotSchedule: true,
			expectStatus: &zfsbackup.Status{
				State:  zfsbackup.NotReady,
				Reason: `source pool "source" must define .spec.snapshots, either empty (the default schedule) or custom intervals`,
			},
		},
		// TODO require source and destination be different
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			run := operator.RunTest(t, TestEnv)
			source := makePool(t, "source", run, tc.sourceErr, nil, map[string]*ssh.TestExecResult{
				"/usr/bin/sudo /usr/sbin/zfs send ": {},
			})
			if !tc.skipSnapshotSchedule {
				source.Spec.Snapshots = &zfspool.SnapshotsSpec{}
				require.NoError(t, TestEnv.Client().Update(TestEnv.Context(), &source))
			}
			destination := makePool(t, "destination", run, tc.destinationErr, nil, map[string]*ssh.TestExecResult{
				"/usr/bin/sudo /usr/sbin/zfs receive ": {},
			})
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

func makePool(tb testing.TB, name string, run operator.TestRunConfig, shouldErr bool, execResults, execPrefixResults map[string]*ssh.TestExecResult) zfspool.Pool {
	tb.Helper()
	poolStateResult := &ssh.TestExecResult{
		Stdout: []byte(`state: ONLINE`),
	}
	if shouldErr {
		poolStateResult = &ssh.TestExecResult{
			Stdout:   []byte(`error!`),
			ExitCode: 1,
		}
	}
	if execResults == nil {
		execResults = make(map[string]*ssh.TestExecResult)
	}
	execResults[fmt.Sprintf(`/usr/sbin/zpool status %s`, name)] = poolStateResult

	sshUser, sshClientPrivateKey, sshServerPublicKey, sshAddr := ssh.TestServer(tb, ssh.TestConfig{
		ExecResults:       execResults,
		ExecPrefixResults: execPrefixResults,
	})
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

func TestBackupSendAndReceive(t *testing.T) {
	t.Parallel()
	run := operator.RunTest(t, TestEnv)
	sourceSSHResults := make(map[string]*ssh.TestExecResult)
	source := makePool(t, "source", run, false, sourceSSHResults, nil)
	source.Spec.Snapshots = &zfspool.SnapshotsSpec{
		Intervals: []zfspool.SnapshotIntervalSpec{
			{Name: "hourly", Interval: metav1.Duration{Duration: 1 * time.Hour}, HistoryLimit: 1},
		},
	}
	require.NoError(t, TestEnv.Client().Update(TestEnv.Context(), &source))
	var sourceSnapshot zfspool.PoolSnapshot
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var sourceSnapshots zfspool.PoolSnapshotList
		assert.NoError(collect, TestEnv.Client().List(TestEnv.Context(), &sourceSnapshots, &client.ListOptions{Namespace: run.Namespace}))
		if assert.Len(collect, sourceSnapshots.Items, 1) {
			sourceSnapshot = *sourceSnapshots.Items[0]
		}
	}, maxWait, tick)
	sourceSSHResults[`/usr/bin/sudo /usr/sbin/zfs send --raw --replicate source\@`+sourceSnapshot.Name] = &ssh.TestExecResult{Stdout: []byte(`some data`)}

	receiveCalled := false
	destination := makePool(t, "destination", run, false, map[string]*ssh.TestExecResult{
		`/usr/bin/sudo /usr/sbin/zfs receive -d destination`: {ExpectStdin: []byte(`some data`), Called: &receiveCalled},
	}, nil)

	const someBackup = "mybackup"
	backup := zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: someBackup, Namespace: run.Namespace},
		Spec: zfsbackup.Spec{
			Source:      corev1.LocalObjectReference{Name: source.Name},
			Destination: corev1.LocalObjectReference{Name: destination.Name},
		},
	}
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &backup))

	backup = zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: someBackup, Namespace: run.Namespace},
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&backup), &backup))
		assert.Equal(collect, &zfsbackup.Status{
			State: zfsbackup.Ready,
		}, backup.Status)
	}, maxWait, tick)
	assert.True(t, receiveCalled, "ZFS receive should be called for sent snapshots")
}

func TestBackupSendAndReceiveIncremental(t *testing.T) {
	t.Parallel()
	// TODO
}
