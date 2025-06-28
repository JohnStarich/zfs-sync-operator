package pool_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/clock"
	"github.com/johnstarich/zfs-sync-operator/internal/operator"
	zfspool "github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/johnstarich/zfs-sync-operator/internal/ssh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSnapshot(t *testing.T) {
	t.Parallel()
	const (
		someZPoolName    = "some-zpool"
		somePoolName     = "some-pool"
		someSnapshotName = "some-snapshot"
	)
	makeCtx := func() context.Context {
		ctx, cancel := context.WithCancel(TestEnv.Context())
		t.Cleanup(cancel)
		return ctx
	}
	for _, tc := range []struct {
		description  string
		execResults  map[string]*ssh.TestExecResult
		snapshotSpec zfspool.SnapshotSpec
		expectStatus *zfspool.SnapshotStatus
	}{
		{
			description: "happy path - single dataset",
			execResults: map[string]*ssh.TestExecResult{
				"/usr/sbin/zpool status " + someZPoolName: {Stdout: []byte(`state: ONLINE`), ExitCode: 0},
				fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs snapshot %s/some-dataset\@%s`, someZPoolName, someSnapshotName): {
					ExitCode: 0,
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: fmt.Sprintf("%s/some-dataset", someZPoolName)},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotCompleted,
				Reason: "",
			},
		},
		{
			description: "slow reconcile shows Running",
			execResults: map[string]*ssh.TestExecResult{
				"/usr/sbin/zpool status " + someZPoolName: {Stdout: []byte(`state: ONLINE`), ExitCode: 0},
				fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs snapshot %s/some-dataset\@%s`, someZPoolName, someSnapshotName): {
					ExitCode:    0,
					WaitContext: makeCtx(),
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: fmt.Sprintf("%s/some-dataset", someZPoolName)},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotPending,
				Reason: "",
			},
		},
		{
			description: "multiple datasets",
			execResults: map[string]*ssh.TestExecResult{
				"/usr/sbin/zpool status " + someZPoolName: {Stdout: []byte(`state: ONLINE`), ExitCode: 0},
				fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs snapshot %[1]s/some-dataset-1\@%[2]s %[1]s/some-dataset-2\@%[2]s`, someZPoolName, someSnapshotName): {
					ExitCode: 0,
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: fmt.Sprintf("%s/some-dataset-1", someZPoolName)},
						{Name: fmt.Sprintf("%s/some-dataset-2", someZPoolName)},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotCompleted,
				Reason: "",
			},
		},
		{
			description: "multiple recursive datasets",
			execResults: map[string]*ssh.TestExecResult{
				"/usr/sbin/zpool status " + someZPoolName: {Stdout: []byte(`state: ONLINE`), ExitCode: 0},
				fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs snapshot -r %[1]s/some-dataset-1\@%[2]s %[1]s/some-dataset-2\@%[2]s`, someZPoolName, someSnapshotName): {
					ExitCode: 0,
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{
							Name:      fmt.Sprintf("%s/some-dataset-1", someZPoolName),
							Recursive: &zfspool.RecursiveDatasetSpec{},
						},
						{
							Name:      fmt.Sprintf("%s/some-dataset-2", someZPoolName),
							Recursive: &zfspool.RecursiveDatasetSpec{},
						},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotCompleted,
				Reason: "",
			},
		},
		{
			description: "recursive datasets that skip specific child datasets",
			execResults: map[string]*ssh.TestExecResult{
				"/usr/sbin/zpool status " + someZPoolName: {Stdout: []byte(`state: ONLINE`), ExitCode: 0},
				`/usr/sbin/zfs get -H -t filesystem\,volume -r -d 1 -o name name ` + someZPoolName: {Stdout: fmt.Appendf(nil, `
%[1]s
%[1]s/some-dataset-1
%[1]s/some-dataset-2
%[1]s/some-dataset-3
`, someZPoolName), ExitCode: 0},
				fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs snapshot -r %[1]s/some-dataset-1\@%[2]s %[1]s/some-dataset-3\@%[2]s`, someZPoolName, someSnapshotName): {ExitCode: 0},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{
							Name: someZPoolName,
							Recursive: &zfspool.RecursiveDatasetSpec{
								SkipChildren: []string{
									fmt.Sprintf("%s/some-dataset-2", someZPoolName),
								},
							},
						},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotCompleted,
				Reason: "",
			},
		},
		{
			description: "cannot complete snapshot past deadline",
			execResults: map[string]*ssh.TestExecResult{
				"/usr/sbin/zpool status " + someZPoolName: {Stdout: []byte(`state: ONLINE`), ExitCode: 0},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool:     corev1.LocalObjectReference{Name: somePoolName},
				Deadline: &metav1.Time{Time: clock.NewTest().RelativeTime(-1 * time.Hour)},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{{Name: fmt.Sprintf("%s/some-dataset", someZPoolName)}},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotFailed,
				Reason: "did not create snapshot before deadline",
			},
		},
		{
			description: "snapshot without matching pool name prefix fails",
			execResults: map[string]*ssh.TestExecResult{
				"/usr/sbin/zpool status " + someZPoolName: {Stdout: []byte(`state: ONLINE`), ExitCode: 0},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: "some-dataset"},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotError,
				Reason: `invalid dataset selector name "some-dataset": name must start with pool name some-zpool`,
			},
		},
		{
			description: "degraded pool fails",
			execResults: map[string]*ssh.TestExecResult{
				"/usr/sbin/zpool status " + someZPoolName: {Stdout: []byte(`state: DEGRADED`), ExitCode: 0},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: fmt.Sprintf("%s/some-dataset", someZPoolName)},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotError,
				Reason: "pool is unhealthy: Degraded",
			},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			run := operator.RunTest(t, TestEnv)
			sshUser, sshClientPrivateKey, _, sshAddr := ssh.TestServer(t, ssh.TestConfig{ExecResults: tc.execResults})
			const (
				sshSecretName         = "ssh"
				sshPrivateKeySelector = "private-key"
			)
			require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: sshSecretName, Namespace: run.Namespace},
				StringData: map[string]string{
					sshPrivateKeySelector: sshClientPrivateKey,
				},
			}))
			require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &zfspool.Pool{
				ObjectMeta: metav1.ObjectMeta{Name: somePoolName, Namespace: run.Namespace},
				Spec: &zfspool.Spec{
					Name: someZPoolName,
					SSH: &zfspool.SSHSpec{
						User:    sshUser,
						Address: sshAddr.String(),
						PrivateKey: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: sshSecretName,
							},
							Key: sshPrivateKeySelector,
						},
					},
				},
			}))
			require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &zfspool.PoolSnapshot{
				ObjectMeta: metav1.ObjectMeta{Name: someSnapshotName, Namespace: run.Namespace},
				Spec:       tc.snapshotSpec,
			}))
			require.EventuallyWithTf(t, func(collect *assert.CollectT) {
				snapshot := zfspool.PoolSnapshot{
					ObjectMeta: metav1.ObjectMeta{Name: someSnapshotName, Namespace: run.Namespace},
				}
				assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&snapshot), &snapshot))
				assert.Equal(collect, tc.expectStatus, snapshot.Status)
			}, maxWait, tick, "namespace = %s", run.Namespace)
		})
	}
}

func TestSnapshotDeleteDestroysZFSSnapshot(t *testing.T) {
	t.Parallel()
	const (
		somePoolName     = "somepool"
		someSnapshotName = "somesnapshot"
	)
	run := operator.RunTest(t, TestEnv)
	destroyCalled := false
	sshUser, sshClientPrivateKey, _, sshAddr := ssh.TestServer(t, ssh.TestConfig{ExecResults: map[string]*ssh.TestExecResult{
		"/usr/sbin/zpool status " + somePoolName: {Stdout: []byte(`state: ONLINE`), ExitCode: 0},
		fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs snapshot -r %s/some-dataset\@%s`, somePoolName, someSnapshotName): {ExitCode: 0},
		fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs destroy -r %s/some-dataset\@%s`, somePoolName, someSnapshotName):  {ExitCode: 0, Called: &destroyCalled},
	}})
	const (
		sshSecretName         = "ssh"
		sshPrivateKeySelector = "private-key"
	)
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: sshSecretName, Namespace: run.Namespace},
		StringData: map[string]string{
			sshPrivateKeySelector: sshClientPrivateKey,
		},
	}))
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: somePoolName, Namespace: run.Namespace},
		Spec: &zfspool.Spec{
			Name: somePoolName,
			SSH: &zfspool.SSHSpec{
				User:    sshUser,
				Address: sshAddr.String(),
				PrivateKey: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: sshSecretName,
					},
					Key: sshPrivateKeySelector,
				},
			},
		},
	}))
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &zfspool.PoolSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: someSnapshotName, Namespace: run.Namespace},
		Spec: zfspool.SnapshotSpec{
			Pool: corev1.LocalObjectReference{Name: somePoolName},
			SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
				Datasets: []zfspool.DatasetSelector{
					{Name: fmt.Sprintf("%s/some-dataset", somePoolName), Recursive: &zfspool.RecursiveDatasetSpec{}},
				},
			},
		},
	}))
	snapshot := zfspool.PoolSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: someSnapshotName, Namespace: run.Namespace},
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&snapshot), &snapshot))
		assert.Equal(collect, &zfspool.SnapshotStatus{
			State: zfspool.SnapshotCompleted,
		}, snapshot.Status)
	}, maxWait, tick)
	require.NoError(t, TestEnv.Client().Delete(TestEnv.Context(), &snapshot))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		snapshot := zfspool.PoolSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: someSnapshotName, Namespace: run.Namespace},
		}
		err := TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&snapshot), &snapshot)
		assert.True(collect, apierrors.IsNotFound(err))
	}, maxWait, tick)
	assert.True(t, destroyCalled, "ZFS destroy should be called for deleted snapshot")
}
