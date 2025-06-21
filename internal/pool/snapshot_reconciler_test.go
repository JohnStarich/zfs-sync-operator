package pool_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/operator"
	zfspool "github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/johnstarich/zfs-sync-operator/internal/ssh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSnapshot(t *testing.T) {
	t.Parallel()
	const (
		somePoolName     = "some-pool"
		someSnapshotName = "some-snapshot"
	)
	makeTimeoutCtx := func(d time.Duration) context.Context {
		ctx, cancel := context.WithTimeout(TestEnv.Context(), d)
		t.Cleanup(cancel)
		return ctx
	}
	for _, tc := range []struct {
		description  string
		execResults  map[string]ssh.TestExecResult
		snapshotSpec zfspool.SnapshotSpec
		expectStatus *zfspool.SnapshotStatus
	}{
		{
			description: "happy path - single dataset",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout:   []byte(`state: ONLINE`),
					ExitCode: 0,
				},
				fmt.Sprintf(`/usr/sbin/zfs snapshot %s/some-dataset\@%s`, somePoolName, someSnapshotName): {
					ExitCode: 0,
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: fmt.Sprintf("%s/some-dataset", somePoolName)},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotCompleted,
				Reason: "",
			},
		},
		{
			description: "happy path - slow reconcile shows Running",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout:   []byte(`state: ONLINE`),
					ExitCode: 0,
				},
				fmt.Sprintf(`/usr/sbin/zfs snapshot %s/some-dataset\@%s`, somePoolName, someSnapshotName): {
					ExitCode:    0,
					WaitContext: makeTimeoutCtx(maxWait),
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: fmt.Sprintf("%s/some-dataset", somePoolName)},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotPending,
				Reason: "",
			},
		},
		{
			description: "happy path - multiple datasets",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout:   []byte(`state: ONLINE`),
					ExitCode: 0,
				},
				fmt.Sprintf(`/usr/sbin/zfs snapshot %[1]s/some-dataset-1\@%[2]s %[1]s/some-dataset-2\@%[2]s`, somePoolName, someSnapshotName): {
					ExitCode: 0,
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: fmt.Sprintf("%s/some-dataset-1", somePoolName)},
						{Name: fmt.Sprintf("%s/some-dataset-2", somePoolName)},
					},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotCompleted,
				Reason: "",
			},
		},
		{
			description: "happy path - multiple recursive datasets",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {
					Stdout:   []byte(`state: ONLINE`),
					ExitCode: 0,
				},
				fmt.Sprintf(`/usr/sbin/zfs snapshot -r %[1]s/some-dataset-1\@%[2]s %[1]s/some-dataset-2\@%[2]s`, somePoolName, someSnapshotName): {
					ExitCode: 0,
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool: corev1.LocalObjectReference{Name: somePoolName},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{
							Name:      fmt.Sprintf("%s/some-dataset-1", somePoolName),
							Recursive: &zfspool.RecursiveDatasetSpec{},
						},
						{
							Name:      fmt.Sprintf("%s/some-dataset-2", somePoolName),
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
			description: "cannot complete snapshot past deadline",
			execResults: map[string]ssh.TestExecResult{
				fmt.Sprintf(`/usr/sbin/zpool status %s`, somePoolName): {Stdout: []byte(`state: ONLINE`)},
				fmt.Sprintf(`/usr/sbin/zfs snapshot %[1]s/some-dataset\@%[2]s`, somePoolName, someSnapshotName): {
					Stdout:   []byte(`some error`),
					ExitCode: 1,
				},
			},
			snapshotSpec: zfspool.SnapshotSpec{
				Pool:     corev1.LocalObjectReference{Name: somePoolName},
				Deadline: &metav1.Time{Time: operator.TestRelativeTime(-1 * time.Hour)},
				SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{{Name: fmt.Sprintf("%s/some-dataset", somePoolName)}},
				},
			},
			expectStatus: &zfspool.SnapshotStatus{
				State:  zfspool.SnapshotFailed,
				Reason: "did not create snapshot before deadline: failed to run '/usr/sbin/zfs snapshot some-pool/some-dataset\\@some-snapshot': some error: Process exited with status 1",
			},
		},
		// TODO test to verify datasets always have pool-name/ as prefix
		// TODO require pool status be Online before creating snapshot
		// TODO ensure a snapshot in Error state counts as active, should only skip over Failed and Completed
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
