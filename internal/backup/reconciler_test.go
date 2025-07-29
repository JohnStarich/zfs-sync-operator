package backup_test

import (
	"fmt"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/johnstarich/go/datasize"
	zfsbackup "github.com/johnstarich/zfs-sync-operator/internal/backup"
	"github.com/johnstarich/zfs-sync-operator/internal/envtestrunner"
	"github.com/johnstarich/zfs-sync-operator/internal/operator"
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
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			run := operator.RunTest(t, TestEnv)
			source := makePool(t, "source", run, TestPoolOptions{
				Unhealthy: tc.sourceErr,
				SSHExecPrefixResults: map[string]*ssh.TestExecResult{
					"/usr/bin/sudo /usr/sbin/zfs send ": {},
				},
			})
			if !tc.skipSnapshotSchedule {
				source.Spec.Snapshots = &zfspool.SnapshotsSpec{}
				require.NoError(t, TestEnv.Client().Update(TestEnv.Context(), &source))
			}
			destination := makePool(t, "destination", run, TestPoolOptions{
				Unhealthy: tc.destinationErr,
				SSHExecPrefixResults: map[string]*ssh.TestExecResult{
					"/usr/bin/sudo /usr/sbin/zfs receive ": {},
				},
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

type TestPoolOptions struct {
	Unhealthy            bool // return an error for 'zpool status' command
	SSHExecResults       map[string]*ssh.TestExecResult
	SSHExecPrefixResults map[string]*ssh.TestExecResult
	WireGuard            bool
}

func makePool(tb testing.TB, name string, run operator.TestRunConfig, options TestPoolOptions) zfspool.Pool {
	tb.Helper()
	if options.SSHExecResults == nil {
		options.SSHExecResults = make(map[string]*ssh.TestExecResult)
	}
	if options.SSHExecPrefixResults == nil {
		options.SSHExecPrefixResults = make(map[string]*ssh.TestExecResult)
	}
	options.SSHExecPrefixResults[`/usr/sbin/zpool status `] = &ssh.TestExecResult{Stdout: []byte(`state: ONLINE`)}
	if options.Unhealthy {
		options.SSHExecPrefixResults[`/usr/sbin/zpool status `] = &ssh.TestExecResult{
			Stdout:   []byte(`error!`),
			ExitCode: 1,
		}
	}

	sshConfig := ssh.TestConfig{
		ExecResults:       options.SSHExecResults,
		ExecPrefixResults: options.SSHExecPrefixResults,
	}
	pool := zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace},
		Spec: &zfspool.Spec{
			Name: name,
		},
	}

	if options.WireGuard {
		presharedKey, err := wgtypes.GenerateKey()
		require.NoError(tb, err)

		peerPrivateKey, err := wgtypes.GeneratePrivateKey()
		require.NoError(tb, err)
		peerPublicKey := peerPrivateKey.PublicKey()

		localPrivateKey, err := wgtypes.GeneratePrivateKey()
		require.NoError(tb, err)
		localPublicKey := localPrivateKey.PublicKey()

		remoteWireGuardInterfaceAddr := netip.MustParseAddr("10.3.0.2")
		localWireGuardInterfaceAddr := netip.MustParseAddr("10.3.0.3")
		wireguardNet, wireguardAddr := wireguard.StartTest(tb, remoteWireGuardInterfaceAddr, presharedKey, peerPrivateKey, localPublicKey)
		sshConfig.Listener, err = wireguardNet.ListenTCPAddrPort(netip.AddrPortFrom(remoteWireGuardInterfaceAddr, 0))
		require.NoError(tb, err)
		tb.Cleanup(func() {
			require.NoError(tb, sshConfig.Listener.Close())
		})

		const (
			wireguardSecretKeyPeerPublicKey   = "peer-public-key"
			wireguardSecretKeyPresharedKey    = "preshared-key"
			wireguardSecretKeyLocalPrivateKey = "local-private-key"
		)
		wireguardSecretName := "wireguard-" + name
		require.NoError(tb, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: wireguardSecretName, Namespace: run.Namespace},
			Data: map[string][]byte{
				wireguardSecretKeyPeerPublicKey:   peerPublicKey[:],
				wireguardSecretKeyPresharedKey:    presharedKey[:],
				wireguardSecretKeyLocalPrivateKey: localPrivateKey[:],
			},
		}))
		pool.Spec.WireGuard = &zfspool.WireGuardSpec{
			LocalAddress: localWireGuardInterfaceAddr,
			PeerAddress:  wireguardAddr.String(),
			PeerPublicKey: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: wireguardSecretName},
				Key:                  wireguardSecretKeyPeerPublicKey,
			},
			PresharedKey: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: wireguardSecretName},
				Key:                  wireguardSecretKeyPresharedKey,
			},
			LocalPrivateKey: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: wireguardSecretName},
				Key:                  wireguardSecretKeyLocalPrivateKey,
			},
		}
	}

	sshUser, sshClientPrivateKey, sshServerPublicKey, sshAddr := ssh.TestServer(tb, sshConfig)
	const sshPrivateKeySelector = "private-key"
	sshSecretName := "ssh-" + name
	require.NoError(tb, TestEnv.Client().Create(TestEnv.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: sshSecretName, Namespace: run.Namespace},
		StringData: map[string]string{
			sshPrivateKeySelector: sshClientPrivateKey,
		},
	}))
	pool.Spec.SSH = &zfspool.SSHSpec{
		User:    sshUser,
		Address: sshAddr.String(),
		HostKey: sshServerPublicKey,
		PrivateKey: corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: sshSecretName,
			},
			Key: sshPrivateKeySelector,
		},
	}

	require.NoError(tb, TestEnv.Client().Create(TestEnv.Context(), &pool))
	return pool
}

func TestBackupSendAndReceive(t *testing.T) {
	t.Parallel()
	run := operator.RunTest(t, TestEnv)
	sourceSSHResults := make(map[string]*ssh.TestExecResult)
	const (
		someDataset1 = "source/some-dataset-1"
		someDataset2 = "source/some-dataset-2"
	)
	source := makePool(t, "source", run, TestPoolOptions{
		SSHExecResults: sourceSSHResults,
		SSHExecPrefixResults: map[string]*ssh.TestExecResult{
			`/usr/bin/sudo /usr/sbin/zfs snapshot -r ` + someDataset1: {ExitCode: 0},
			`/usr/bin/sudo /usr/sbin/zfs snapshot -r ` + someDataset2: {ExitCode: 0},
		},
	})
	source.Spec.Snapshots = &zfspool.SnapshotsSpec{
		Intervals: []zfspool.SnapshotIntervalSpec{
			{Name: "hourly", Interval: metav1.Duration{Duration: 1 * time.Hour}, HistoryLimit: 1},
		},
		Template: zfspool.SnapshotSpecTemplate{
			Datasets: []zfspool.DatasetSelector{
				{Name: someDataset1, Recursive: &zfspool.RecursiveDatasetSpec{}},
				{Name: someDataset2, Recursive: &zfspool.RecursiveDatasetSpec{}},
			},
		},
	}
	require.NoError(t, TestEnv.Client().Update(TestEnv.Context(), &source))
	var sourceSnapshot zfspool.PoolSnapshot
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var sourceSnapshots zfspool.PoolSnapshotList
		assert.NoError(collect, TestEnv.Client().List(TestEnv.Context(), &sourceSnapshots, &client.ListOptions{Namespace: run.Namespace}))
		if assert.Len(collect, sourceSnapshots.Items, 1) {
			sourceSnapshot = *sourceSnapshots.Items[0]
			if assert.NotNil(collect, sourceSnapshot.Status) {
				assert.Equalf(collect, zfspool.SnapshotCompleted, sourceSnapshot.Status.State, "Status: %v", sourceSnapshot.Status)
			}
		}
	}, maxWait, tick)
	send1Called, send2Called := false, false
	sourceSSHResults[fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs send --raw --replicate %s\@%s`, someDataset1, sourceSnapshot.Name)] = &ssh.TestExecResult{Stdout: []byte(`some data`), Called: &send1Called}
	sourceSSHResults[fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs send --raw --replicate %s\@%s`, someDataset2, sourceSnapshot.Name)] = &ssh.TestExecResult{Stdout: []byte(`some data`), Called: &send2Called}

	receiveCalled := false
	destination := makePool(t, "destination", run, TestPoolOptions{
		SSHExecResults: map[string]*ssh.TestExecResult{
			`/usr/bin/sudo /usr/sbin/zfs receive -d -s destination`: {ExpectStdin: []byte(`some data`), Called: &receiveCalled},
		},
	})

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
			State:            zfsbackup.Ready,
			LastSentSnapshot: &corev1.LocalObjectReference{Name: sourceSnapshot.Name},
		}, backup.Status)
	}, maxWait, tick)
	assert.True(t, send1Called, "ZFS send should be called for the snapshot's dataset 1")
	assert.True(t, send2Called, "ZFS send should be called for the snapshot's dataset 2")
	assert.True(t, receiveCalled, "ZFS receive should be called for sent snapshots")
}

func TestBackupSendAndReceiveIncremental(t *testing.T) {
	t.Parallel()
	run := operator.RunTest(t, TestEnv)
	// Create pools and wait for scheduled snapshot
	sourceSSHResults := make(map[string]*ssh.TestExecResult)
	const someDataset = "source/some-dataset"
	source := makePool(t, "source", run, TestPoolOptions{
		SSHExecResults: sourceSSHResults,
		SSHExecPrefixResults: map[string]*ssh.TestExecResult{
			`/usr/bin/sudo /usr/sbin/zfs snapshot -r ` + someDataset: {ExitCode: 0},
		},
	})
	source.Spec.Snapshots = &zfspool.SnapshotsSpec{
		Intervals: []zfspool.SnapshotIntervalSpec{
			{Name: "hourly", Interval: metav1.Duration{Duration: 1 * time.Hour}, HistoryLimit: 1},
		},
		Template: zfspool.SnapshotSpecTemplate{
			Datasets: []zfspool.DatasetSelector{{Name: someDataset, Recursive: &zfspool.RecursiveDatasetSpec{}}},
		},
	}
	require.NoError(t, TestEnv.Client().Update(TestEnv.Context(), &source))
	var sourceSnapshot zfspool.PoolSnapshot
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var sourceSnapshots zfspool.PoolSnapshotList
		assert.NoError(collect, TestEnv.Client().List(TestEnv.Context(), &sourceSnapshots, &client.ListOptions{Namespace: run.Namespace}))
		if assert.Len(collect, sourceSnapshots.Items, 1) {
			sourceSnapshot = *sourceSnapshots.Items[0]
			if assert.NotNil(collect, sourceSnapshot.Status) {
				assert.Equalf(collect, zfspool.SnapshotCompleted, sourceSnapshot.Status.State, "Status: %v", sourceSnapshot.Status)
			}
		}
	}, maxWait, tick)
	sourceSSHResults[fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs send --raw --replicate %s\@%s`, someDataset, sourceSnapshot.Name)] = &ssh.TestExecResult{Stdout: []byte(`some data`)}

	receiveCalled := false
	destination := makePool(t, "destination", run, TestPoolOptions{
		SSHExecResults: map[string]*ssh.TestExecResult{
			`/usr/bin/sudo /usr/sbin/zfs receive -d -s destination`: {ExpectStdin: []byte(`some data`), Called: &receiveCalled},
		},
	})

	// Create backup and wait for initial sync
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
			State:            zfsbackup.Ready,
			LastSentSnapshot: &corev1.LocalObjectReference{Name: sourceSnapshot.Name},
		}, backup.Status)
	}, maxWait, tick)
	assert.True(t, receiveCalled, "ZFS receive should be called for sent snapshots")

	// Create next snapshot and wait for incremental sync
	receiveCalled = false
	const incrementalSnapshotName = "incremental-snapshot"
	sourceSSHResults[fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs send --raw --replicate -I \@%s %s\@%s`, sourceSnapshot.Name, someDataset, incrementalSnapshotName)] = &ssh.TestExecResult{Stdout: []byte(`some data`)}
	incrementalSnapshot := zfspool.PoolSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: incrementalSnapshotName, Namespace: run.Namespace,
			Annotations: map[string]string{"zfs-sync-operator.johnstarich.com/snapshot-timestamp": run.Clock.RoundedRelativeTime(1*time.Hour, 2*time.Hour).Format(time.RFC3339)},
		},
		Spec: zfspool.SnapshotSpec{
			Pool: corev1.LocalObjectReference{Name: source.Name},
			SnapshotSpecTemplate: zfspool.SnapshotSpecTemplate{
				Datasets: []zfspool.DatasetSelector{{Name: someDataset, Recursive: &zfspool.RecursiveDatasetSpec{}}},
			},
		},
	}
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &incrementalSnapshot))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&incrementalSnapshot), &incrementalSnapshot))
		if assert.NotNil(collect, incrementalSnapshot.Status) {
			assert.Equalf(collect, zfspool.SnapshotCompleted, incrementalSnapshot.Status.State, "Status: %v", incrementalSnapshot.Status)
		}
	}, maxWait, tick)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&backup), &backup))
		assert.Equal(collect, &zfsbackup.Status{
			State:            zfsbackup.Ready,
			LastSentSnapshot: &corev1.LocalObjectReference{Name: incrementalSnapshot.Name},
		}, backup.Status)
	}, maxWait, tick)
	assert.True(t, receiveCalled, "ZFS receive should be called for incrementally sent snapshots")
}

func TestBackupResumeSendAndReceive(t *testing.T) {
	t.Parallel()
	run := operator.RunTest(t, TestEnv)
	// Create pools and wait for scheduled snapshot
	sourceSSHResults := make(map[string]*ssh.TestExecResult)
	const (
		someSourceDataset      = "source/some-dataset"
		someDestinationDataset = "destination/some-dataset"
	)
	source := makePool(t, "source", run, TestPoolOptions{
		SSHExecResults: sourceSSHResults,
		SSHExecPrefixResults: map[string]*ssh.TestExecResult{
			`/usr/bin/sudo /usr/sbin/zfs snapshot -r ` + someSourceDataset: {ExitCode: 0},
		},
	})
	source.Spec.Snapshots = &zfspool.SnapshotsSpec{
		Intervals: []zfspool.SnapshotIntervalSpec{
			{Name: "hourly", Interval: metav1.Duration{Duration: 1 * time.Hour}, HistoryLimit: 1},
		},
		Template: zfspool.SnapshotSpecTemplate{
			Datasets: []zfspool.DatasetSelector{{Name: someSourceDataset, Recursive: &zfspool.RecursiveDatasetSpec{}}},
		},
	}
	require.NoError(t, TestEnv.Client().Update(TestEnv.Context(), &source))
	var sourceSnapshot zfspool.PoolSnapshot
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var sourceSnapshots zfspool.PoolSnapshotList
		assert.NoError(collect, TestEnv.Client().List(TestEnv.Context(), &sourceSnapshots, &client.ListOptions{Namespace: run.Namespace}))
		if assert.Len(collect, sourceSnapshots.Items, 1) {
			sourceSnapshot = *sourceSnapshots.Items[0]
			if assert.NotNil(collect, sourceSnapshot.Status) {
				assert.Equalf(collect, zfspool.SnapshotCompleted, sourceSnapshot.Status.State, "Status: %v", sourceSnapshot.Status)
			}
		}
	}, maxWait, tick)
	const someReceiveResumeToken = "some-receive-resume-token"
	sourceSSHResults[fmt.Sprintf(`/usr/bin/sudo /usr/sbin/zfs send -t %s`, someReceiveResumeToken)] = &ssh.TestExecResult{Stdout: []byte(`some data`)}

	// Create backup and patch status as "in progress"
	const someBackup = "mybackup"
	backup := zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: someBackup, Namespace: run.Namespace},
		Spec: zfsbackup.Spec{
			Source: corev1.LocalObjectReference{Name: source.Name},
		},
	}
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &backup))
	require.NoError(t, TestEnv.Client().Status().Patch(TestEnv.Context(), &zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: someBackup, Namespace: run.Namespace},
		Status: &zfsbackup.Status{
			InProgressSnapshot: &corev1.LocalObjectReference{
				Name: sourceSnapshot.Name,
			},
		},
	}, client.Merge))

	// then set up destination pool
	receiveCalled := false
	destination := makePool(t, "destination", run, TestPoolOptions{
		SSHExecResults: map[string]*ssh.TestExecResult{
			fmt.Sprintf(`/usr/sbin/zfs get -H -o value receive\_resume\_token %s`, someDestinationDataset): {Stdout: []byte(someReceiveResumeToken)},
			`/usr/bin/sudo /usr/sbin/zfs receive -d -s destination`:                                        {ExpectStdin: []byte(`some data`), Called: &receiveCalled},
		},
	})
	require.NoError(t, TestEnv.Client().Patch(TestEnv.Context(), &zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: someBackup, Namespace: run.Namespace},
		Spec: zfsbackup.Spec{
			Destination: corev1.LocalObjectReference{Name: destination.Name},
		},
	}, client.Merge))

	// and wait for sync
	backup = zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: someBackup, Namespace: run.Namespace},
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&backup), &backup))
		assert.Equal(collect, &zfsbackup.Status{
			State:            zfsbackup.Ready,
			LastSentSnapshot: &corev1.LocalObjectReference{Name: sourceSnapshot.Name},
		}, backup.Status)
	}, maxWait, tick)
	assert.True(t, receiveCalled, "ZFS receive should be called for resumed send snapshots")
}

type zeroReader struct{}

func (z zeroReader) Read(b []byte) (int, error) {
	return len(b), nil
}

func BenchmarkSendSpeed(b *testing.B) {
	const maxWait = maxWait * 2 // double wait time in benchmark
	for _, tc := range []struct {
		description         string
		source, destination TestPoolOptions
	}{
		{
			description: "SSH to SSH",
			source:      TestPoolOptions{},
			destination: TestPoolOptions{},
		},
		{
			description: "SSH to WireGuard",
			source:      TestPoolOptions{},
			destination: TestPoolOptions{WireGuard: true},
		},
		{
			description: "WireGuard to SSH",
			source:      TestPoolOptions{WireGuard: true},
			destination: TestPoolOptions{},
		},
		{
			description: "WireGuard to WireGuard",
			source:      TestPoolOptions{WireGuard: true},
			destination: TestPoolOptions{WireGuard: true},
		},
	} {
		b.Run(tc.description, func(b *testing.B) {
			iterationBytes := datasize.Mebibytes(1).Bytes()
			bytes := int64(b.N) * iterationBytes
			b.SetBytes(iterationBytes)

			run := operator.RunTest(b, TestEnv)
			tc.source.SSHExecPrefixResults = map[string]*ssh.TestExecResult{
				`/usr/sbin/zpool status `:               {Stdout: []byte(`state: ONLINE`)},
				`/usr/bin/sudo /usr/sbin/zfs snapshot `: {ExitCode: 0},
				`/usr/bin/sudo /usr/sbin/zfs send `:     {StdoutReader: io.LimitReader(zeroReader{}, bytes)},
			}
			source := makePool(b, "source", run, tc.source)
			const someDataset = "source/some-dataset-1"
			source.Spec.Snapshots = &zfspool.SnapshotsSpec{
				Intervals: []zfspool.SnapshotIntervalSpec{
					{Name: "hourly", Interval: metav1.Duration{Duration: 1 * time.Hour}, HistoryLimit: 1},
				},
				Template: zfspool.SnapshotSpecTemplate{
					Datasets: []zfspool.DatasetSelector{
						{Name: someDataset, Recursive: &zfspool.RecursiveDatasetSpec{}},
					},
				},
			}
			require.NoError(b, TestEnv.Client().Update(TestEnv.Context(), &source))
			var sourceSnapshot zfspool.PoolSnapshot
			require.EventuallyWithT(b, func(collect *assert.CollectT) {
				var sourceSnapshots zfspool.PoolSnapshotList
				assert.NoError(collect, TestEnv.Client().List(TestEnv.Context(), &sourceSnapshots, &client.ListOptions{Namespace: run.Namespace}))
				if assert.Len(collect, sourceSnapshots.Items, 1) {
					sourceSnapshot = *sourceSnapshots.Items[0]
					if assert.NotNil(collect, sourceSnapshot.Status) {
						assert.Equalf(collect, zfspool.SnapshotCompleted, sourceSnapshot.Status.State, "Status: %v", sourceSnapshot.Status)
					}
				}
			}, maxWait, tick)

			receiveCalled := false
			tc.destination.SSHExecPrefixResults = map[string]*ssh.TestExecResult{
				`/usr/sbin/zpool status `:              {Stdout: []byte(`state: ONLINE`)},
				`/usr/bin/sudo /usr/sbin/zfs receive `: {ReadStdin: true, Called: &receiveCalled},
			}
			destination := makePool(b, "destination", run, tc.destination)

			const someBackup = "mybackup"
			backup := zfsbackup.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: someBackup, Namespace: run.Namespace},
				Spec: zfsbackup.Spec{
					Source:      corev1.LocalObjectReference{Name: source.Name},
					Destination: corev1.LocalObjectReference{Name: destination.Name},
				},
			}
			require.NoError(b, TestEnv.Client().Create(TestEnv.Context(), &backup))

			startedSending := false
			watchBackupStatusUpdates(b, run.Namespace, func(status zfsbackup.Status) bool {
				b.Log("Backup state:", status.State, status.Reason)
				switch status.State {
				case zfsbackup.Ready:
					if startedSending && status.LastSentSnapshot != nil && status.LastSentSnapshot.Name == sourceSnapshot.Name {
						b.StopTimer()
						return false
					}
				case zfsbackup.Sending:
					if !startedSending {
						b.ResetTimer()
					}
					startedSending = true
				case // no action
					zfsbackup.Error,
					zfsbackup.NotReady:
				}
				return true
			})
			assert.True(b, receiveCalled, "ZFS receive should be called for sent snapshots")
		})
	}
}

func watchBackupStatusUpdates(tb testing.TB, namespace string, handleStatus func(zfsbackup.Status) bool) {
	watcher, err := TestEnv.Client().Watch(TestEnv.Context(), &zfsbackup.BackupList{}, &client.ListOptions{
		Namespace: namespace,
	})
	require.NoError(tb, err)
	for event := range watcher.ResultChan() {
		backup, ok := event.Object.(*zfsbackup.Backup)
		if ok && backup.Status != nil {
			if !handleStatus(*backup.Status) {
				watcher.Stop()
			}
		}
	}
}
