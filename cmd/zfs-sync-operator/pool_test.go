package main

import (
	"testing"
	"time"

	zfspool "github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/johnstarich/zfs-sync-operator/internal/ssh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPool(t *testing.T) {
	t.Parallel()
	run := RunTest(t)
	sshUser, sshPrivateKey, sshAddr := ssh.TestServer(t, ssh.TestConfig{
		ExecResults: map[string]ssh.TestExecResult{
			"/usr/bin/echo Hello, World!": {
				Stdout: []byte("Hello, World!\n"),
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
	pool := zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: run.Namespace},
		Spec: zfspool.Spec{
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
	}
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &pool))
	pool = zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: run.Namespace},
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&pool), &pool))
		assert.Equal(collect, "Hello, World!\n", pool.Status.State)
	}, 1*time.Second, 10*time.Millisecond)
}
