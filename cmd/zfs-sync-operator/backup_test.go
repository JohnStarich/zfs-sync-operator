package main

import (
	"testing"
	"time"

	zfsbackup "github.com/johnstarich/zfs-sync-operator/internal/backup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestBackup(t *testing.T) {
	t.Parallel()
	run := RunTest(t)
	backup := zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "mybackup", Namespace: run.Namespace},
	}
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &backup))
	backup = zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "mybackup", Namespace: run.Namespace},
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&backup), &backup))
		assert.Equal(collect, "Ready", backup.Status.State)
	}, 1*time.Second, 10*time.Millisecond)
}
