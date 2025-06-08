package backup_test

import (
	"testing"
	"time"

	zfsbackup "github.com/johnstarich/zfs-sync-operator/internal/backup"
	"github.com/johnstarich/zfs-sync-operator/internal/envtestrunner"
	"github.com/johnstarich/zfs-sync-operator/internal/operator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var TestEnv *envtestrunner.Runner //nolint:gochecknoglobals // The test environment is very expensive to set up, so this performance optimization is required for fast test execution.

func TestMain(m *testing.M) {
	operator.RunTestMain(m, &TestEnv)
}

func TestBackup(t *testing.T) {
	t.Parallel()
	run := operator.RunTest(t, TestEnv)
	backup := zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "mybackup", Namespace: run.Namespace},
	}
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &backup))
	backup = zfsbackup.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "mybackup", Namespace: run.Namespace},
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&backup), &backup))
		if assert.NotNil(collect, backup.Status) {
			assert.Equal(collect, "Ready", backup.Status.State)
		}
	}, 1*time.Second, 10*time.Millisecond)
}
