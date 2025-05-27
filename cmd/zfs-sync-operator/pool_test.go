package main

import (
	"testing"
	"time"

	zfspool "github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPool(t *testing.T) {
	t.Parallel()
	run := RunTest(t)
	pool := zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: run.Namespace},
	}
	require.NoError(t, TestEnv.Client().Create(TestEnv.Context(), &pool))
	pool = zfspool.Pool{
		ObjectMeta: metav1.ObjectMeta{Name: "mypool", Namespace: run.Namespace},
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, TestEnv.Client().Get(TestEnv.Context(), client.ObjectKeyFromObject(&pool), &pool))
		assert.Equal(collect, "Ready", pool.Status.State)
	}, 1*time.Second, 10*time.Millisecond)
}
