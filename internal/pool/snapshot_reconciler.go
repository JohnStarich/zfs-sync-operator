package pool

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// SnapshotReconciler reconciles PoolSnapshot resources to create a set of ZFS snapshots across the whole pool
type SnapshotReconciler struct {
	client client.Client
}

// registerSnapshotReconciler registers a PoolSnapshot reconciler with manager
func registerSnapshotReconciler(manager manager.Manager) error {
	ctrl, err := controller.New("poolsnapshot", manager, controller.Options{
		Reconciler: &SnapshotReconciler{
			client: manager.GetClient(),
		},
	})
	if err != nil {
		return err
	}

	if err := ctrl.Watch(source.Kind(
		manager.GetCache(),
		&PoolSnapshot{},
		&handler.TypedEnqueueRequestForObject[*PoolSnapshot]{},
		predicate.TypedGenerationChangedPredicate[*PoolSnapshot]{},
	)); err != nil {
		return err
	}

	return nil
}

func (r *SnapshotReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking request", "request", request)
	var snapshot PoolSnapshot
	if err := r.client.Get(ctx, request.NamespacedName, &snapshot); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}
