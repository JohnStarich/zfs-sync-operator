package backup

import (
	"context"
	"fmt"

	"github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Reconciler reconciles Backup resources to validate their Pools and associated connections
type Reconciler struct {
	client client.Client
}

// RegisterReconciler registers a Backup reconciler with manager
func RegisterReconciler(manager manager.Manager) error {
	ctrl, err := controller.New("backup", manager, controller.Options{
		Reconciler: &Reconciler{client: manager.GetClient()},
	})
	if err != nil {
		return err
	}

	if err := ctrl.Watch(source.Kind(
		manager.GetCache(),
		&Backup{},
		&handler.TypedEnqueueRequestForObject[*Backup]{},
		predicate.TypedGenerationChangedPredicate[*Backup]{},
	)); err != nil {
		return err
	}

	return nil
}

// Reconcile implements [reconcile.Reconciler]
func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking request", "request", request)
	var backup Backup
	if err := r.client.Get(ctx, request.NamespacedName, &backup); err != nil {
		return reconcile.Result{}, err
	}
	logger.Info("got backup", "backup", backup.Spec)

	state, reason, reconcileErr := r.reconcile(ctx, backup)
	if reconcileErr != nil {
		logger.Error(reconcileErr, "reconcile failed")
		state = "Error"
		reason = reconcileErr.Error()
	} else {
		logger.Info("backup reconciled successfully", "state", state)
	}
	backup.Status = &Status{
		State:  state,
		Reason: reason,
	}
	if err := r.client.Status().Update(ctx, &backup); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *Reconciler) reconcile(context.Context, Backup) (state, reason string, returnedErr error) {
	return "Ready", "", nil
}
