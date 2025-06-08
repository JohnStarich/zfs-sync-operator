package backup

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Reconciler reconciles Backup resources to validate their Pools and associated connections
type Reconciler struct {
	client client.Client
}

// NewReconciler returns a new backup reconciler
func NewReconciler(client client.Client) *Reconciler {
	return &Reconciler{client: client}
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

func (r *Reconciler) reconcile(ctx context.Context, backup Backup) (state, reason string, returnedErr error) {
	/*
		var source, destination pool.Pool
		if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Source.Name}, &source); err != nil {
			return "", "", errors.WithMessage(err, "get source Pool")
		}
		if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Destination.Name}, &destination); err != nil {
			return "", "", errors.WithMessage(err, "get destination Pool")
		}
	*/
	return "Ready", "", nil
}
