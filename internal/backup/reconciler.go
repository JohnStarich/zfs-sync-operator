package backup

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Reconciler struct {
	client client.Client
}

func NewReconciler(client client.Client) *Reconciler {
	return &Reconciler{client: client}
}

func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.FromContext(ctx)
	log.Info("checking request", "request", request)
	var backup Backup
	if err := r.client.Get(ctx, request.NamespacedName, &backup); err != nil {
		return reconcile.Result{}, err
	}
	backup.Status.State = "Ready"
	if err := r.client.Update(ctx, &backup); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}
