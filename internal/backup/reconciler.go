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
	err := r.client.Get(ctx, request.NamespacedName, &backup)
	if err != nil {
		return reconcile.Result{}, err
	}
	log.Info("backup event!", "backup", backup)
	return reconcile.Result{}, nil
}
