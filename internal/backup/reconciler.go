package backup

import (
	"context"
	"fmt"

	"github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
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

const (
	sourceProperty      = ".spec.source.name"
	destinationProperty = ".spec.destination.name"
)

// RegisterReconciler registers a Backup reconciler with manager
func RegisterReconciler(manager manager.Manager) error {
	reconciler := &Reconciler{client: manager.GetClient()}

	ctrl, err := controller.New("backup", manager, controller.Options{
		Reconciler: reconciler,
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
	if err := ctrl.Watch(source.Kind(
		manager.GetCache(),
		&pool.Pool{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, pool *pool.Pool) []reconcile.Request {
			logger := log.FromContext(ctx)
			allBackups, err := reconciler.matchingBackups(ctx, pool)
			if err != nil {
				logger.Error(err, "Failed to find matching backups for pool", "pool", pool.Name, "namespace", pool.Namespace)
				return nil
			}

			var requests []reconcile.Request
			for _, backup := range allBackups {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name:      backup.Name,
						Namespace: pool.Namespace,
					},
				})
			}
			return requests
		}),
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
	return reconcile.Result{}, reconcileErr
}

func (r *Reconciler) reconcile(ctx context.Context, backup Backup) (state, reason string, returnedErr error) {
	var source, destination pool.Pool
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Source.Name}, &source); err != nil {
		return "", "", errors.WithMessage(err, "get source Pool")
	}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Destination.Name}, &destination); err != nil {
		return "", "", errors.WithMessage(err, "get destination Pool")
	}

	if source.Status == nil || source.Status.State != pool.Online {
		return "NotReady", fmt.Sprintf("source pool %q is unhealthy", source.Name), nil
	}
	if destination.Status == nil || destination.Status.State != pool.Online {
		return "NotReady", fmt.Sprintf("destination pool %q is unhealthy", destination.Name), nil
	}
	return "Ready", "", nil
}

func (r *Reconciler) matchingBackups(ctx context.Context, pool *pool.Pool) ([]Backup, error) {
	var allBackups []Backup
	{
		// Fetch all backups with this pool as their source.
		var sourceBackups BackupList
		err := r.client.List(ctx, &sourceBackups, &client.ListOptions{
			FieldSelector: fields.OneTermEqualSelector(sourceProperty, pool.Name),
			Namespace:     pool.Namespace,
		})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to list matching source backups")
		}
		allBackups = append(allBackups, sourceBackups.Items...)
	}

	{
		// Fetch all backups with this pool as their destination.
		// There may be duplicates, but this shouldn't matter once they're deduplicated by the event handler.
		var remainingDestinationBackups BackupList
		err := r.client.List(ctx, &remainingDestinationBackups, &client.ListOptions{
			FieldSelector: fields.OneTermEqualSelector(destinationProperty, pool.Name),
			Namespace:     pool.Namespace,
		})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to list any remaining destination backups")
		}
		allBackups = append(allBackups, remainingDestinationBackups.Items...)
	}
	return allBackups, nil
}
