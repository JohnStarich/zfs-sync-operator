package backup

import (
	"context"
	"fmt"

	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
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
	sourceProperty      = "source"
	destinationProperty = "destination"
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

	resourceVersion, state, reason, reconcileErr := r.reconcile(ctx, backup)
	backup.ResourceVersion = resourceVersion
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

func (r *Reconciler) reconcile(ctx context.Context, backup Backup) (resourceVersion, state, reason string, returnedErr error) {
	var source, destination pool.Pool
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Source.Name}, &source); err != nil {
		return backup.ResourceVersion, "", "", errors.WithMessage(err, "get source Pool")
	}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Destination.Name}, &destination); err != nil {
		return backup.ResourceVersion, "", "", errors.WithMessage(err, "get destination Pool")
	}

	needLabels := map[string]string{
		name.Label(sourceProperty):      source.Name,
		name.Label(destinationProperty): destination.Name,
	}
	if backup.Labels == nil {
		backup.Labels = make(map[string]string)
	}
	needsUpdate := false
	for key, needValue := range needLabels {
		if backup.Labels[key] != needValue {
			backup.Labels[key] = needValue
			needsUpdate = true
		}
	}
	if needsUpdate {
		if err := r.client.Update(ctx, &backup); err != nil {
			return backup.ResourceVersion, "", "", err
		}
	}

	if source.Status == nil || source.Status.State != "Online" { // TODO use const
		return backup.ResourceVersion, "NotReady", fmt.Sprintf("source pool %q is unhealthy", source.Name), nil
	}
	if destination.Status == nil || destination.Status.State != "Online" { // TODO use const
		return backup.ResourceVersion, "NotReady", fmt.Sprintf("destination pool %q is unhealthy", destination.Name), nil
	}
	return backup.ResourceVersion, "Ready", "", nil
}

func (r *Reconciler) matchingBackups(ctx context.Context, pool *pool.Pool) ([]Backup, error) {
	var allBackups []Backup
	{
		// Fetch all backups with this pool as their source.
		matchesSource, err := labels.NewRequirement(name.Label(sourceProperty), selection.Equals, []string{pool.Name})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to prepare backup list for source query requirement")
		}
		var sourceBackups BackupList
		err = r.client.List(ctx, &sourceBackups, &client.ListOptions{
			LabelSelector: labels.NewSelector().Add(*matchesSource),
			Namespace:     pool.Namespace,
		})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to list matching source backups")
		}
		allBackups = append(allBackups, sourceBackups.Items...)
	}

	{
		// Fetch all backups with this pool as their destination.
		// Also filter out any previous source match to avoid duplicates.
		notMatchesSource, err := labels.NewRequirement(name.Label(sourceProperty), selection.NotEquals, []string{pool.Name})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to prepare backup list for not-source query requirement")
		}
		matchesDestination, err := labels.NewRequirement(name.Label(destinationProperty), selection.Equals, []string{pool.Name})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to prepare backup list for destination query requirement")
		}
		var remainingDestinationBackups BackupList
		err = r.client.List(ctx, &remainingDestinationBackups, &client.ListOptions{
			LabelSelector: labels.NewSelector().Add(*matchesDestination, *notMatchesSource),
			Namespace:     pool.Namespace,
		})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to list any remaining destination backups")
		}
		allBackups = append(allBackups, remainingDestinationBackups.Items...)
	}
	return allBackups, nil
}
