package backup

import (
	"context"
	"fmt"
	"io"
	"slices"

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
func RegisterReconciler(ctx context.Context, manager manager.Manager) error {
	reconciler := &Reconciler{client: manager.GetClient()}

	ctrl, err := controller.New("backup", manager, controller.Options{
		Reconciler: reconciler,
	})
	if err != nil {
		return err
	}

	err = manager.GetFieldIndexer().IndexField(ctx, &Backup{}, sourceProperty, func(o client.Object) []string {
		backup := o.(*Backup)
		if backup.Status == nil {
			return nil
		}
		return []string{backup.Spec.Source.Name}
	})
	if err != nil {
		return errors.WithMessagef(err, "failed to index Backup %s", sourceProperty)
	}
	err = manager.GetFieldIndexer().IndexField(ctx, &Backup{}, destinationProperty, func(o client.Object) []string {
		backup := o.(*Backup)
		if backup.Status == nil {
			return nil
		}
		return []string{backup.Spec.Destination.Name}
	})
	if err != nil {
		return errors.WithMessagef(err, "failed to index Backup %s", destinationProperty)
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
			logger.Info("Received pool update", "matching backups", len(requests))
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

func (r *Reconciler) reconcile(ctx context.Context, backup Backup) (State, string, error) {
	var source, destination pool.Pool
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Source.Name}, &source); err != nil {
		return "", "", errors.WithMessage(err, "get source Pool")
	}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Destination.Name}, &destination); err != nil {
		return "", "", errors.WithMessage(err, "get destination Pool")
	}

	if err := validatePoolIsHealthy("source", source); err != nil {
		return NotReady, err.Error(), nil
	}
	if source.Spec == nil || source.Spec.Snapshots == nil {
		return NotReady, fmt.Sprintf("source pool %q must define .spec.snapshots, either empty (the default schedule) or custom intervals", source.Name), nil
	}
	if err := validatePoolIsHealthy("destination", destination); err != nil {
		return NotReady, err.Error(), nil
	}

	var completedSnapshots pool.PoolSnapshotList
	err := r.client.List(ctx, &completedSnapshots, &client.ListOptions{
		FieldSelector: fields.AndSelectors(
			fields.OneTermEqualSelector(".spec.pool.name", source.Name),
			fields.OneTermEqualSelector(".status.state", string(pool.SnapshotCompleted)),
		),
		Namespace: backup.Namespace,
	})
	if err != nil {
		return "", "", err
	}
	if err := pool.SortScheduledSnapshots(completedSnapshots.Items); err != nil {
		return "", "", err
	}
	sendSnapshots := completedSnapshots.Items
	if backup.Status.LastSentSnapshot != nil {
		lastSentSnapshotIndex := slices.IndexFunc(sendSnapshots, func(snapshot *pool.PoolSnapshot) bool {
			return snapshot.Name == backup.Status.LastSentSnapshot.Name
		})
		if lastSentSnapshotIndex != -1 {
			sendSnapshots = sendSnapshots[lastSentSnapshotIndex+1:]
		}
	}
	if len(sendSnapshots) == 0 {
		return Ready, "", nil
	}
	sendLastSnapshot := sendSnapshots[len(sendSnapshots)-1]

	err = source.WithConnection(ctx, r.client, func(sourceConn *pool.Connection) error {
		return destination.WithConnection(ctx, r.client, func(destinationConn *pool.Connection) error {
			return r.reconcileWithConnections(ctx, backup, source, destination, sourceConn, destinationConn, sendLastSnapshot)
		})
	})
	if err != nil {
		return "", "", err
	}

	return Ready, "", nil
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
		var destinationBackups BackupList
		err := r.client.List(ctx, &destinationBackups, &client.ListOptions{
			FieldSelector: fields.OneTermEqualSelector(destinationProperty, pool.Name),
			Namespace:     pool.Namespace,
		})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to list matching destination backups")
		}
		allBackups = append(allBackups, destinationBackups.Items...)
	}
	return allBackups, nil
}

func (r *Reconciler) reconcileWithConnections(ctx context.Context, backup Backup, sourcePool, destinationPool pool.Pool, sourceConn, destinationConn *pool.Connection, snapshot *pool.PoolSnapshot) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sendArgs := []string{
		"send",
		"--raw",          // Always send raw streams. Must be enabled to avoid decrypting and sending data in the clear.
		"--skip-missing", // Send snapshots with this name, skipping any datasets in the hierarchy missing the snapshot.
	}
	if backup.Status != nil && backup.Status.LastSentSnapshot != nil { // Use incremental send after first send
		sendArgs = append(sendArgs, "-I", "@"+backup.Status.LastSentSnapshot.Name)
	}
	sendArgs = append(sendArgs, fmt.Sprintf("%s@%s", sourcePool.Spec.Name, snapshot.Name)) // TODO guarantee actually unique ID (UID), not just uniquely generated snapshot's name

	const maxErrors = 2
	errs := make(chan error, maxErrors)
	reader, writer := io.Pipe() // TODO show progress
	go func() {
		errs <- sourceConn.ExecWriteStdout(ctx, writer, "/usr/sbin/zfs", sendArgs...)
	}()
	go func() {
		errs <- destinationConn.ExecReadStdin(ctx, reader, "/usr/sbin/zfs", "receive",
			"-d", // Preserve hierarchy when using recursive replicated streams
			destinationPool.Spec.Name,
		)
	}()
	for range maxErrors {
		select {
		case err := <-errs:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func validatePoolIsHealthy(contextualName string, p pool.Pool) (returnedErr error) {
	defer func() {
		returnedErr = errors.WithMessagef(returnedErr, "%s pool %q is unhealthy", contextualName, p.Name)
	}()
	switch {
	case p.Status != nil && p.Status.State == pool.Online:
		return nil
	case p.Status == nil:
		return errors.New("no status available")
	case p.Status.Reason == "":
		return errors.New(string(p.Status.State))
	default:
		return errors.Errorf("%s: %s", p.Status.State, p.Status.Reason)
	}
}
