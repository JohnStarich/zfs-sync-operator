package pool

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
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
	client  client.Client
	timeNow func() time.Time
}

// registerSnapshotReconciler registers a PoolSnapshot reconciler with manager
func registerSnapshotReconciler(ctx context.Context, manager manager.Manager, timeNow func() time.Time) error {
	ctrl, err := controller.New("poolsnapshot", manager, controller.Options{
		Reconciler: &SnapshotReconciler{
			client:  manager.GetClient(),
			timeNow: timeNow,
		},
	})
	if err != nil {
		return err
	}

	err = manager.GetFieldIndexer().IndexField(ctx, &PoolSnapshot{}, ".spec.pool.name", func(o client.Object) []string {
		return []string{o.(*PoolSnapshot).Spec.Pool.Name}
	})
	if err != nil {
		return errors.WithMessage(err, "failed to index PoolSnapshot.spec.pool.name")
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
	state, reason, reconcileErr := r.reconcile(ctx, &snapshot)
	if reconcileErr == nil {
		logger.Info("pool reconciled successfully", "state", state)
		snapshot.Status = &SnapshotStatus{
			State:  state,
			Reason: reason,
		}
	} else {
		logger.Error(reconcileErr, "reconcile failed")
		snapshot.Status = &SnapshotStatus{
			State:  SnapshotError,
			Reason: reconcileErr.Error(),
		}
	}
	if err := r.client.Status().Update(ctx, &snapshot); err != nil {
		return reconcile.Result{}, errors.Wrap(err, "failed to update status")
	}
	return reconcile.Result{}, reconcileErr
}

func (r *SnapshotReconciler) reconcile(ctx context.Context, snapshot *PoolSnapshot) (SnapshotState, string, error) {
	if snapshot.Status != nil {
		switch snapshot.Status.State {
		case SnapshotCompleted, SnapshotFailed:
			return snapshot.Status.State, snapshot.Status.Reason, nil
		case SnapshotError:
			if snapshot.Spec.Deadline.Time.Before(r.timeNow()) {
				return SnapshotFailed, "did not create snapshot before deadline: " + snapshot.Status.Reason, nil
			}
		case SnapshotPending:
			if snapshot.Spec.Deadline.Time.Before(r.timeNow()) {
				return SnapshotFailed, "did not create snapshot before deadline", nil
			}
		}
	}

	var pool Pool
	if err := r.client.Get(ctx, client.ObjectKey{Name: snapshot.Spec.Pool.Name, Namespace: snapshot.Namespace}, &pool); err != nil {
		return "", "", err
	}
	var state SnapshotState
	var reason string
	_, err := pool.WithSession(ctx, r.client, func(sshSession *ssh.Session) error {
		var err error
		state, reason, err = r.reconcileWithSSH(ctx, pool, snapshot, sshSession)
		return err
	})
	return state, reason, err
}

func (r *SnapshotReconciler) reconcileWithSSH(ctx context.Context, pool Pool, snapshot *PoolSnapshot, sshSession *ssh.Session) (SnapshotState, string, error) {
	if len(snapshot.Spec.Datasets) == 0 {
		return "", "", errors.New(".spec.datasets must specify at least 1 dataset")
	}
	snapshot.Status = &SnapshotStatus{State: SnapshotPending}
	if err := r.client.Status().Update(ctx, snapshot); err != nil {
		return "", "", err
	}

	var recursiveDatasets, singularDatasets []string
	for _, dataset := range snapshot.Spec.Datasets {
		if !pool.validDatasetName(dataset.Name) {
			return "", "", errors.Errorf("invalid dataset selector name %q: name must start with pool name %s", dataset.Name, pool.Spec.Name)
		}
		if dataset.Recursive == nil {
			singularDatasets = append(singularDatasets, dataset.Name)
		} else {
			recursiveDatasets = append(recursiveDatasets, dataset.Name)
			// TODO handle skipChildren
		}
	}
	if len(recursiveDatasets) > 0 {
		name, args := snapshotCommand(recursiveDatasets, snapshot.Name, true)
		command := safelyFormatCommand(name, args...)
		output, err := sshSession.CombinedOutput(command)
		output = bytes.TrimSpace(output)
		if err != nil {
			return "", "", errors.Wrapf(err, `failed to run '%s': %s`, command, string(output))
		}
	}
	if len(singularDatasets) > 0 {
		name, args := snapshotCommand(singularDatasets, snapshot.Name, false)
		command := safelyFormatCommand(name, args...)
		output, err := sshSession.CombinedOutput(command)
		output = bytes.TrimSpace(output)
		if err != nil {
			return "", "", errors.Wrapf(err, `failed to run '%s': %s`, command, string(output))
		}
	}

	return SnapshotCompleted, "", nil
}

func snapshotCommand(datasets []string, snapshotName string, recursive bool) (name string, args []string) {
	name = "/usr/sbin/zfs"
	args = []string{"snapshot"}
	if recursive {
		args = append(args, "-r")
	}
	for _, dataset := range datasets {
		args = append(args, fmt.Sprintf("%s@%s", dataset, snapshotName))
	}
	return
}
