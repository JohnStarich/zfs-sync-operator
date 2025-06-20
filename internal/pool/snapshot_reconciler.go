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

	var pool Pool
	if err := r.client.Get(ctx, client.ObjectKey{Name: snapshot.Spec.Pool.Name, Namespace: request.Namespace}, &pool); err != nil {
		return reconcile.Result{}, err
	}
	var state SnapshotState
	var reason string
	_, reconcileErr := pool.WithSession(ctx, r.client, func(sshSession *ssh.Session) error {
		var err error
		state, reason, err = r.reconcile(ctx, &snapshot, sshSession)
		return err
	})
	result := reconcile.Result{}
	if reconcileErr == nil {
		logger.Info("pool reconciled successfully", "state", state)
		snapshot.Status = &SnapshotStatus{
			State:  state,
			Reason: reason,
		}
	} else {
		logger.Error(reconcileErr, "reconcile failed")
		const retryErrorWait = 2 * time.Minute
		result.RequeueAfter = retryErrorWait
		pool.Status = &Status{
			State:  Error,
			Reason: reconcileErr.Error(),
		}
	}
	statusErr := r.client.Status().Update(ctx, &snapshot)
	return result, errors.Wrap(statusErr, "failed to update status")
}

func (r *SnapshotReconciler) reconcile(ctx context.Context, snapshot *PoolSnapshot, sshSession *ssh.Session) (SnapshotState, string, error) {
	if len(snapshot.Spec.Datasets) == 0 {
		return "", "", errors.New(".spec.datasets must specify at least 1 dataset")
	}
	snapshot.Status = &SnapshotStatus{State: SnapshotPending}
	if err := r.client.Status().Update(ctx, snapshot); err != nil {
		return "", "", err
	}

	var recursiveDatasets, singularDatasets []string
	for _, dataset := range snapshot.Spec.Datasets {
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
