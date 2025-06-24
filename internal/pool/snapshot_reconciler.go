package pool

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
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

// SnapshotReconciler reconciles PoolSnapshot resources to create a set of ZFS snapshots across the whole pool
type SnapshotReconciler struct {
	client  client.Client
	timeNow func() time.Time
}

const (
	poolNameProperty         = ".spec.pool.name"
	snapshotDestroyFinalizer = name.DomainPrefix + "zfs-destroy-snapshot"
)

// registerSnapshotReconciler registers a PoolSnapshot reconciler with manager
func registerSnapshotReconciler(ctx context.Context, manager manager.Manager, timeNow func() time.Time) error {
	reconciler := &SnapshotReconciler{
		client:  manager.GetClient(),
		timeNow: timeNow,
	}

	ctrl, err := controller.New("poolsnapshot", manager, controller.Options{
		Reconciler: reconciler,
	})
	if err != nil {
		return err
	}

	err = manager.GetFieldIndexer().IndexField(ctx, &PoolSnapshot{}, poolNameProperty, func(o client.Object) []string {
		return []string{o.(*PoolSnapshot).Spec.Pool.Name}
	})
	if err != nil {
		return errors.WithMessagef(err, "failed to index PoolSnapshot %s", poolNameProperty)
	}
	if err := ctrl.Watch(source.Kind(
		manager.GetCache(),
		&PoolSnapshot{},
		&handler.TypedEnqueueRequestForObject[*PoolSnapshot]{},
		predicate.TypedGenerationChangedPredicate[*PoolSnapshot]{},
	)); err != nil {
		return err
	}

	if err := ctrl.Watch(source.Kind(
		manager.GetCache(),
		&Pool{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, pool *Pool) []reconcile.Request {
			logger := log.FromContext(ctx)
			allSnapshots, err := reconciler.matchingSnapshots(ctx, pool)
			if err != nil {
				logger.Error(err, "Failed to find matching snapshots for pool", "pool", pool.Name, "namespace", pool.Namespace)
				return nil
			}

			var requests []reconcile.Request
			for _, snapshot := range allSnapshots {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name:      snapshot.Name,
						Namespace: pool.Namespace,
					},
				})
			}
			logger.Info("Received pool update", "matching snapshots", len(requests))
			return requests
		}),
	)); err != nil {
		return err
	}

	return nil
}

// Reconcile implements [reconcile.Reconciler]
func (r *SnapshotReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking request", "request", request)
	var snapshot PoolSnapshot
	if err := r.client.Get(ctx, request.NamespacedName, &snapshot); err != nil {
		return reconcile.Result{}, err
	}
	state, reason, reconcileErr := r.reconcile(ctx, &snapshot)
	if reconcileErr == nil {
		logger.Info("poolsnapshot reconciled successfully", "state", state)
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
	if snapshot.Status == nil { // Guard against nil status
		snapshot.Status = &SnapshotStatus{State: SnapshotPending}
	}
	if snapshot.DeletionTimestamp == nil && snapshot.Status.State == SnapshotCompleted || snapshot.Status.State == SnapshotFailed {
		return snapshot.Status.State, snapshot.Status.Reason, nil
	}

	var pool Pool
	if err := r.client.Get(ctx, client.ObjectKey{Name: snapshot.Spec.Pool.Name, Namespace: snapshot.Namespace}, &pool); err != nil {
		return "", "", err
	}
	if pool.Status == nil {
		return SnapshotError, "pool is not ready", nil
	}
	if pool.Status.State != Online {
		message := string(pool.Status.State)
		if pool.Status.Reason != "" {
			message = fmt.Sprintf("%s: %s", pool.Status.State, pool.Status.Reason)
		}
		return "", "", errors.Errorf("pool is unhealthy: %s", message)
	}

	var state SnapshotState
	var reason string
	err := pool.WithConnection(ctx, r.client, func(sshClient *ssh.Client) error {
		var err error
		state, reason, err = r.reconcileWithSSH(ctx, pool, snapshot, sshClient)
		return err
	})
	return state, reason, err
}

func (r *SnapshotReconciler) reconcileWithSSH(ctx context.Context, pool Pool, snapshot *PoolSnapshot, sshClient *ssh.Client) (SnapshotState, string, error) {
	if len(snapshot.Spec.Datasets) == 0 {
		return "", "", errors.New(".spec.datasets must specify at least 1 dataset")
	}
	var recursiveDatasets, singularDatasets []string // Prepare snapshot's matching datasets
	for _, dataset := range snapshot.Spec.Datasets {
		if !pool.validDatasetName(dataset.Name) {
			return "", "", errors.Errorf("invalid dataset selector name %q: name must start with pool name %s", dataset.Name, pool.Spec.Name)
		}
		switch {
		case dataset.Recursive == nil:
			singularDatasets = append(singularDatasets, dataset.Name)
		case len(dataset.Recursive.SkipChildren) > 0:
			childDatasets, err := r.childDatasets(dataset.Name, sshClient)
			if err != nil {
				return "", "", errors.WithMessage(err, "failed to fetch child datasets")
			}
			for _, child := range childDatasets {
				if !slices.Contains(dataset.Recursive.SkipChildren, child) {
					recursiveDatasets = append(recursiveDatasets, child)
				}
			}
		default:
			recursiveDatasets = append(recursiveDatasets, dataset.Name)
		}
	}

	if snapshot.DeletionTimestamp != nil {
		if !slices.Contains(snapshot.Finalizers, snapshotDestroyFinalizer) {
			return snapshot.Status.State, snapshot.Status.Reason, nil
		}
		if len(recursiveDatasets) > 0 {
			name, args := destroySnapshotCommand(recursiveDatasets, snapshot.Name, true)
			command := safelyFormatCommand(name, args...)
			sshSession, err := sshClient.NewSession()
			if err != nil {
				return "", "", errors.WithMessage(err, "failed to start ssh session")
			}
			output, err := sshSession.CombinedOutput(command)
			output = bytes.TrimSpace(output)
			if err != nil {
				return "", "", errors.Wrapf(err, `failed to run '%s': %s`, command, string(output))
			}
		}
		if len(singularDatasets) > 0 {
			name, args := destroySnapshotCommand(singularDatasets, snapshot.Name, false)
			command := safelyFormatCommand(name, args...)
			sshSession, err := sshClient.NewSession()
			if err != nil {
				return "", "", errors.WithMessage(err, "failed to start ssh session")
			}
			output, err := sshSession.CombinedOutput(command)
			output = bytes.TrimSpace(output)
			if err != nil {
				return "", "", errors.Wrapf(err, `failed to run '%s': %s`, command, string(output))
			}
		}

		snapshot.Finalizers = slices.DeleteFunc(snapshot.Finalizers, func(s string) bool { return s == snapshotDestroyFinalizer })
		return snapshot.Status.State, snapshot.Status.Reason, r.client.Update(ctx, snapshot)
	}

	if snapshot.Spec.Deadline != nil && snapshot.Spec.Deadline.Time.Before(r.timeNow()) {
		return SnapshotFailed, "did not create snapshot before deadline", nil
	}

	snapshot.Status = &SnapshotStatus{State: SnapshotPending}
	if err := r.client.Status().Update(ctx, snapshot); err != nil {
		return "", "", err
	}
	if !slices.Contains(snapshot.Finalizers, snapshotDestroyFinalizer) {
		snapshot.Finalizers = append(snapshot.Finalizers, snapshotDestroyFinalizer)
		if err := r.client.Update(ctx, snapshot); err != nil {
			return "", "", err
		}
	}

	if len(recursiveDatasets) > 0 {
		name, args := createSnapshotCommand(recursiveDatasets, snapshot.Name, true)
		command := safelyFormatCommand(name, args...)
		sshSession, err := sshClient.NewSession()
		if err != nil {
			return "", "", errors.WithMessage(err, "failed to start ssh session")
		}
		output, err := sshSession.CombinedOutput(command)
		output = bytes.TrimSpace(output)
		if err != nil {
			return "", "", errors.Wrapf(err, `failed to run '%s': %s`, command, string(output))
		}
	}
	if len(singularDatasets) > 0 {
		name, args := createSnapshotCommand(singularDatasets, snapshot.Name, false)
		command := safelyFormatCommand(name, args...)
		sshSession, err := sshClient.NewSession()
		if err != nil {
			return "", "", errors.WithMessage(err, "failed to start ssh session")
		}
		output, err := sshSession.CombinedOutput(command)
		output = bytes.TrimSpace(output)
		if err != nil {
			return "", "", errors.Wrapf(err, `failed to run '%s': %s`, command, string(output))
		}
	}

	return SnapshotCompleted, "", nil
}

func createSnapshotCommand(datasets []string, snapshotName string, recursive bool) (name string, args []string) {
	return "/usr/sbin/zfs", append([]string{"snapshot"}, snapshotArgs(datasets, snapshotName, recursive)...)
}

func destroySnapshotCommand(datasets []string, snapshotName string, recursive bool) (name string, args []string) {
	return "/usr/sbin/zfs", append([]string{"destroy"}, snapshotArgs(datasets, snapshotName, recursive)...)
}

func snapshotArgs(datasets []string, snapshotName string, recursive bool) (args []string) {
	if recursive {
		args = append(args, "-r")
	}
	for _, dataset := range datasets {
		args = append(args, fmt.Sprintf("%s@%s", dataset, snapshotName))
	}
	return
}

func (r *SnapshotReconciler) matchingSnapshots(ctx context.Context, pool *Pool) ([]*PoolSnapshot, error) {
	var snapshots PoolSnapshotList
	err := r.client.List(ctx, &snapshots, &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(poolNameProperty, pool.Name),
		Namespace:     pool.Namespace,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "failed to list matching pool snapshots")
	}
	return snapshots.Items, nil
}

func (r *SnapshotReconciler) childDatasets(datasetName string, sshClient *ssh.Client) ([]string, error) {
	command := safelyFormatCommand(
		"/usr/sbin/zfs", "get",
		"-H",                      // Machine parseable format
		"-t", "filesystem,volume", // Only snapshottable types
		"-r", "-d", "1", // Recurse with a max depth of 1, only get immediate children
		"-o", "name", // Only output the dataset name column
		"name",      // Only request the name property
		datasetName, // The parent dataset
	)
	sshSession, err := sshClient.NewSession()
	if err != nil {
		return nil, err
	}
	output, err := sshSession.CombinedOutput(command)
	output = bytes.TrimSpace(output)
	outputStr := string(output)
	if err != nil {
		return nil, errors.WithMessage(err, outputStr)
	}
	zfsGetItems := strings.Split(outputStr, "\n")
	zfsGetItems = slices.DeleteFunc(zfsGetItems, func(name string) bool {
		// 'zfs get -r -d 1 datasetname' includes the parent in the returned list. Filter it out.
		return name == datasetName
	})
	return zfsGetItems, nil
}
