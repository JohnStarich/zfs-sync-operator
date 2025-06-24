package pool

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	snapshotIntervalLabel       = name.DomainPrefix + "snapshot-interval-name"
	snapshotTimestampAnnotation = name.DomainPrefix + "snapshot-timestamp"
)

// Reconciler reconciles Pool resources to validate their Pools and associated connections
type Reconciler struct {
	client         client.Client
	maxSessionWait time.Duration
	timeNow        func() time.Time
}

// RegisterReconciler registers a Pool reconciler with manager
func RegisterReconciler(ctx context.Context, manager manager.Manager, maxSessionWait time.Duration, timeNow func() time.Time) error {
	ctrl, err := controller.New("pool", manager, controller.Options{
		Reconciler: &Reconciler{
			client:         manager.GetClient(),
			maxSessionWait: maxSessionWait,
			timeNow:        timeNow,
		},
	})
	if err != nil {
		return err
	}

	if err := ctrl.Watch(source.Kind(
		manager.GetCache(),
		&Pool{},
		&handler.TypedEnqueueRequestForObject[*Pool]{},
		predicate.TypedGenerationChangedPredicate[*Pool]{},
	)); err != nil {
		return err
	}

	if err := ctrl.Watch(source.Kind(
		manager.GetCache(),
		&PoolSnapshot{},
		handler.TypedEnqueueRequestForOwner[*PoolSnapshot](manager.GetScheme(), manager.GetRESTMapper(), &Pool{}),
	)); err != nil {
		return err
	}

	return registerSnapshotReconciler(ctx, manager, timeNow)
}

// Reconcile implements [reconcile.Reconciler]
func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking request", "request", request)
	var pool Pool
	if err := r.client.Get(ctx, request.NamespacedName, &pool); err != nil {
		return reconcile.Result{}, err
	}
	logger.Info("got pool", "pool", pool.Spec, "generation", pool.Generation)
	reconcileErr := r.reconcile(ctx, &pool)
	if reconcileErr != nil {
		logger.Error(reconcileErr, "reconcile failed")
		pool.Status = &Status{
			State:  Error,
			Reason: reconcileErr.Error(),
		}
		if err := r.client.Status().Update(ctx, &pool); err != nil {
			return reconcile.Result{}, errors.Wrap(err, "failed to update status")
		}
		return reconcile.Result{}, reconcileErr
	}
	logger.Info("pool reconciled successfully", "state", pool.Status.State)
	return reconcile.Result{}, nil
}

func (r *Reconciler) reconcile(ctx context.Context, pool *Pool) error {
	ctx, cancel := context.WithTimeout(ctx, r.maxSessionWait)
	defer cancel()
	return pool.WithConnection(ctx, r.client, func(connection *Connection) error {
		return r.reconcileWithConnection(ctx, pool, connection)
	})
}

func (r *Reconciler) reconcileWithConnection(ctx context.Context, pool *Pool, connection *Connection) error {
	logger := log.FromContext(ctx)
	zpoolStatus, err := connection.ExecCombinedOutput(ctx, "/usr/sbin/zpool", "status", pool.Spec.Name)
	if err != nil {
		if bytes.HasSuffix(zpoolStatus, []byte(": no such pool")) {
			pool.Status = &Status{
				State:  NotFound,
				Reason: fmt.Sprintf("zpool with name '%s' could not be found", pool.Spec.Name),
			}
			return r.client.Status().Update(ctx, pool)
		}
		return err
	}
	pool.Status = &Status{
		State: stateFromStateField(stateFieldFromZpoolStatus(zpoolStatus)),
	}
	if err := r.client.Status().Update(ctx, pool); err != nil {
		return err
	}

	if pool.Spec.Snapshots != nil {
		intervals, err := pool.Spec.Snapshots.validateIntervals()
		if err != nil {
			return err
		}
		now := r.timeNow()
		for _, interval := range intervals {
			// TODO set requeue after time for next soonest snapshot
			var allSnapshots PoolSnapshotList
			err := r.client.List(ctx, &allSnapshots, &client.ListOptions{
				LabelSelector: labels.SelectorFromSet(labels.Set{snapshotIntervalLabel: interval.Name}),
				FieldSelector: fields.AndSelectors(
					fields.OneTermEqualSelector(".spec.pool.name", pool.Name),
				),
				Namespace: pool.Namespace,
			})
			if err != nil {
				return errors.WithMessagef(err, "failed to retrieve PoolSnapshots for pool %s on interval %s", pool.Name, interval.Name)
			}
			if err := SortScheduledSnapshots(allSnapshots.Items); err != nil {
				return err
			}

			var completed, pending, failed []*PoolSnapshot
			nilStatus := 0
			for _, snapshot := range allSnapshots.Items {
				var state SnapshotState
				if snapshot.Status != nil {
					state = snapshot.Status.State
				} else {
					state = SnapshotPending
					nilStatus++
				}
				switch state {
				case SnapshotCompleted:
					completed = append(completed, snapshot)
				case SnapshotPending, SnapshotError:
					pending = append(pending, snapshot)
				case SnapshotFailed:
					failed = append(failed, snapshot)
				}
			}
			logger.Info("Looked up connected snapshots", "nil", nilStatus, "pending", len(pending), "completed", len(completed), "failed", len(failed))

			if len(pending) == 0 {
				nextTime, shouldCreateNextSnapshot, err := nextSnapshot(ctx, now, interval.Interval.Duration, completed)
				if err != nil {
					return err
				}
				if shouldCreateNextSnapshot {
					snapshot := PoolSnapshot{
						ObjectMeta: metav1.ObjectMeta{
							GenerateName: interval.Name + "-",
							Namespace:    pool.Namespace,
							Labels:       map[string]string{snapshotIntervalLabel: interval.Name},
							Annotations:  map[string]string{snapshotTimestampAnnotation: nextTime.Format(time.RFC3339)},
						},
						Spec: SnapshotSpec{
							Pool:                 corev1.LocalObjectReference{Name: pool.Name},
							Deadline:             &metav1.Time{Time: nextTime.Add(interval.Interval.Duration)},
							SnapshotSpecTemplate: pool.Spec.Snapshots.Template,
						},
					}
					if err := controllerutil.SetControllerReference(pool, &snapshot, r.client.Scheme()); err != nil {
						return err
					}
					if err := r.client.Create(ctx, &snapshot); err != nil {
						return err
					}
				}
			}
			const maxFailedHistory = 1
			if len(failed) > maxFailedHistory {
				expired := dropRight(failed, maxFailedHistory)
				logger.Info("Cleaning up oldest failed snapshots", "failed", len(expired), "maxFailedHistory", maxFailedHistory)
				for _, expiredSnapshot := range expired {
					if err := r.client.Delete(ctx, expiredSnapshot); err != nil {
						return err
					}
				}
			}
			if unsignedLen(completed) > interval.HistoryLimit {
				expired := dropRight(completed, interval.HistoryLimit)
				logger.Info("Cleaning up snapshots older than maximum history", "expired", len(expired), "maxHistory", interval.HistoryLimit)
				for _, expiredSnapshot := range expired {
					if err := r.client.Delete(ctx, expiredSnapshot); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// stateFieldFromZpoolStatus parses the plain text output of 'zpool status <pool>'.
//
// TODO Parse output with JSON: https://github.com/JohnStarich/zfs-sync-operator/issues/15
func stateFieldFromZpoolStatus(status []byte) string {
	status = bytes.TrimSpace(status) // remove leading blank lines, if any
	scanner := bufio.NewScanner(bytes.NewReader(status))
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Fields(line)
		if len(tokens) != 2 { // stop at break point between fields and vdev list
			break
		}
		field, value := tokens[0], tokens[1]
		if field == "state:" {
			return value
		}
	}
	return ""
}

func dropRight[Value any](values []Value, n uint) []Value {
	if unsignedLen(values) < n {
		return nil
	}
	return values[:unsignedLen(values)-n]
}

func unsignedLen[Value any](values []Value) uint {
	return uint(len(values))
}

// SortScheduledSnapshots sorts snapshots by their scheduled timestamp.
// This only applies to PoolSnapshots created by a Pool's snapshots configuration.
// If any snapshot is encountered with an invalid timestamp, the sort fails.
func SortScheduledSnapshots(snapshots []*PoolSnapshot) error {
	var sortErr error
	slices.SortFunc(snapshots, func(a, b *PoolSnapshot) int {
		annotationA, annotationB := a.Annotations[snapshotTimestampAnnotation], b.Annotations[snapshotTimestampAnnotation]
		timeA, err := time.Parse(time.RFC3339, annotationA)
		if sortErr == nil && err != nil {
			sortErr = errors.WithMessagef(err, "pool snapshot %s", b.Name)
			return 0
		}
		timeB, err := time.Parse(time.RFC3339, annotationB)
		if sortErr == nil && err != nil {
			sortErr = errors.WithMessagef(err, "pool snapshot %s", b.Name)
			return 0
		}
		return timeA.Compare(timeB)
	})
	return sortErr
}

func nextSnapshot(ctx context.Context, now time.Time, interval time.Duration, completedSnapshots []*PoolSnapshot) (time.Time, bool, error) {
	logger := log.FromContext(ctx)
	now = now.UTC()
	if len(completedSnapshots) == 0 {
		nextTime := now.Round(interval).Add(interval)
		logger.Info("No completed snapshots, recommending next snapshot time", "nextTime", nextTime)
		return nextTime, true, nil
	}
	last := completedSnapshots[len(completedSnapshots)-1]
	lastTime, err := time.Parse(time.RFC3339, last.Annotations[snapshotTimestampAnnotation])
	if err != nil {
		return time.Time{}, false, err
	}
	nextTime := lastTime.Add(interval)
	beforeNow := nextTime.Before(now) || nextTime.Equal(now)
	logger.Info("Found completed snapshot, recommending next time after interval", "nextTime", nextTime, "beforeNow", beforeNow)
	return nextTime, beforeNow, nil
}
