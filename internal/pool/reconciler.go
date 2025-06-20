package pool

import (
	"bufio"
	"bytes"
	"context"
	"slices"
	"strings"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	snapshotIntervalLabel       = name.LabelPrefix + "snapshot-interval-name"
	snapshotTimestampAnnotation = name.LabelPrefix + "snapshot-timestamp"
)

// Reconciler reconciles Pool resources to validate their Pools and associated connections
type Reconciler struct {
	client         client.Client
	maxSessionWait time.Duration
	timeNow        func() time.Time
}

// RegisterReconciler registers a Pool reconciler with manager
func RegisterReconciler(manager manager.Manager, maxSessionWait time.Duration, timeNow func() time.Time) error {
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

	return registerSnapshotReconciler(manager)
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
	resourceVersion, results, reconcileErr := r.reconcile(ctx, pool)

	result := reconcile.Result{}
	pool.ResourceVersion = resourceVersion
	if reconcileErr == nil {
		logger.Info("pool reconciled successfully", "state", results.State)
		pool.Status = &Status{
			State:  results.State,
			Reason: results.Reason,
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
	statusErr := r.client.Status().Update(ctx, &pool)
	return result, errors.Wrap(statusErr, "failed to update status")
}

func (r *Reconciler) reconcile(ctx context.Context, pool Pool) (resourceVersion string, results sshResults, returnedErr error) {
	ctx, cancel := context.WithTimeout(ctx, r.maxSessionWait)
	defer cancel()

	resourceVersion, err := pool.WithSession(ctx, r.client, func(session *ssh.Session) error {
		var err error
		results, err = r.reconcileWithSSHSession(ctx, pool, session)
		return err
	})
	return resourceVersion, results, err
}

type sshResults struct {
	State  State
	Reason string
}

func (r *Reconciler) reconcileWithSSHSession(ctx context.Context, pool Pool, session *ssh.Session) (results sshResults, err error) {
	logger := log.FromContext(ctx)
	command := safelyFormatCommand("/usr/sbin/zpool", "status", pool.Spec.Name)
	zpoolStatus, err := session.CombinedOutput(command)
	zpoolStatus = bytes.TrimSpace(zpoolStatus)
	if err != nil {
		if bytes.HasSuffix(zpoolStatus, []byte(": no such pool")) {
			return sshResults{
				State:  NotFound,
				Reason: string(zpoolStatus),
			}, nil
		}
		return results, errors.Wrapf(err, `failed to run '%s': %s`, command, string(zpoolStatus))
	}
	results = sshResults{
		State: stateFromStateField(stateFieldFromZpoolStatus(zpoolStatus)),
	}

	if pool.Spec.Snapshots != nil {
		intervals, err := pool.Spec.Snapshots.validateIntervals()
		if err != nil {
			return results, err
		}
		now := r.timeNow()
		for _, interval := range intervals {
			var completedSnapshots PoolSnapshotList
			err := r.client.List(ctx, &completedSnapshots, &client.ListOptions{
				LabelSelector: labels.SelectorFromSet(labels.Set{snapshotIntervalLabel: interval.Name}),
				FieldSelector: fields.AndSelectors(
					fields.OneTermEqualSelector(".spec.pool.name", pool.Name),
				),
				Namespace: pool.Namespace,
			})
			if err != nil {
				return results, errors.WithMessagef(err, "failed to retrieve PoolSnapshots for pool %s on interval %s", pool.Name, interval.Name)
			}
			if err := SortSnapshotsByDesiredTimestamp(completedSnapshots.Items); err != nil {
				return results, err
			}

			var completed, active, other []*PoolSnapshot
			nilStatus := 0
			for _, snapshot := range completedSnapshots.Items {
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
				case SnapshotPending, SnapshotRunning:
					active = append(active, snapshot)
				default:
					other = append(other, snapshot)
				}
			}
			logger.Info("Looked up connected snapshots", "nil", nilStatus, "completed", len(completed), "active", len(active), "other", len(other))

			nextTime, shouldCreateNextSnapshot, err := nextSnapshot(ctx, now, interval.Interval.Duration, completed)
			if err != nil {
				return results, err
			}
			if len(active) == 0 && shouldCreateNextSnapshot {
				spec := pool.Spec.Snapshots.Template
				spec.Pool.Name = pool.Name
				err := r.client.Create(ctx, &PoolSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						GenerateName: interval.Name + "-",
						Namespace:    pool.Namespace,
						Labels:       map[string]string{snapshotIntervalLabel: interval.Name},
						Annotations:  map[string]string{snapshotTimestampAnnotation: nextTime.Format(time.RFC3339)},
					},
					Spec: spec,
				})
				if err != nil {
					return results, err
				}
			}
			const maxFailedHistory = 1
			if len(other) > maxFailedHistory {
				expired := dropRight(other, maxFailedHistory)
				logger.Info("Cleaning up oldest failed snapshots", "failed", len(expired), "maxFailedHistory", maxFailedHistory)
				for _, expiredSnapshot := range expired {
					if err := r.client.Delete(ctx, expiredSnapshot); err != nil {
						return results, err
					}
				}
			}
			if unsignedLen(completed) > interval.HistoryLimit {
				expired := dropRight(completed, interval.HistoryLimit)
				logger.Info("Cleaning up snapshots older than maximum history", "expired", len(expired), "maxHistory", interval.HistoryLimit)
				for _, expiredSnapshot := range expired {
					if err := r.client.Delete(ctx, expiredSnapshot); err != nil {
						return results, err
					}
				}
			}
		}
	}

	return results, nil
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

func SortSnapshotsByDesiredTimestamp(snapshots []*PoolSnapshot) error {
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
		nextTime := now.Add(interval).Round(interval)
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
