package pool

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/clock"
	"github.com/johnstarich/zfs-sync-operator/internal/idgen"
	"github.com/johnstarich/zfs-sync-operator/internal/metrics"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	clock          clock.Clock
	maxSessionWait time.Duration
	stateGauge     *prometheus.GaugeVec
	uuidGenerator  idgen.IDGenerator
}

// RegisterReconciler registers a Pool reconciler with manager
func RegisterReconciler(ctx context.Context, manager manager.Manager, metricsRegistry prometheus.Registerer, maxSessionWait time.Duration, clock clock.Clock, uuidGenerator idgen.IDGenerator) error {
	const (
		poolSubsystem = "pool"
	)
	ctrl, err := controller.New("pool", manager, controller.Options{
		Reconciler: &Reconciler{
			client:         manager.GetClient(),
			clock:          clock,
			maxSessionWait: maxSessionWait,
			stateGauge: metrics.MustRegister(metricsRegistry, prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: name.Metrics,
				Subsystem: poolSubsystem,
				Name:      "state",
				Help:      "The status.state of each pool",
			}, []string{
				metrics.NameLabel,
				metrics.NamespaceLabel,
				metrics.StateLabel,
			})),
			uuidGenerator: uuidGenerator,
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

	return registerSnapshotReconciler(ctx, manager, clock)
}

// Reconcile implements [reconcile.Reconciler]
func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking request", "request", request)
	var pool *Pool
	{
		var getPool Pool
		if err := r.client.Get(ctx, request.NamespacedName, &getPool); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		} else if err == nil {
			logger.Info("got pool", "pool", getPool.Spec, "generation", getPool.Generation)
			pool = &getPool
		}
	}
	reconcileResult, reconcileErr := r.reconcile(ctx, pool)

	poolStateGauge := r.stateGauge.MustCurryWith(prometheus.Labels{
		metrics.NameLabel:      request.Name,
		metrics.NamespaceLabel: request.Namespace,
	})
	for state := range AllStates() {
		poolStateGauge.With(prometheus.Labels{metrics.StateLabel: state.String()}).Set(metrics.CountTrue(pool != nil && state == pool.Status.State))
	}
	return reconcileResult, reconcileErr
}

func (r *Reconciler) reconcile(ctx context.Context, pool *Pool) (reconcile.Result, error) {
	if pool == nil { // deleted
		return reconcile.Result{}, nil
	}

	logger := log.FromContext(ctx)
	requeueAfter, reconcileErr := r.reconcileWithTimeout(ctx, pool, r.maxSessionWait)
	if reconcileErr != nil {
		logger.Error(reconcileErr, "reconcile failed")
		pool.Status = &Status{
			State:  Error,
			Reason: reconcileErr.Error(),
		}
		if err := r.client.Status().Update(ctx, pool); err != nil {
			return reconcile.Result{}, errors.Wrap(err, "failed to update status")
		}
		return reconcile.Result{}, reconcileErr
	}
	logger.Info("pool reconciled successfully", "state", pool.Status.State)
	return reconcile.Result{
		RequeueAfter: requeueAfter,
	}, nil
}

func (r *Reconciler) reconcileWithTimeout(ctx context.Context, pool *Pool, timeout time.Duration) (requeueAfter time.Duration, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	reconcileErr := pool.WithConnection(ctx, r.client, func(connection *Connection) error {
		var err error
		requeueAfter, err = r.reconcileWithConnection(ctx, pool, connection)
		return err
	})
	return requeueAfter, reconcileErr
}

func (r *Reconciler) reconcileWithConnection(ctx context.Context, pool *Pool, connection *Connection) (requeueAfter time.Duration, err error) {
	logger := log.FromContext(ctx)
	zpoolStatus, err := connection.ExecCombinedOutput(ctx, "/usr/sbin/zpool", "status", pool.Spec.Name)
	if err != nil {
		if bytes.HasSuffix(zpoolStatus, []byte(": no such pool")) {
			pool.Status = &Status{
				State:  NotFound,
				Reason: fmt.Sprintf("zpool with name '%s' could not be found", pool.Spec.Name),
			}
			return 0, r.client.Status().Update(ctx, pool)
		}
		return 0, err
	}
	pool.Status = &Status{
		State: stateFromStateField(stateFieldFromZpoolStatus(zpoolStatus)),
	}
	if err := r.client.Status().Update(ctx, pool); err != nil {
		return 0, err
	}

	if pool.Spec.Snapshots != nil {
		intervals, err := pool.Spec.Snapshots.validateIntervals()
		if err != nil {
			return 0, err
		}
		now := r.clock.Now()
		requeueAfter = 0 * time.Second
		for _, interval := range intervals {
			wait, err := r.reconcileSnapshotInterval(ctx, now, pool, interval)
			if err != nil {
				return 0, errors.WithMessagef(err, "failed reconciling interval %s", interval.Name)
			}
			logger.Info("Interval's next wait time", "wait", wait)
			if requeueAfter == 0 || (wait != 0 && wait < requeueAfter) {
				requeueAfter = wait
			}
		}
		logger.Info("Requeuing to handle next interval", "requeueAfter", requeueAfter)
	}
	return requeueAfter, nil
}

func (r *Reconciler) reconcileSnapshotInterval(ctx context.Context, now time.Time, pool *Pool, interval SnapshotIntervalSpec) (timeUntilDeadline time.Duration, err error) {
	logger := log.FromContext(ctx)

	snapshots, err := r.intervalSnapshotsByState(ctx, pool, interval)
	if err != nil {
		return 0, err
	}

	if len(snapshots[SnapshotPending])+len(snapshots[SnapshotError]) == 0 {
		nextTime, nextTimeIsInThePast, err := nextSnapshot(ctx, now, interval.Interval.Duration, snapshots[SnapshotCompleted])
		if err != nil {
			return 0, err
		}
		timeUntilDeadline = nextTime.Sub(now)
		if nextTimeIsInThePast {
			deadline := nextTime.Add(interval.Interval.Duration)
			snapshot := PoolSnapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:        fmt.Sprintf("%s-%s", interval.Name, r.uuidGenerator.MustNewID()),
					Namespace:   pool.Namespace,
					Labels:      map[string]string{snapshotIntervalLabel: interval.Name},
					Annotations: map[string]string{snapshotTimestampAnnotation: nextTime.Format(time.RFC3339)},
				},
				Spec: SnapshotSpec{
					Pool:                 corev1.LocalObjectReference{Name: pool.Name},
					Deadline:             &metav1.Time{Time: deadline},
					SnapshotSpecTemplate: pool.Spec.Snapshots.Template,
				},
			}
			if err := controllerutil.SetControllerReference(pool, &snapshot, r.client.Scheme()); err != nil {
				return 0, err
			}
			if err := r.client.Create(ctx, &snapshot); err != nil {
				return 0, err
			}
			timeUntilDeadline = deadline.Sub(now)
		}
	}
	const maxFailedHistory = 1
	if len(snapshots[SnapshotFailed]) > maxFailedHistory {
		expired := dropRight(snapshots[SnapshotFailed], maxFailedHistory)
		logger.Info("Cleaning up oldest failed snapshots", "failed", len(expired), "maxFailedHistory", maxFailedHistory)
		for _, expiredSnapshot := range expired {
			if err := r.client.Delete(ctx, expiredSnapshot); err != nil {
				return 0, err
			}
		}
	}
	if unsignedLen(snapshots[SnapshotCompleted]) > interval.HistoryLimit {
		expired := dropRight(snapshots[SnapshotCompleted], interval.HistoryLimit)
		logger.Info("Cleaning up snapshots older than maximum history", "expired", len(expired), "maxHistory", interval.HistoryLimit)
		for _, expiredSnapshot := range expired {
			if err := r.client.Delete(ctx, expiredSnapshot); err != nil {
				return 0, err
			}
		}
	}
	return timeUntilDeadline, nil
}

func (r *Reconciler) intervalSnapshotsByState(ctx context.Context, pool *Pool, interval SnapshotIntervalSpec) (map[SnapshotState][]*PoolSnapshot, error) {
	logger := log.FromContext(ctx)

	var allSnapshots PoolSnapshotList
	err := r.client.List(ctx, &allSnapshots, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{snapshotIntervalLabel: interval.Name}),
		FieldSelector: fields.AndSelectors(
			fields.OneTermEqualSelector(".spec.pool.name", pool.Name),
		),
		Namespace: pool.Namespace,
	})
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to retrieve PoolSnapshots for pool %s on interval %s", pool.Name, interval.Name)
	}
	if err := SortScheduledSnapshots(allSnapshots.Items); err != nil {
		return nil, err
	}

	snapshotsByState := make(map[SnapshotState][]*PoolSnapshot)
	nilStatus := 0
	for _, snapshot := range allSnapshots.Items {
		var state SnapshotState
		if snapshot.Status != nil {
			state = snapshot.Status.State
		} else {
			state = SnapshotPending
			nilStatus++
		}
		snapshotsByState[state] = append(snapshotsByState[state], snapshot)
	}
	logger.Info("Looked up connected snapshots",
		"nil", nilStatus,
		"pending", len(snapshotsByState[SnapshotPending]),
		"error", len(snapshotsByState[SnapshotError]),
		"completed", len(snapshotsByState[SnapshotCompleted]),
		"failed", len(snapshotsByState[SnapshotFailed]),
	)
	return snapshotsByState, nil
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
