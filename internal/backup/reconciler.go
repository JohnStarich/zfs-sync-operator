package backup

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/johnstarich/go/datasize"
	"github.com/johnstarich/zfs-sync-operator/internal/iocount"
	"github.com/johnstarich/zfs-sync-operator/internal/metrics"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	client        client.Client
	sentBytes     *prometheus.CounterVec
	sentSnapshots *prometheus.CounterVec
	stateGauge    *prometheus.GaugeVec
}

const (
	sourceProperty      = ".spec.source.name"
	destinationProperty = ".spec.destination.name"
)

// RegisterReconciler registers a Backup reconciler with manager
func RegisterReconciler(ctx context.Context, manager manager.Manager, metricsRegistry prometheus.Registerer) error {
	const (
		backupSubsystem = "backup"
	)
	reconciler := &Reconciler{
		client: manager.GetClient(),
		sentBytes: metrics.MustRegister(metricsRegistry, prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: name.Metrics,
			Subsystem: backupSubsystem,
			Name:      "sent_bytes",
			Help:      "The number of successfully sent bytes across all snapshots",
		}, []string{
			metrics.NameLabel,
			metrics.NamespaceLabel,
		})),
		sentSnapshots: metrics.MustRegister(metricsRegistry, prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: name.Metrics,
			Subsystem: backupSubsystem,
			Name:      "sent_snapshots",
			Help:      "The number of successfully sent snapshots",
		}, []string{
			metrics.NameLabel,
			metrics.NamespaceLabel,
		})),
		stateGauge: metrics.MustRegister(metricsRegistry, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: name.Metrics,
			Subsystem: backupSubsystem,
			Name:      "state",
			Help:      "The status.state of each backup",
		}, []string{
			metrics.NameLabel,
			metrics.NamespaceLabel,
			metrics.StateLabel,
		})),
	}

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

	if err := ctrl.Watch(source.Kind( // Watch pool status for source/destination health
		manager.GetCache(),
		&pool.Pool{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, pool *pool.Pool) []reconcile.Request {
			logger := log.FromContext(ctx)
			allBackups, err := reconciler.matchingBackups(ctx, pool.Name, pool.Namespace)
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

	if err := ctrl.Watch(source.Kind( // Watch snapshot status to start sending
		manager.GetCache(),
		&pool.PoolSnapshot{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, snapshot *pool.PoolSnapshot) []reconcile.Request {
			logger := log.FromContext(ctx)
			allBackups, err := reconciler.matchingBackups(ctx, snapshot.Spec.Pool.Name, snapshot.Namespace)
			if err != nil {
				logger.Error(err, "Failed to find matching backups for snapshot", "snapshot", snapshot.Name, "namespace", snapshot.Namespace)
				return nil
			}

			var requests []reconcile.Request
			for _, backup := range allBackups {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name:      backup.Name,
						Namespace: snapshot.Namespace,
					},
				})
			}
			logger.Info("Received snapshot update", "matching backups", len(requests))
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
	var backup *Backup
	{
		var getBackup Backup
		if err := r.client.Get(ctx, request.NamespacedName, &getBackup); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		} else if err == nil {
			logger.Info("got backup", "spec", getBackup.Spec, "status", getBackup.Status)
			if getBackup.Status == nil { // guard against nil status
				getBackup.Status = &Status{}
			}
			backup = &getBackup
		}
	}

	reconcileErr := r.reconcile(ctx, backup)

	backupStateGauge := r.stateGauge.MustCurryWith(prometheus.Labels{
		metrics.NameLabel:      request.Name,
		metrics.NamespaceLabel: request.Namespace,
	})
	for state := range AllStates() {
		backupStateGauge.With(prometheus.Labels{metrics.StateLabel: state.String()}).Set(metrics.CountTrue(backup != nil && state == backup.Status.State))
	}
	return reconcile.Result{}, reconcileErr
}

func (r *Reconciler) reconcile(ctx context.Context, backup *Backup) error {
	if backup == nil { // deleted
		return nil
	}
	logger := log.FromContext(ctx)
	var reconcileErr error
	backup.Status.State, reconcileErr = r.reconcileValid(ctx, backup)
	backup.Status.Reason = ""

	var returnErr error
	if backup.Status.State == UnexpectedError {
		backup.Status.State = Error
		returnErr = reconcileErr
	}
	if reconcileErr != nil {
		logger.Error(reconcileErr, "reconcile failed")
		backup.Status.Reason = reconcileErr.Error()
	} else {
		logger.Info("backup reconciled successfully", "status", backup.Status)
	}

	if err := r.client.Status().Update(ctx, backup); err != nil {
		return err
	}
	return returnErr
}

func (r *Reconciler) reconcileValid(ctx context.Context, backup *Backup) (State, error) {
	logger := log.FromContext(ctx)
	var source, destination pool.Pool
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Source.Name}, &source); err != nil {
		return UnexpectedError, errors.WithMessage(err, "get source Pool")
	}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Destination.Name}, &destination); err != nil {
		return UnexpectedError, errors.WithMessage(err, "get destination Pool")
	}

	if err := validatePoolIsHealthy("source", source); err != nil {
		return NotReady, err
	}
	if source.Spec == nil || source.Spec.Snapshots == nil {
		return NotReady, errors.Errorf("source pool %q must define .spec.snapshots, either empty (the default schedule) or custom intervals", source.Name)
	}
	if err := validatePoolIsHealthy("destination", destination); err != nil {
		return NotReady, err
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
		return UnexpectedError, err
	}
	if err := pool.SortScheduledSnapshots(completedSnapshots.Items); err != nil {
		return UnexpectedError, err
	}
	sendSnapshots := completedSnapshots.Items
	logger.Info("Found backup-ready snapshots", "snapshots", len(sendSnapshots))
	if backup.Status != nil && backup.Status.LastSentSnapshot != nil {
		lastSentSnapshotIndex := slices.IndexFunc(sendSnapshots, func(snapshot *pool.PoolSnapshot) bool {
			return snapshot.Name == backup.Status.LastSentSnapshot.Name
		})
		if lastSentSnapshotIndex != -1 {
			sendSnapshots = sendSnapshots[lastSentSnapshotIndex+1:]
			logger.Info("Found last sent snapshot, using incremental send", "last", backup.Status.LastSentSnapshot.Name, "snapshots", len(sendSnapshots))
		}
	}
	if len(sendSnapshots) == 0 {
		logger.Info("All snapshots have already been sent, done!")
		return Ready, nil
	}
	sendLastSnapshot := sendSnapshots[len(sendSnapshots)-1]
	logger.Info("Sending", "snapshot", sendLastSnapshot.Name)

	err = source.WithConnection(ctx, r.client, func(sourceConn *pool.Connection) error {
		return destination.WithConnection(ctx, r.client, func(destinationConn *pool.Connection) error {
			return r.sendPoolSnapshot(ctx, backup, source, destination, sourceConn, destinationConn, sendLastSnapshot)
		})
	})
	if err != nil {
		return UnexpectedError, err
	}

	return Ready, nil
}

func (r *Reconciler) matchingBackups(ctx context.Context, poolName, poolNamespace string) ([]Backup, error) {
	var allBackups []Backup
	{
		// Fetch all backups with this pool as their source.
		var sourceBackups BackupList
		err := r.client.List(ctx, &sourceBackups, &client.ListOptions{
			FieldSelector: fields.OneTermEqualSelector(sourceProperty, poolName),
			Namespace:     poolNamespace,
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
			FieldSelector: fields.OneTermEqualSelector(destinationProperty, poolName),
			Namespace:     poolNamespace,
		})
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to list matching destination backups")
		}
		allBackups = append(allBackups, destinationBackups.Items...)
	}
	return allBackups, nil
}

func (r *Reconciler) sendPoolSnapshot(ctx context.Context, backup *Backup, sourcePool, destinationPool pool.Pool, sourceConn, destinationConn *pool.Connection, snapshot *pool.PoolSnapshot) error {
	if snapshot.Status == nil || snapshot.Status.DatasetNames == nil {
		return errors.Errorf("snapshot %q status does not contain snapshotted dataset names to send: %+v", snapshot.Name, snapshot.Status)
	}
	for _, datasetName := range *snapshot.Status.DatasetNames {
		err := r.sendDatasetSnapshot(ctx, backup, sourcePool, destinationPool, sourceConn, destinationConn, datasetName, snapshot.Name)
		if err != nil {
			return err
		}
	}
	r.sentSnapshots.With(prometheus.Labels{
		metrics.NameLabel:      backup.Name,
		metrics.NamespaceLabel: backup.Namespace,
	}).Inc()
	backup.Status.InProgressSnapshot = nil
	backup.Status.LastSentSnapshot = &corev1.LocalObjectReference{Name: snapshot.Name}
	return r.client.Status().Update(ctx, backup)
}

func (r *Reconciler) sendDatasetSnapshot(ctx context.Context, backup *Backup, sourcePool, destinationPool pool.Pool, sourceConn, destinationConn *pool.Connection, sourceDatasetName, snapshotName string) error {
	logger := log.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	const datasetSeparator = "/"
	sourcePrefix := sourcePool.Spec.Name + datasetSeparator
	if !strings.HasPrefix(sourceDatasetName, sourcePrefix) {
		return errors.Errorf("unexpected dataset name %q must begin with source pool's name %q", sourceDatasetName, sourcePrefix)
	}
	destinationDatasetName := destinationPool.Spec.Name + datasetSeparator + strings.TrimPrefix(sourceDatasetName, sourcePrefix)

	sendArgs := []string{"/usr/bin/sudo", "/usr/sbin/zfs", "send"}
	var receiveResumeToken *string
	if backup.Status.InProgressSnapshot != nil && backup.Status.InProgressSnapshot.Name == snapshotName {
		const zfsGetUnsetValue = "-"
		tokenBytes, err := destinationConn.ExecCombinedOutput(ctx, "/usr/sbin/zfs", "get",
			"-H",          // Skip table headers
			"-o", "value", // Only return the token's value
			"receive_resume_token", // Only return the token
			destinationDatasetName,
		)
		if err != nil {
			logger.Error(err, "Failed to retrieve receive_resume_token, sending from scratch...")
		} else if token := string(tokenBytes); token != zfsGetUnsetValue {
			receiveResumeToken = &token
		} else {
			logger.Info("No receive_resume_token available for in progress send, sending from scratch...")
		}
	}

	fullSnapshotName := fmt.Sprintf("%s@%s", sourceDatasetName, snapshotName)
	if receiveResumeToken == nil { // If we're not resuming an interrupted send, create a normal send stream with all options
		sendArgs = append(sendArgs,
			"--raw",       // Always send raw streams. Must be enabled to avoid decrypting and sending data in the clear.
			"--replicate", // Copies all data contained in the snapshot - properties, snapshots, descendent file systems, and clones are preserved.
		)
		if backup.Status.LastSentSnapshot != nil { // Use incremental send after first send
			sendArgs = append(sendArgs, "-I", "@"+backup.Status.LastSentSnapshot.Name)
		}
		sendArgs = append(sendArgs, fullSnapshotName)
	} else {
		sendArgs = append(sendArgs, "-t", *receiveResumeToken) // Attempt to resume an interrupted send via a "receive_resume_token"
	}

	receiveArgs := []string{
		"/usr/bin/sudo", "/usr/sbin/zfs", "receive",
		"-d", // Preserve hierarchy when using recursive replicated streams
		"-s", // Enable send to resume later if interrupted
		destinationPool.Spec.Name,
	}
	logger.Info("Syncing", "sendCommand", sendArgs, "receiveCommand", receiveArgs)

	const maxExpectedErrors = 2
	errs := make(chan error, maxExpectedErrors)
	pipeReader, pipeWriter := io.Pipe()
	countWriter := iocount.NewWriter(pipeWriter)
	go func() {
		errs <- sourceConn.ExecWriteStdout(ctx, countWriter, sendArgs[0], sendArgs[1:]...)
	}()
	go func() {
		errs <- destinationConn.ExecReadStdin(ctx, pipeReader, receiveArgs[0], receiveArgs[1:]...)
	}()
	go func() {
		sentBytes := r.sentBytes.With(prometheus.Labels{
			metrics.NameLabel:      backup.Name,
			metrics.NamespaceLabel: backup.Namespace,
		})
		const statusUpdateInterval = 30 * time.Second
		lastCount := countWriter.Count()
		ticker := time.NewTicker(statusUpdateInterval)
		for {
			status := Status{
				State: Sending,
			}
			newCount := countWriter.Count()
			delta := newCount - lastCount
			sentBytes.Add(float64(delta))
			sentValue, sentUnit := datasize.Bytes(newCount).FormatIEC()
			rate := delta / int64(statusUpdateInterval/time.Second)
			rateValue, rateUnit := datasize.Bytes(rate).FormatIEC()
			status.Reason = fmt.Sprintf("sending %s: sent %.1f %s (%.0f %s/s)", fullSnapshotName, sentValue, sentUnit, rateValue, rateUnit)
			if newCount > 0 {
				status.InProgressSnapshot = &corev1.LocalObjectReference{Name: snapshotName}
			}

			lastCount = newCount
			if err := r.patchStatus(ctx, backup.Namespace, backup.Name, status); err != nil {
				logger.Error(err, "Failed to patch status for Sending")
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	for range maxExpectedErrors {
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
		return errors.New(p.Status.State.String())
	default:
		return errors.Errorf("%s: %s", p.Status.State, p.Status.Reason)
	}
}

func (r *Reconciler) patchStatus(ctx context.Context, namespace, name string, status Status) error {
	return r.client.Status().Patch(ctx, &Backup{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status:     &status,
	}, client.Merge)
}
