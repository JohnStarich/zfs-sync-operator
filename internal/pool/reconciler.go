package pool

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Reconciler reconciles Pool resources to validate their Pools and associated connections
type Reconciler struct {
	client         ctrlclient.Client
	maxSessionWait time.Duration
}

// RegisterReconciler registers a Pool reconciler with manager
func RegisterReconciler(manager manager.Manager, maxSessionWait time.Duration) error {
	ctrl, err := controller.New("pool", manager, controller.Options{
		Reconciler: &Reconciler{
			client:         manager.GetClient(),
			maxSessionWait: maxSessionWait,
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

	return nil
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
	resourceVersion, state, reason, reconcileErr := r.reconcile(ctx, pool)

	result := reconcile.Result{}
	pool.ResourceVersion = resourceVersion
	if reconcileErr == nil {
		logger.Info("pool reconciled successfully", "state", state)
		pool.Status = &Status{
			State:  state,
			Reason: reason,
		}
	} else {
		logger.Error(reconcileErr, "reconcile failed")
		const retryErrorWait = 2 * time.Minute
		result.RequeueAfter = retryErrorWait
		pool.Status = &Status{
			State:  "Error",
			Reason: reconcileErr.Error(),
		}
	}
	statusErr := r.client.Status().Update(ctx, &pool)
	return result, errors.Wrap(statusErr, "failed to update status")
}

func (r *Reconciler) reconcile(ctx context.Context, pool Pool) (resourceVersion, state, reason string, returnedErr error) {
	ctx, cancel := context.WithTimeout(ctx, r.maxSessionWait)
	defer cancel()

	command := safelyFormatCommand("/usr/sbin/zpool", "status", pool.Spec.Name)
	var zpoolStatus []byte
	resourceVersion, err := pool.WithSession(ctx, r.client, func(session *ssh.Session) error {
		var err error
		zpoolStatus, err = session.CombinedOutput(command)
		zpoolStatus = bytes.TrimSpace(zpoolStatus)
		return errors.Wrapf(err, `failed to run '%s': %s`, command, string(zpoolStatus))
	})
	if err != nil {
		if bytes.HasSuffix(zpoolStatus, []byte(": no such pool")) {
			return resourceVersion, "NotFound", string(zpoolStatus), nil
		}
		return resourceVersion, "", "", err
	}
	stateField := stateFieldFromZpoolStatus(zpoolStatus)
	return resourceVersion, stateFromStateField(stateField), "", nil
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

func stateFromStateField(state string) string {
	// A pool's health status is described by one of three states: online, degraded, or faulted.
	// - https://openzfs.github.io/openzfs-docs/man/v0.8/8/zpool.8.html#Device_Failure_and_Recovery
	switch strings.ToUpper(state) { // should already be in uppercase, but uppercasing defensively
	case "ONLINE", "DEGRADED", "FAULTED":
		return strings.ToUpper(state[0:1]) + strings.ToLower(state[1:])
	default:
		return "Unknown"
	}
}
