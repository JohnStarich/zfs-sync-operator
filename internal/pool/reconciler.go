package pool

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Reconciler reconciles Pool resources to validate their Pools and associated connections
type Reconciler struct {
	client         ctrlclient.Client
	maxSessionWait time.Duration
}

// NewReconciler returns a new pool reconciler
func NewReconciler(client ctrlclient.Client, maxSessionWait time.Duration) *Reconciler {
	return &Reconciler{
		client:         client,
		maxSessionWait: maxSessionWait,
	}
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
	statusUpdate := Pool{
		TypeMeta: typeMeta(),
		ObjectMeta: metav1.ObjectMeta{
			Name:            request.Name,
			Namespace:       request.Namespace,
			ResourceVersion: resourceVersion,
		},
	}
	if reconcileErr == nil {
		logger.Info("pool detected successfully", "state", state)
		statusUpdate.Status = &Status{
			State:  state,
			Reason: reason,
		}
	} else {
		logger.Error(reconcileErr, "reconcile failed")
		const retryErrorWait = 1 * time.Minute
		result.RequeueAfter = retryErrorWait
		statusUpdate.Status = &Status{
			State:  "Error",
			Reason: reconcileErr.Error(),
		}
	}
	statusErr := r.client.Status().Update(ctx, &statusUpdate)
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
// TODO Once JSON support is accessible to test against, switch to parsing it.
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

func toPointer[Value any](value Value) *Value {
	return &value
}
