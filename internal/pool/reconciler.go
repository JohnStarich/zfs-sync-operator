package pool

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/name"
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
	logger.Info("got pool", "status", pool.Status, "pool", pool.Spec)
	if pool.Status != nil && pool.Status.State != nil && *pool.Status.State == "Online" { // TODO verify nothing has changed
		// TODO can we ignore status field changes? only ack spec or secret updates?
		return reconcile.Result{}, nil
	}

	state, reason, reconcileErr := r.reconcile(ctx, pool)

	result := reconcile.Result{}
	poolStatusPatch := Pool{
		TypeMeta:   typeMeta(),
		ObjectMeta: metav1.ObjectMeta{Name: request.Name, Namespace: request.Namespace},
	}
	if reconcileErr == nil {
		logger.Info("pool detected successfully", "state", state)
		poolStatusPatch.Status = &Status{
			State:  toPointer(state),
			Reason: toPointer(reason),
		}
	} else {
		logger.Error(reconcileErr, "reconcile failed")
		const retryErrorWait = 1 * time.Minute
		result.RequeueAfter = retryErrorWait
		poolStatusPatch.Status = &Status{
			State:  toPointer("Error"),
			Reason: toPointer(reconcileErr.Error()),
		}
	}
	statusErr := r.client.Patch(ctx, &poolStatusPatch, ctrlclient.Merge, &ctrlclient.PatchOptions{
		FieldManager: name.Operator,
	})
	return result, errors.Wrap(statusErr, "failed to update status")
}

func (r *Reconciler) reconcile(ctx context.Context, pool Pool) (state, reason string, returnedErr error) {
	ctx, cancel := context.WithTimeout(ctx, r.maxSessionWait)
	defer cancel()

	command := safelyFormatCommand("/usr/sbin/zpool", "status", pool.Spec.Name)
	var zpoolStatus []byte
	err := pool.WithSession(ctx, r.client, func(session *ssh.Session) error {
		var err error
		zpoolStatus, err = session.CombinedOutput(command)
		zpoolStatus = bytes.TrimSpace(zpoolStatus)
		return errors.Wrapf(err, `failed to run '%s': %s`, command, string(zpoolStatus))
	})
	if err != nil {
		if bytes.HasSuffix(zpoolStatus, []byte(": no such pool")) {
			return "NotFound", string(zpoolStatus), nil
		}
		return "", "", err
	}
	stateField := stateFieldFromZpoolStatus(zpoolStatus)
	return stateFromStateField(stateField), "", nil
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
	switch state {
	case "ONLINE":
		return "Online"
	default:
		// TODO handle all known zpool states
		return "Unknown"
	}
}

func toPointer[Value any](value Value) *Value {
	return &value
}
