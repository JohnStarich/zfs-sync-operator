// Package operator constructs and runs instances of [Operator]
package operator

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"net"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/go-logr/logr"
	zfsconfig "github.com/johnstarich/zfs-sync-operator/config"
	"github.com/johnstarich/zfs-sync-operator/internal/backup"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/johnstarich/zfs-sync-operator/internal/pointer"
	"github.com/johnstarich/zfs-sync-operator/internal/pool"
	"github.com/pkg/errors"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Run starts an [Operator] with the given runtime context, CLI args, and output stream.
// Waits until ctx is canceled, then shuts down and returns.
func Run(ctx context.Context, args []string, out io.Writer) error {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	logLevel := flagSet.Int("log-level", 0, "The log level. Defaults to Info. Use -4 for Debug, 4 for Warn, and 8 for Error.")
	err := flagSet.Parse(args)
	if err != nil {
		return err
	}
	if len(flagSet.Args()) > 0 {
		return errors.New(name.Operator + ": this command does not take any arguments")
	}
	restConfig, err := clientconfig.GetConfig()
	if err != nil {
		return err
	}
	const operatorNamespace = name.Operator + "-system"
	o, err := New(ctx, restConfig, Config{
		LogHandler: slog.NewJSONHandler(out, &slog.HandlerOptions{Level: slog.Level(*logLevel)}),
		Namespace:  operatorNamespace,
	})
	if err != nil {
		return err
	}
	return o.Wait()
}

func mustNewScheme() *apiruntime.Scheme {
	scheme := apiruntime.NewScheme()
	err := clientsetscheme.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}
	backup.MustAddToScheme(scheme)
	pool.MustAddToScheme(scheme)
	return scheme
}

// Operator manages the lifecycle of the ZFS offsite backup operator. See the README for more details.
type Operator struct {
	manager  manager.Manager
	startErr chan error
}

// Config contains configuration to set up an [Operator]
type Config struct {
	LogHandler         slog.Handler
	MetricsPort        string
	Namespace          string
	idempotentMetrics  bool             // disables safety checks for double metrics registrations
	maxSessionWait     time.Duration    // allows tests to shorten wait times for faster pass/fail results
	onlyWatchNamespace string           // in tests only, restrict watches to this namespace
	timeNow            func() time.Time // in tests only, control time for more consistent, assertable results
}

// New returns a new [Operator]
func New(ctx context.Context, restConfig *rest.Config, c Config) (*Operator, error) {
	logger := logr.FromSlogHandler(c.LogHandler)
	ctx = log.IntoContext(ctx, logger)
	if c.MetricsPort == "" {
		c.MetricsPort = "8080"
	}
	if c.maxSessionWait == 0 {
		c.maxSessionWait = 1 * time.Minute
	}
	if c.timeNow == nil {
		c.timeNow = time.Now
	}

	cacheOptions := cache.Options{}
	if c.onlyWatchNamespace != "" {
		cacheOptions.DefaultNamespaces = map[string]cache.Config{c.onlyWatchNamespace: {}}
	}

	const reconcilesPerCPU = 2 // Most of the controllers are network-bound, allow a little shared CPU time
	mgr, err := manager.New(restConfig, manager.Options{
		Cache:                         cacheOptions,
		LeaderElection:                true,
		LeaderElectionID:              name.Domain,
		LeaderElectionNamespace:       c.Namespace,
		LeaderElectionReleaseOnCancel: true,
		Logger:                        logger,
		BaseContext:                   func() context.Context { return ctx },
		Scheme:                        mustNewScheme(),
		Controller: config.Controller{
			SkipNameValidation:      pointer.Of(c.idempotentMetrics),
			MaxConcurrentReconciles: runtime.NumCPU() * reconcilesPerCPU,
		},
		Metrics: server.Options{
			BindAddress: net.JoinHostPort("", c.MetricsPort),
		},
	})
	if err != nil {
		return nil, err
	}
	if err := backup.RegisterReconciler(mgr); err != nil {
		return nil, err
	}
	if err := pool.RegisterReconciler(mgr, c.maxSessionWait, c.timeNow); err != nil {
		return nil, err
	}

	crds, err := zfsconfig.CustomResourceDefinitions()
	if err != nil {
		return nil, err
	}
	// TODO move index creation into registration, remove selectableFields from CRDs since they're only used by this operator
	if err := indexAllSelectableFields(ctx, mgr, crds); err != nil {
		return nil, err
	}

	operator := &Operator{
		manager:  mgr,
		startErr: make(chan error),
	}
	go func() {
		operator.startErr <- operator.manager.Start(ctx)
	}()
	return operator, nil
}

func (o *Operator) waitUntilLeader() error {
	select {
	case err := <-o.startErr:
		return err
	case <-o.manager.Elected():
		return nil
	}
}

// Wait blocks until 'o' has terminated, then returns any error encountered
func (o *Operator) Wait() error {
	return <-o.startErr
}

func indexAllSelectableFields(ctx context.Context, mgr manager.Manager, crds []*apiextensionsv1.CustomResourceDefinition) error {
	logger := log.FromContext(ctx)
	scheme := mgr.GetScheme()
	indexer := mgr.GetFieldIndexer()
	for _, crd := range crds {
		for _, version := range crd.Spec.Versions {
			logger.Info("Setting up CRD selectableFields indexes", "crd", crd.Name, "version", version.Name, "fields", version.SelectableFields)
			for _, selectableField := range version.SelectableFields {
				typ, ok := scheme.AllKnownTypes()[schema.GroupVersionKind{
					Group:   crd.Spec.Group,
					Version: version.Name,
					Kind:    crd.Spec.Names.Kind,
				}]
				if !ok {
					return errors.Errorf("group version kind for custom resource not found: %s", crd.GroupVersionKind())
				}
				err := indexer.IndexField(ctx, reflect.New(typ).Interface().(client.Object), selectableField.JSONPath, func(o client.Object) []string {
					return findJSONPath(ctx, o, selectableField.JSONPath)
				})
				if err != nil {
					return errors.WithMessagef(err, "failed to index field %q on type %s", selectableField.JSONPath, crd.Spec.Names.Kind)
				}
			}
		}
	}
	return nil
}

func findJSONPath(ctx context.Context, value any, jsonPath string) []string {
	logger := log.FromContext(ctx)
	jsonValue, err := json.Marshal(value)
	if err != nil {
		logger.Error(err, "failed to find jsonPath in value", "value", value)
		return nil
	}
	var destructuredValue any
	if err := json.Unmarshal(jsonValue, &destructuredValue); err != nil {
		logger.Error(err, "failed to unmarshal JSON")
		return nil
	}
	const keySeparator = "."
	keyPath := strings.Split(strings.TrimPrefix(jsonPath, keySeparator), keySeparator)
	findValue, ok := findKeyPath(destructuredValue, keyPath)
	if !ok {
		return nil
	}
	stringValue, ok := findValue.(string)
	if !ok {
		return nil
	}
	return []string{stringValue}
}

func findKeyPath(value any, keys []string) (any, bool) {
	if len(keys) == 0 {
		return value, true
	}
	mapValue, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	return findKeyPath(mapValue[keys[0]], keys[1:])
}
