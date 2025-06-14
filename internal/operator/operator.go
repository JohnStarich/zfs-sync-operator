// Package operator constructs and runs instances of [Operator]
package operator

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"runtime"
	"time"

	"github.com/go-logr/logr"
	"github.com/johnstarich/zfs-sync-operator/internal/backup"
	"github.com/johnstarich/zfs-sync-operator/internal/name"
	"github.com/johnstarich/zfs-sync-operator/internal/pointer"
	"github.com/johnstarich/zfs-sync-operator/internal/pool"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	clientconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
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
	LogHandler        slog.Handler
	MetricsPort       string
	Namespace         string
	idempotentMetrics bool          // disables safety checks for double metrics registrations
	maxSessionWait    time.Duration // allows tests to shorten wait times for faster pass/fail results
}

// New returns a new [Operator]
func New(ctx context.Context, restConfig *rest.Config, c Config) (*Operator, error) {
	if c.MetricsPort == "" {
		c.MetricsPort = "8080"
	}
	if c.maxSessionWait == 0 {
		c.maxSessionWait = 1 * time.Minute
	}
	logger := logr.FromSlogHandler(c.LogHandler)

	const reconcilesPerCPU = 2 // Most of the controllers are network-bound, allow a little shared CPU time
	mgr, err := manager.New(restConfig, manager.Options{
		LeaderElection:                true,
		LeaderElectionID:              name.Operator + ".johnstarich.com",
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

	{ // Backup
		ctrl, err := controller.New("backup", mgr, controller.Options{
			Reconciler: backup.NewReconciler(mgr.GetClient()),
		})
		if err != nil {
			return nil, err
		}
		if err := ctrl.Watch(source.Kind(
			mgr.GetCache(),
			&backup.Backup{},
			&handler.TypedEnqueueRequestForObject[*backup.Backup]{},
			predicate.TypedGenerationChangedPredicate[*backup.Backup]{},
		)); err != nil {
			return nil, err
		}
	}
	{ // Pool
		ctrl, err := controller.New("pool", mgr, controller.Options{
			Reconciler: pool.NewReconciler(mgr.GetClient(), c.maxSessionWait),
		})
		if err != nil {
			return nil, err
		}
		if err := ctrl.Watch(source.Kind(
			mgr.GetCache(),
			&pool.Pool{},
			&handler.TypedEnqueueRequestForObject[*pool.Pool]{},
			predicate.TypedGenerationChangedPredicate[*pool.Pool]{},
		)); err != nil {
			return nil, err
		}
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
