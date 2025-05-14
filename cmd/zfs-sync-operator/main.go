// Command zfs-sync-operator runs the ZFS offsite backup operator. See the README for more details.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"

	"github.com/go-logr/logr"
	"github.com/johnstarich/zfs-sync-operator/internal/backup"
	"k8s.io/apimachinery/pkg/runtime"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	err := runWithArgs(ctx, os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runWithArgs(ctx context.Context, args []string, out io.Writer) error {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	err := flagSet.Parse(args)
	if err != nil {
		return err
	}
	if len(flagSet.Args()) > 0 {
		return errors.New("zfs-sync-operator: this command does not take any arguments")
	}
	restConfig, err := config.GetConfig()
	if err != nil {
		return err
	}
	const operatorNamespace = "zfs-sync-operator-system"
	o, err := New(ctx, out, operatorNamespace, restConfig)
	if err != nil {
		return err
	}
	return o.Wait()
}

func mustNewScheme() *runtime.Scheme {
	scheme := backup.MustScheme()
	err := clientsetscheme.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}
	return scheme
}

// Operator manages the lifecycle of the ZFS offsite backup operator. See the README for more details.
type Operator struct {
	manager  manager.Manager
	startErr chan error
}

// New returns a new [Operator]
func New(ctx context.Context, out io.Writer, operatorNamespace string, restConfig *rest.Config) (*Operator, error) {
	logger := logr.FromSlogHandler(slog.NewJSONHandler(out, nil))

	mgr, err := manager.New(restConfig, manager.Options{
		LeaderElection:                true,
		LeaderElectionID:              "zfs-sync-operator.johnstarich.com",
		LeaderElectionNamespace:       operatorNamespace,
		LeaderElectionReleaseOnCancel: true,
		Logger:                        logger,
		BaseContext:                   func() context.Context { return ctx },
		Scheme:                        mustNewScheme(),
	})
	if err != nil {
		return nil, err
	}

	ctrl, err := controller.New("backup", mgr, controller.Options{
		Reconciler: backup.NewReconciler(mgr.GetClient()),
	})
	if err != nil {
		return nil, err
	}
	if err := ctrl.Watch(source.Kind(mgr.GetCache(), &backup.Backup{}, &handler.TypedEnqueueRequestForObject[*backup.Backup]{})); err != nil {
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
