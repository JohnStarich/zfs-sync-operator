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
	"sigs.k8s.io/controller-runtime/pkg/builder"
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
	return Run(ctx, out, operatorNamespace, restConfig)
}

func mustNewScheme() *runtime.Scheme {
	scheme := backup.MustScheme()
	err := clientsetscheme.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}
	return scheme
}

func Run(ctx context.Context, out io.Writer, operatorNamespace string, restConfig *rest.Config) error {
	logger := logr.FromSlogHandler(slog.NewJSONHandler(out, nil))

	mgr, err := manager.New(restConfig, manager.Options{
		LeaderElection:                true,
		LeaderElectionID:              "zfs-sync-operator.johnstarich.com",
		LeaderElectionNamespace:       operatorNamespace,
		LeaderElectionReleaseOnCancel: true,
		Logger:                        logger,
		BaseContext: func() context.Context {
			return ctx
		},
		Scheme: mustNewScheme(),
	})
	if err != nil {
		return err
	}

	ctrl, err := controller.New("backup", mgr, controller.Options{
		Reconciler: backup.NewReconciler(mgr.GetClient()),
	})
	if err != nil {
		return err
	}

	if err := ctrl.Watch(source.Kind(mgr.GetCache(), &backup.Backup{}, &handler.TypedEnqueueRequestForObject[*backup.Backup]{})); err != nil {
		return err
	}

	// TODO validate Backup
	if err := builder.WebhookManagedBy(mgr).For(&backup.Backup{}).Complete(); err != nil {
		return err
	}
	return mgr.Start(ctx)
}
