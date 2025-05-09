package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/johnstarich/zfs-sync-operator/internal/zfsbackup"
	corev1 "k8s.io/api/core/v1"
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
	err := run(ctx, os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	err := flagSet.Parse(args)
	if err != nil {
		return err
	}
	if len(flagSet.Args()) > 0 {
		return errors.New("zfs-sync-operator: this command does not take any arguments")
	}

	fmt.Fprintln(out, "Ready!")
	restConfig, err := config.GetConfig()
	if err != nil {
		return err
	}
	mgr, err := manager.New(restConfig, manager.Options{
		LeaderElection:          true,
		LeaderElectionID:        "zfs-sync-operator.johnstarich.com",
		LeaderElectionNamespace: "zfs-sync-operator-system",
		BaseContext:             func() context.Context { return ctx },
	})
	if err != nil {
		return err
	}

	ctrl, err := controller.New("zfsbackup", mgr, controller.Options{
		Reconciler: zfsbackup.New(mgr.GetClient()),
	})
	if err != nil {
		return err
	}

	if err := ctrl.Watch(source.Kind(mgr.GetCache(), &corev1.Pod{}, &handler.TypedEnqueueRequestForObject[*corev1.Pod]{})); err != nil {
		return err
	}

	// TODO validate ZFSBackup
	if err := builder.WebhookManagedBy(mgr).For(&corev1.Pod{}).Complete(); err != nil {
		return err
	}

	return mgr.Start(ctx)
}
