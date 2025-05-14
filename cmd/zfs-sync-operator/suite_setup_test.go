package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"testing"

	"github.com/johnstarich/zfs-sync-operator/internal/envtestrunner"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var TestEnv *envtestrunner.Runner //nolint:gochecknoglobals // The test environment is very expensive to set up, so this performance optimization is required for fast test execution.

func TestMain(m *testing.M) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	TestEnv = envtestrunner.New(ctx, m.Run, mustNewScheme())
	if err := TestEnv.Run(os.Stdout); err != nil {
		exitCode := 1
		var exitCoder interface{ ExitCode() int }
		if errors.As(err, &exitCoder) && exitCoder.ExitCode() != 0 {
			exitCode = exitCoder.ExitCode()
		}
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(exitCode)
	}
}

type TestRunConfig struct {
	Namespace string
}

func RunTest(tb testing.TB) (returnedConfig TestRunConfig) {
	tb.Helper()
	ctx, cancel := context.WithCancel(TestEnv.Context())
	shutdownCtx, shutdownComplete := context.WithCancel(context.Background())
	tb.Cleanup(func() {
		cancel()
		<-shutdownCtx.Done()
	})
	defer func() {
		if returnedConfig == (TestRunConfig{}) { // if setup fails, allow cleanup to finish
			shutdownComplete()
		}
	}()

	namespace := strings.ToLower(tb.Name())
	err := TestEnv.Client().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	})
	if err != nil {
		tb.Fatal(err)
	}

	operator, err := New(ctx, envtestrunner.NewWriter(tb), namespace, TestEnv.RESTConfig())
	if err != nil {
		tb.Fatal(err)
	}
	if err := operator.waitUntilLeader(); err != nil {
		tb.Fatal(err)
	}
	go func() {
		defer shutdownComplete()
		err := operator.Wait()
		if err != nil {
			tb.Fatal(err)
		}
	}()

	return TestRunConfig{
		Namespace: namespace,
	}
}
