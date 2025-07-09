package operator

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"testing"
	"time"

	"github.com/johnstarich/zfs-sync-operator/internal/clock"
	"github.com/johnstarich/zfs-sync-operator/internal/envtestrunner"
	"github.com/johnstarich/zfs-sync-operator/internal/idgen"
	"github.com/johnstarich/zfs-sync-operator/internal/testlog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RunTestMain orchestrates a package's tests to run with an envtestrunner. It controls the full lifecycle of the package.
//
// Example:
//
//	var TestEnv *envtestrunner.Runner
//
//	func TestMain(m *testing.M) {
//	    operator.RunTestMain(m, &TestEnv)
//	}
//
//	func TestFoo(t *testing.T) {
//		t.Parallel()
//		run := operator.RunTest(t, TestEnv)
//		...
//	}
func RunTestMain(m *testing.M, storeTestEnv **envtestrunner.Runner) {
	flag.Parse()
	if testing.Short() {
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	testEnv := envtestrunner.New(ctx, m.Run, mustNewScheme())
	*storeTestEnv = testEnv
	if err := testEnv.Run(os.Stdout); err != nil {
		exitCode := 1
		var exitCoder interface{ ExitCode() int }
		if errors.As(err, &exitCoder) && exitCoder.ExitCode() != 0 {
			exitCode = exitCoder.ExitCode()
		}
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(exitCode)
	}
}

// TestRunConfig contains data necessary for running tests. Returned from [RunTest].
type TestRunConfig struct {
	Clock     *clock.Test
	Namespace string
}

// RunTest sets up an [Operator] and a namespace to create resources in. Uses the existing global [envtestrunner.Runner] set up from a [RunTestMain].
func RunTest(tb testing.TB, testEnv *envtestrunner.Runner) (returnedConfig TestRunConfig) {
	tb.Helper()
	ctx, cancel := context.WithCancel(testEnv.Context())
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

	namespaceObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namespaceName(tb),
		},
	}
	err := testEnv.Client().Create(ctx, namespaceObj)
	if err != nil {
		tb.Fatal(err)
	}
	namespace := namespaceObj.Name

	level := slog.LevelInfo
	if testing.Verbose() {
		level = slog.LevelDebug
	}
	clock := clock.NewTest()
	operator, err := New(ctx, testEnv.RESTConfig(), Config{
		LogHandler:         testlog.NewLogHandler(tb, level),
		MetricsPort:        "0",
		Namespace:          namespace,
		clock:              clock,
		idempotentMetrics:  true,
		maxSessionWait:     8 * time.Second,
		onlyWatchNamespace: namespace,
		uuidGenerator:      idgen.NewDeterministicTest(tb),
	})
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
		Clock:     clock,
		Namespace: namespace,
	}
}

func namespaceName(tb testing.TB) string {
	name := tb.Name()
	const (
		maxNamespaceLength = 63
		randomSuffixLength = 6
		maxLength          = maxNamespaceLength - randomSuffixLength
	)
	if len(name) > maxLength {
		name = name[:maxLength]
	}
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, "#", "-")
	name = strings.ReplaceAll(name, "_", "-")
	return name
}
