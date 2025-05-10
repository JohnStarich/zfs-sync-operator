package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestMain(m *testing.M) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	runner := newTestRunner(m.Run)
	if err := runner.Run(ctx, os.Stdout); err != nil {
		exitCode := 1
		var exitCoder interface{ ExitCode() int }
		if errors.As(err, &exitCoder) && exitCoder.ExitCode() != 0 {
			exitCode = exitCoder.ExitCode()
		}
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(exitCode)
	}
}

func newTestRunner(runTests func() int) *testRunner {
	return &testRunner{
		runTests: runTests,
	}
}

type testRunner struct {
	runTests func() int
	env      *envtest.Environment
}

func (r *testRunner) Run(ctx context.Context, out io.Writer) (returnedErr error) {
	if err := r.setUp(ctx, out); err != nil {
		return err
	}
	defer func() {
		const tearDownTimeout = 5 * time.Second
		tearDownCtx, cancel := context.WithTimeout(context.Background(), tearDownTimeout)
		defer cancel()
		err := r.tearDown(tearDownCtx)
		if err != nil && returnedErr == nil {
			returnedErr = err
		}
	}()
	exitCode := r.runTests()
	if exitCode != 0 {
		return exitError{exitCode: exitCode}
	}
	return nil
}

type exitError struct {
	exitCode int
}

func (e exitError) Error() string {
	return fmt.Sprintf("exit status %d", e.exitCode)
}

func runCommand(ctx context.Context, errOut io.Writer, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = errOut
	err := cmd.Run()
	return buf.String(), err
}

func (r *testRunner) setUp(ctx context.Context, out io.Writer) (returnedErr error) {
	defer func() { returnedErr = errors.WithStack(returnedErr) }()

	goPathOutput, err := runCommand(ctx, out, "go", "env", "GOPATH")
	if err != nil {
		return err
	}
	envTestPath := filepath.Join(strings.TrimSpace(goPathOutput), "bin", "setup-envtest")
	if _, err := exec.LookPath(envTestPath); err != nil {
		// TODO In Go 1.24+, use 'go get -tool' to lock in the version inside go.mod
		const setupEnvTestVersion = "52b1791"
		if _, err := runCommand(ctx, out, "go", "install", "sigs.k8s.io/controller-runtime/tools/setup-envtest@"+setupEnvTestVersion); err != nil {
			return err
		}
	}
	const kubeVersion = "1.31"
	kubebuilderAssetsPath, err := runCommand(ctx, out, envTestPath, "use", "-p", "path", kubeVersion)
	if err != nil {
		return err
	}
	r.env = &envtest.Environment{BinaryAssetsDirectory: kubebuilderAssetsPath}
	_, err = r.env.Start()
	if err != nil {
		return err
	}
	return nil
}

func (r *testRunner) tearDown(context.Context) error {
	return r.env.Stop()
}
