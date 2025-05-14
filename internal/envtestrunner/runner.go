// Package envtestrunner automates envtest binary installation, startup, and shutdown.
package envtestrunner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/johnstarich/zfs-sync-operator/config"
	"github.com/pkg/errors"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

// Runner automates envtest binary installation, startup, and shutdown
type Runner struct {
	ctx      context.Context
	env      *envtest.Environment
	client   client.Client
	runTests func() int
	scheme   *runtime.Scheme
}

// New returns a new [Runner] with the given scheme and func to run all tests in a package.
// Typically runTests is [testing.M.Run].
func New(ctx context.Context, runTests func() int, scheme *runtime.Scheme) *Runner {
	return &Runner{
		ctx:      ctx,
		runTests: runTests,
		scheme:   scheme,
	}
}

// Run executes all tests and directs output to the given writer
func (r *Runner) Run(out io.Writer) (returnedErr error) {
	if err := r.setUp(r.ctx, out); err != nil {
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

// Context is a convenient context to use that is canceled on test suite shutdown
func (r *Runner) Context() context.Context {
	return r.ctx
}

// RESTConfig should be passed to an operator's constructor
func (r *Runner) RESTConfig() *rest.Config {
	return r.env.Config
}

// Client should be used to interact with the test kube-apiserver
func (r *Runner) Client() client.Client {
	return r.client
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

func (r *Runner) setUp(ctx context.Context, out io.Writer) (returnedErr error) {
	defer func() { returnedErr = errors.WithStack(returnedErr) }()

	goPathOutput, err := runCommand(ctx, out, "go", "env", "GOPATH")
	if err != nil {
		return err
	}
	envTestPath := filepath.Join(strings.TrimSpace(goPathOutput), "bin", "setup-envtest")
	_, err = exec.LookPath(envTestPath)
	if err != nil {
		// TODO In Go 1.24+, use 'go get -tool' to lock in the version inside go.mod
		const setupEnvTestVersion = "52b1791"
		_, err = runCommand(ctx, out, "go", "install", "sigs.k8s.io/controller-runtime/tools/setup-envtest@"+setupEnvTestVersion)
		if err != nil {
			return err
		}
	}
	const kubeVersion = "1.31"
	kubebuilderAssetsPath, err := runCommand(ctx, out, envTestPath, "use", "-p", "path", kubeVersion)
	if err != nil {
		return err
	}
	crds, err := crdsFromFS(config.FS())
	if err != nil {
		return err
	}
	r.env = &envtest.Environment{
		BinaryAssetsDirectory: kubebuilderAssetsPath,
		CRDs:                  crds,
		ErrorIfCRDPathMissing: true,
		Scheme:                r.scheme,
	}
	_, err = r.env.Start()
	if err != nil {
		return err
	}
	r.client, err = client.New(r.env.Config, client.Options{
		Scheme: r.scheme,
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *Runner) tearDown(context.Context) error {
	return r.env.Stop()
}

func crdsFromFS(files fs.FS) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	var crds []*apiextensionsv1.CustomResourceDefinition
	err := fs.WalkDir(files, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		contents, err := fs.ReadFile(files, path)
		if err != nil {
			return err
		}
		var crd apiextensionsv1.CustomResourceDefinition
		err = yaml.Unmarshal(contents, &crd)
		if err != nil {
			return err
		}
		crds = append(crds, &crd)
		return nil
	})
	return crds, err
}
