SHELL = bash

# Define versions for this build. For tagged releases, IMAGE_TAG and SEMANTIC_VERSION are the same semantic version value (with 'v' prefix).
# For untagged builds, like in CI or a local environment, IMAGE_TAG is the current commit hash and SEMANTIC_VERSION uses the default of v0.0.0.
IMAGE_TAG := $(or ${GITHUB_REF_NAME},$(shell git rev-parse --verify HEAD))
SEMANTIC_VERSION := $(or ${GITHUB_REF_NAME},v0.0.0)
export

LINT_VERSION = 2.1.6
GO_BIN = $(shell printf '%s/bin' "$$(go env GOPATH)")
CONTAINER_BUILDER = $(shell which podman 2>/dev/null || which docker)

.PHONY: lint-deps
lint-deps:
	@if ! which golangci-lint >/dev/null || [[ "$$(golangci-lint version 2>&1)" != *${LINT_VERSION}* ]]; then \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "${GO_BIN}" v"${LINT_VERSION}"; \
	fi

.PHONY: lint
lint: lint-deps
	"${GO_BIN}/golangci-lint" run

.PHONY: test
test:
	go test -race ./...

.PHONY: build
build:
	"${CONTAINER_BUILDER}" build \
		--file Containerfile \
		--tag "ghcr.io/johnstarich/zfs-sync-operator:$${IMAGE_TAG#v}" \
		.

.PHONY: run
run:
	kubectl apply -f ./internal/config/crd
	go run ./cmd/zfs-sync-operator -log-level -4

.PHONY: package-helm
package-helm:
	@if ! which helm 2>/dev/null >/dev/null; then \
		echo 'Helm must be installed to run "make package". See https://helm.sh/docs/intro/install/ for installation instructions.'; \
		exit 2; \
	fi
	helm package \
		--version "$${SEMANTIC_VERSION#v}" \
		--destination ignore/charts \
		internal/config

.PHONY: deploy-operator
deploy-operator: build
	"${CONTAINER_BUILDER}" push "ghcr.io/johnstarich/zfs-sync-operator:$${IMAGE_TAG#v}"

.PHONY: deploy-helm
deploy-helm: package-helm
	helm push "ignore/charts/zfs-sync-operator-helm-charts-$${SEMANTIC_VERSION#v}.tgz" oci://ghcr.io/johnstarich
