SHELL = bash

CHART_RELEASER_VERSION = 1.8.1
LINT_VERSION = 2.1.6
GO_BIN = $(shell printf '%s/bin' "$$(go env GOPATH)")
CONTAINER_BUILDER = $(shell which podman 2>/dev/null || which docker)

# Define versions for this build. For tagged releases, IMAGE_TAG and SEMANTIC_VERSION are the same semantic version value.
# For untagged builds, like in CI or a local environment, IMAGE_TAG is the current commit hash and SEMANTIC_VERSION uses the default of 0.0.0.
ifeq (${GITHUB_REF_TYPE},tag)
	IMAGE_TAG := ${GITHUB_REF_NAME:v%=%}
	SEMANTIC_VERSION := ${GITHUB_REF_NAME:v%=%}
else
	IMAGE_TAG := $(shell git rev-parse --verify HEAD)
	SEMANTIC_VERSION := 0.0.0
endif

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
		--tag "ghcr.io/johnstarich/zfs-sync-operator:${IMAGE_TAG}" \
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
		--version "${SEMANTIC_VERSION}" \
		--destination ignore/charts \
		internal/config

.PHONY: deploy-helm
deploy-helm: package-helm
	@set -e; \
		if ! which cr 2>/dev/null; then \
			curl -sSL "https://github.com/helm/chart-releaser/releases/download/v${CHART_RELEASER_VERSION}/chart-releaser_${CHART_RELEASER_VERSION}_$$(go env GOOS)_$$(go env GOARCH).tar.gz" | tar -xvzO cr > "${GO_BIN}/cr"; \
			chmod +x "${GO_BIN}/cr"; \
		fi
	# NOTE: Local uploads may panic if using ssh or git remote URLs: https://github.com/helm/chart-releaser/issues/124
	"${GO_BIN}/cr" upload \
		--git-repo zfs-sync-operator \
		--owner johnstarich \
		--package-path ignore/charts \
		--pr \
		--skip-existing \
		--token "${GITHUB_TOKEN}"
	"${GO_BIN}/cr" index \
		--git-repo zfs-sync-operator \
		--index-path . \
		--owner johnstarich \
		--package-path ignore/charts \
		--push \
		--token "${GITHUB_TOKEN}"

.PHONY: deploy-operator
deploy-operator: build
	"${CONTAINER_BUILDER}" push "ghcr.io/johnstarich/zfs-sync-operator:${IMAGE_TAG}"
