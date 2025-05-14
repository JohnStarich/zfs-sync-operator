LINT_VERSION = 2.1.6
GO_BIN = $(shell printf '%s/bin' "$$(go env GOPATH)")
SHELL = bash

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
