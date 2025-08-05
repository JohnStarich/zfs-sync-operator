FROM docker.io/library/golang:1 AS builder
RUN mkdir /tmp/empty-dir
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /zfs-sync-operator ./cmd/zfs-sync-operator

FROM scratch
USER 1000:1000
# Due to a test import and a package-level init() func, /tmp/kubebuilder-envtest must exist.
COPY --from=builder /tmp/empty-dir /tmp/kubebuilder-envtest

COPY --from=builder /zfs-sync-operator /
ENTRYPOINT ["/zfs-sync-operator"]
