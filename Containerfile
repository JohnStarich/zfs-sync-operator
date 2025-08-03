FROM golang:1 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /zfs-sync-operator ./cmd/zfs-sync-operator

FROM scratch
USER 1000:1000
COPY --from=builder /zfs-sync-operator /
ENTRYPOINT ["/zfs-sync-operator"]
