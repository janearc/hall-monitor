# Stage 1: build the static binary. hm imports its contract bindings from
# big-little-mesh as a pinned module (no-copies compliant), so there is no
# codegen stage here -- go mod download brings everything.
FROM golang:1.26-alpine AS builder

WORKDIR /src

# dependency layer, cached across fleet builds
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# static binary: no C linkage, runs on scratch
RUN CGO_ENABLED=0 GOOS=linux go build -o hm ./cmd/hm

# Stage 2: the empty runtime. hm reads the bus and serves three endpoints;
# it needs a filesystem for exactly nothing (its durable store arrives with
# the lease ledger in v2, on a PVC, not in the image).
FROM scratch

COPY --from=builder /src/hm /usr/local/bin/hm

# control port: /health, /metrics, /truth
EXPOSE 8090

ENTRYPOINT ["/usr/local/bin/hm"]
