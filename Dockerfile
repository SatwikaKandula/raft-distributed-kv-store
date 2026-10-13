# ---------- Build stage ----------
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN mkdir -p /out \
    && go build -o /out/raft-node ./cmd/node \
    && go build -o /out/raft-client ./cmd/client \
    && go build -o /out/raft-admin ./cmd/admin \
    && go build -o /out/raft-benchmark ./cmd/benchmark

# ---------- Runtime stage ----------
FROM alpine:3.22

WORKDIR /app
RUN mkdir -p /app/data

COPY --from=builder /out/raft-node /usr/local/bin/raft-node
COPY --from=builder /out/raft-client /usr/local/bin/raft-client
COPY --from=builder /out/raft-admin /usr/local/bin/raft-admin
COPY --from=builder /out/raft-benchmark /usr/local/bin/raft-benchmark

ENTRYPOINT ["raft-node"]
