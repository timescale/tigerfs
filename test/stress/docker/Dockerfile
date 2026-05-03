# tigerfs-stress docker-FUSE image.
#
# Stage 1 builds both binaries from source so the runner and tigerfs are
# guaranteed to match the working tree. Stage 2 is a small alpine runtime
# with both fuse2 and fuse3 installed (tigerfs's unmount path invokes
# `fusermount`, the fuse2 binary name; alpine's fuse3 package alone does
# not provide it).

FROM golang:1.25-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /out/tigerfs ./cmd/tigerfs && \
    go build -o /out/tigerfs-stress ./test/stress


FROM alpine:latest

RUN apk add --no-cache fuse fuse3 postgresql-client ca-certificates

COPY --from=builder /out/tigerfs /usr/local/bin/tigerfs
COPY --from=builder /out/tigerfs-stress /usr/local/bin/tigerfs-stress

WORKDIR /work

# Default: idle. The launcher (scripts/stress-docker.sh) drives the container
# via `docker compose exec`, so the entrypoint just needs to keep PID 1 alive.
CMD ["sleep", "infinity"]
