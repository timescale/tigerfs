# Multi-stage Dockerfile for TigerFS

# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git fuse-dev

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary
RUN CGO_ENABLED=1 go build -o tigerfs ./cmd/tigerfs

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache fuse ca-certificates

# Create non-root user
RUN addgroup -g 1000 tigerfs && \
    adduser -D -u 1000 -G tigerfs tigerfs

# Copy binary from builder
COPY --from=builder /build/tigerfs /usr/local/bin/tigerfs

# Create mount point
RUN mkdir -p /mnt/db && chown tigerfs:tigerfs /mnt/db

# Switch to non-root user
USER tigerfs

WORKDIR /home/tigerfs

ENTRYPOINT ["/usr/local/bin/tigerfs"]
CMD ["--help"]
