# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o tigerfs ./cmd/tigerfs

# Runtime stage
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    fuse3 \
    postgresql-client \
    less \
    jq \
    tree \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js for Claude Code
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code
RUN npm install -g @anthropic-ai/claude-code

COPY --from=builder /build/tigerfs /usr/local/bin/tigerfs

RUN mkdir -p /mnt/db

CMD ["bash", "-c", "echo 'TigerFS Demo Ready!' && echo '' && echo 'Mount the database:' && echo '  tigerfs mount postgres://demo:demo@postgres:5432/demo /mnt/db' && echo '' && echo 'Then explore:' && echo '  ls /mnt/db' && echo '  cat /mnt/db/users/1' && echo '  cat /mnt/db/users/1.json' && echo '' && sleep infinity"]
