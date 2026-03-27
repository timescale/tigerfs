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
    vim \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js for Claude Code
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code
RUN npm install -g @anthropic-ai/claude-code

# Install Tiger CLI
RUN curl -s https://packagecloud.io/install/repositories/timescale/tiger-cli/script.deb.sh | os=any dist=any bash \
    && apt-get install -y tiger-cli \
    && rm -rf /var/lib/apt/lists/*

# Install Ghost CLI
RUN curl -s https://packagecloud.io/install/repositories/timescale/ghost/script.deb.sh | os=any dist=any bash \
    && apt-get install -y ghost \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/tigerfs /usr/local/bin/tigerfs

RUN mkdir -p /mnt/db

# Add convenience alias for mounting
RUN echo "alias mnt='tigerfs mount --insecure-no-ssl postgres://demo:demo@postgres:5432/demo /mnt/db &'" >> /root/.bashrc

CMD ["bash", "-c", "echo 'TigerFS Demo Ready!' && echo '' && echo 'Mount the database:' && echo '  mnt                  # alias for the command below' && echo '  tigerfs mount --insecure-no-ssl postgres://demo:demo@postgres:5432/demo /mnt/db &' && echo '' && echo 'Then explore:' && echo '  ls /mnt/db' && echo '  cat /mnt/db/users/1' && echo '  cat /mnt/db/users/1.json' && echo '' && sleep infinity"]
