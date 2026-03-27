#!/bin/bash
# TigerFS Demo Launcher
# Unified entry point for Docker (Linux FUSE) and macOS (native NFS) demos.
#
# Usage: ./demo.sh [start|stop|status|restart|shell] [--docker|--mac]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}==>${NC} $1"; }
error() { echo -e "${RED}==>${NC} $1"; }

# ---------------------------------------------------------------------------
# Parse arguments: COMMAND [--docker|--mac]
# ---------------------------------------------------------------------------
COMMAND="${1:-start}"
MODE=""

for arg in "$@"; do
    case "$arg" in
        --docker) MODE="docker" ;;
        --mac)    MODE="mac" ;;
        start|stop|status|restart|shell) ;;
        *)
            echo "Usage: $0 {start|stop|status|restart|shell} [--docker|--mac]"
            echo ""
            echo "Commands:"
            echo "  start   - Start PostgreSQL, mount TigerFS, seed demo apps"
            echo "  stop    - Unmount TigerFS and stop containers"
            echo "  status  - Show current status"
            echo "  restart - Stop and start again"
            echo "  shell   - Enter the TigerFS environment"
            echo ""
            echo "Modes:"
            echo "  --docker  Docker containers for both PostgreSQL and TigerFS (default)"
            echo "  --mac     PostgreSQL in Docker, TigerFS runs natively on macOS"
            exit 1
            ;;
    esac
done

# Auto-detect mode if not specified
if [ -z "$MODE" ]; then
    if [ "$(uname)" = "Darwin" ]; then
        MODE="mac"
    else
        MODE="docker"
    fi
fi

# Mode-specific settings
if [ "$MODE" = "docker" ]; then
    COMPOSE_FILE="$SCRIPT_DIR/docker/docker-compose.yml"
    MOUNTPOINT="/mnt/db"
    CONN_STR="postgres://demo:demo@postgres:5432/demo"
else
    COMPOSE_FILE="$SCRIPT_DIR/mac/docker-compose.yml"
    MOUNTPOINT="/tmp/tigerfs-demo"
    CONN_STR="postgres://demo:demo@localhost:5432/demo"
fi

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
check_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed."
        exit 1
    fi
    if ! docker info &> /dev/null; then
        error "Docker is not running."
        exit 1
    fi
}

wait_for_postgres() {
    info "Waiting for PostgreSQL to be ready..."
    RETRIES=0
    until docker compose -f "$COMPOSE_FILE" logs postgres 2>&1 | \
        grep -q "PostgreSQL init process complete"; do
        RETRIES=$((RETRIES + 1))
        if [ $RETRIES -ge 60 ]; then
            error "PostgreSQL init did not complete after 60 seconds"
            exit 1
        fi
        sleep 1
    done
    until docker compose -f "$COMPOSE_FILE" exec -T postgres \
        pg_isready -U demo -d demo &> /dev/null; do
        sleep 1
    done
    info "PostgreSQL is ready"
}

run_seed() {
    local mount="$1"
    info "Seeding file-first apps via mount..."
    "$SCRIPT_DIR/seed.sh" "$mount"
}

# ---------------------------------------------------------------------------
# Docker mode
# ---------------------------------------------------------------------------
docker_is_mounted() {
    docker compose -f "$COMPOSE_FILE" exec -T tigerfs \
        mount 2>/dev/null | grep -q "$MOUNTPOINT"
}

docker_start() {
    check_docker

    if docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q tigerfs; then
        if docker_is_mounted; then
            warn "TigerFS is already mounted at $MOUNTPOINT (inside container)"
            echo "Run './demo.sh stop' first to restart."
            exit 1
        fi
    fi

    info "Building and starting containers..."
    docker compose -f "$COMPOSE_FILE" up -d --build

    wait_for_postgres
    info "Connection string: $CONN_STR"

    info "Mounting TigerFS at $MOUNTPOINT (inside container)..."
    docker compose -f "$COMPOSE_FILE" exec -T tigerfs \
        tigerfs mount "$CONN_STR" "$MOUNTPOINT" &

    sleep 2
    if ! docker_is_mounted; then
        error "Failed to mount TigerFS"
        exit 1
    fi

    # Seed file-first apps inside the container
    info "Seeding file-first apps..."
    docker compose -f "$COMPOSE_FILE" exec -T tigerfs \
        bash -c "$(cat "$SCRIPT_DIR/seed.sh")" -- "$MOUNTPOINT"

    info "TigerFS mounted successfully!"
    echo ""
    echo "Enter the container to explore:"
    echo "  ./demo.sh shell"
    echo ""
    echo "Or run commands directly:"
    echo "  docker compose -f $COMPOSE_FILE exec tigerfs ls -al $MOUNTPOINT/"
    echo "  docker compose -f $COMPOSE_FILE exec tigerfs cat $MOUNTPOINT/users/1.json"
    echo ""
    echo "Stop the demo with: ./demo.sh stop"
}

docker_stop() {
    info "Stopping TigerFS demo..."

    if docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q tigerfs; then
        if docker_is_mounted; then
            info "Unmounting TigerFS..."
            docker compose -f "$COMPOSE_FILE" exec -T tigerfs \
                umount "$MOUNTPOINT" 2>/dev/null || true
        fi
    fi

    info "Stopping containers..."
    docker compose -f "$COMPOSE_FILE" down

    info "Demo stopped"
}

docker_status() {
    echo "=== TigerFS Docker Demo Status ==="
    echo ""

    if ! docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q tigerfs; then
        echo -e "Containers: ${RED}NOT RUNNING${NC}"
        return
    fi

    echo -e "Containers: ${GREEN}RUNNING${NC}"

    if docker_is_mounted; then
        echo -e "Mount:      ${GREEN}MOUNTED${NC} at $MOUNTPOINT (inside container)"
    else
        echo -e "Mount:      ${RED}NOT MOUNTED${NC}"
    fi

    echo ""
    echo "Container status:"
    docker compose -f "$COMPOSE_FILE" ps
}

docker_shell() {
    check_docker

    if ! docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q tigerfs; then
        error "Demo is not running. Run './demo.sh start' first."
        exit 1
    fi

    info "Entering TigerFS container..."
    docker compose -f "$COMPOSE_FILE" exec -w "$MOUNTPOINT" tigerfs bash
}

# ---------------------------------------------------------------------------
# macOS mode
# ---------------------------------------------------------------------------
mac_start() {
    check_docker

    if ! command -v go &> /dev/null; then
        error "Go is not installed. Please install Go 1.21+."
        exit 1
    fi

    if mount | grep -q "$MOUNTPOINT"; then
        warn "TigerFS is already mounted at $MOUNTPOINT"
        echo "Run './demo.sh stop' first to restart."
        exit 1
    fi

    info "Starting PostgreSQL container..."
    docker compose -f "$COMPOSE_FILE" up -d

    wait_for_postgres
    info "Connection string: $CONN_STR"

    # Build TigerFS if needed
    if [ ! -x "$REPO_ROOT/bin/tigerfs" ]; then
        info "Building TigerFS..."
        (cd "$REPO_ROOT" && go build -o bin/tigerfs ./cmd/tigerfs)
    fi

    mkdir -p "$MOUNTPOINT"

    info "Mounting TigerFS at $MOUNTPOINT..."
    "$REPO_ROOT/bin/tigerfs" mount "$CONN_STR" "$MOUNTPOINT" &
    TIGERFS_PID=$!

    sleep 2
    if ! mount | grep -q "$MOUNTPOINT"; then
        error "Failed to mount TigerFS"
        kill $TIGERFS_PID 2>/dev/null || true
        exit 1
    fi

    # Seed file-first apps
    run_seed "$MOUNTPOINT"

    info "TigerFS mounted successfully!"
    echo ""
    echo "Try these commands:"
    echo "  cd $MOUNTPOINT ; ls -al"
    echo "  cat users/1.json"
    echo "  cat products/1.json"
    echo "  cat categories/.export/tsv"
    echo "  ls blog/                    # List blog posts"
    echo "  cat blog/hello-world.md     # Read a markdown post"
    echo "  ls docs/                    # List documentation"
    echo "  cat snippets/todo.txt       # Read a plain text snippet"
    echo ""
    echo "Stop the demo with: ./demo.sh stop"
}

mac_stop() {
    info "Stopping TigerFS demo..."

    if pgrep -f "tigerfs.*$MOUNTPOINT" > /dev/null 2>&1; then
        info "Killing tigerfs processes for $MOUNTPOINT..."
        pkill -f "tigerfs.*$MOUNTPOINT" 2>/dev/null || true
        sleep 1
    fi

    if pgrep -f "$REPO_ROOT/bin/tigerfs" > /dev/null 2>&1; then
        info "Killing tigerfs processes from this repo..."
        pkill -f "$REPO_ROOT/bin/tigerfs" 2>/dev/null || true
        sleep 1
    fi

    if mount | grep -q "$MOUNTPOINT"; then
        info "Unmounting $MOUNTPOINT..."
        umount "$MOUNTPOINT" 2>/dev/null || true
        sleep 1
        if mount | grep -q "$MOUNTPOINT"; then
            diskutil unmount force "$MOUNTPOINT" 2>/dev/null || true
            sleep 1
        fi
        if mount | grep -q "$MOUNTPOINT"; then
            umount -f "$MOUNTPOINT" 2>/dev/null || true
        fi
    fi

    if [ -d "$MOUNTPOINT" ] && [ -z "$(ls -A "$MOUNTPOINT" 2>/dev/null)" ]; then
        rmdir "$MOUNTPOINT" 2>/dev/null || true
    fi

    info "Stopping PostgreSQL container..."
    docker compose -f "$COMPOSE_FILE" down 2>/dev/null || true

    if mount | grep -q "$MOUNTPOINT"; then
        warn "Warning: $MOUNTPOINT may still be mounted. Try: sudo umount -f $MOUNTPOINT"
    else
        info "Demo stopped successfully"
    fi
}

mac_status() {
    echo "=== TigerFS macOS Demo Status ==="
    echo ""

    if mount | grep -q "$MOUNTPOINT"; then
        echo -e "Mount:      ${GREEN}MOUNTED${NC} at $MOUNTPOINT"
    else
        echo -e "Mount:      ${RED}NOT MOUNTED${NC}"
    fi

    echo ""
    echo "PostgreSQL container:"
    docker compose -f "$COMPOSE_FILE" ps 2>/dev/null || echo "  Not running"
}

mac_shell() {
    if ! mount | grep -q "$MOUNTPOINT"; then
        error "Demo is not running. Run './demo.sh start' first."
        exit 1
    fi
    cd "$MOUNTPOINT"
    exec "${SHELL:-bash}"
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "$COMMAND" in
    start)
        if [ "$MODE" = "docker" ]; then docker_start; else mac_start; fi
        ;;
    stop)
        if [ "$MODE" = "docker" ]; then docker_stop; else mac_stop; fi
        ;;
    status)
        if [ "$MODE" = "docker" ]; then docker_status; else mac_status; fi
        ;;
    restart)
        if [ "$MODE" = "docker" ]; then docker_stop; else mac_stop; fi
        sleep 1
        if [ "$MODE" = "docker" ]; then docker_start; else mac_start; fi
        ;;
    shell)
        if [ "$MODE" = "docker" ]; then docker_shell; else mac_shell; fi
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart|shell} [--docker|--mac]"
        exit 1
        ;;
esac
