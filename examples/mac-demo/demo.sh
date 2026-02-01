#!/bin/bash
# TigerFS macOS Demo
# Runs TigerFS natively on macOS with PostgreSQL in Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MOUNTPOINT="/tmp/tigerfs-demo"
CONN_STR="postgres://demo:demo@localhost:5432/demo"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}==>${NC} $1"; }
error() { echo -e "${RED}==>${NC} $1"; }

check_dependencies() {
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed. Please install Docker Desktop for macOS."
        exit 1
    fi
    if ! docker info &> /dev/null; then
        error "Docker is not running. Please start Docker Desktop."
        exit 1
    fi
    if ! command -v go &> /dev/null; then
        error "Go is not installed. Please install Go 1.21+."
        exit 1
    fi
}

start_demo() {
    check_dependencies

    # Check if already running
    if mount | grep -q "$MOUNTPOINT"; then
        warn "TigerFS is already mounted at $MOUNTPOINT"
        echo "Run './demo.sh stop' first to restart."
        exit 1
    fi

    # Start PostgreSQL
    info "Starting PostgreSQL container..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

    # Wait for PostgreSQL to be ready
    info "Waiting for PostgreSQL to be ready..."
    until docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec -T postgres pg_isready -U demo -d demo &> /dev/null; do
        sleep 1
    done
    info "PostgreSQL is ready"
    info "Connection string: $CONN_STR"

    # Build TigerFS if needed
    if [[ ! -x "$REPO_ROOT/bin/tigerfs" ]]; then
        info "Building TigerFS..."
        (cd "$REPO_ROOT" && go build -o bin/tigerfs ./cmd/tigerfs)
    fi

    # Create mountpoint
    mkdir -p "$MOUNTPOINT"

    # Mount TigerFS in background
    info "Mounting TigerFS at $MOUNTPOINT..."
    "$REPO_ROOT/bin/tigerfs" mount "$CONN_STR" "$MOUNTPOINT" &
    TIGERFS_PID=$!

    # Wait for mount to be ready
    sleep 2
    if ! mount | grep -q "$MOUNTPOINT"; then
        error "Failed to mount TigerFS"
        kill $TIGERFS_PID 2>/dev/null || true
        exit 1
    fi

    info "TigerFS mounted successfully!"
    echo ""
    echo "Try these commands:"
    echo "  ls $MOUNTPOINT"
    echo "  ls $MOUNTPOINT/users"
    echo "  cat $MOUNTPOINT/users/1.json"
    echo "  cat $MOUNTPOINT/products/1.json"
    echo ""
    echo "Stop the demo with: ./demo.sh stop"
}

stop_demo() {
    info "Stopping TigerFS demo..."

    # Kill any tigerfs processes mounting to this mountpoint
    if pgrep -f "tigerfs.*$MOUNTPOINT" > /dev/null 2>&1; then
        info "Killing tigerfs processes for $MOUNTPOINT..."
        pkill -f "tigerfs.*$MOUNTPOINT" 2>/dev/null || true
        sleep 1
    fi

    # Also kill any tigerfs processes from this repo (in case of stale processes)
    if pgrep -f "$REPO_ROOT/bin/tigerfs" > /dev/null 2>&1; then
        info "Killing tigerfs processes from this repo..."
        pkill -f "$REPO_ROOT/bin/tigerfs" 2>/dev/null || true
        sleep 1
    fi

    # Unmount TigerFS (try multiple methods)
    if mount | grep -q "$MOUNTPOINT"; then
        info "Unmounting $MOUNTPOINT..."
        # Try regular unmount first
        umount "$MOUNTPOINT" 2>/dev/null || true
        sleep 1
        # If still mounted, try diskutil
        if mount | grep -q "$MOUNTPOINT"; then
            diskutil unmount force "$MOUNTPOINT" 2>/dev/null || true
            sleep 1
        fi
        # If still mounted, try umount -f (force)
        if mount | grep -q "$MOUNTPOINT"; then
            umount -f "$MOUNTPOINT" 2>/dev/null || true
        fi
    fi

    # Clean up mountpoint directory if empty
    if [[ -d "$MOUNTPOINT" ]] && [[ -z "$(ls -A "$MOUNTPOINT" 2>/dev/null)" ]]; then
        rmdir "$MOUNTPOINT" 2>/dev/null || true
    fi

    # Stop PostgreSQL
    info "Stopping PostgreSQL container..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down 2>/dev/null || true

    # Final check
    if mount | grep -q "$MOUNTPOINT"; then
        warn "Warning: $MOUNTPOINT may still be mounted. Try: sudo umount -f $MOUNTPOINT"
    else
        info "Demo stopped successfully"
    fi
}

show_status() {
    echo "=== TigerFS Demo Status ==="
    echo ""

    # Check mount
    if mount | grep -q "$MOUNTPOINT"; then
        echo -e "Mount:      ${GREEN}MOUNTED${NC} at $MOUNTPOINT"
    else
        echo -e "Mount:      ${RED}NOT MOUNTED${NC}"
    fi

    # Check PostgreSQL
    echo ""
    echo "PostgreSQL container:"
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps 2>/dev/null || echo "  Not running"
}

case "${1:-start}" in
    start)
        start_demo
        ;;
    stop)
        stop_demo
        ;;
    status)
        show_status
        ;;
    restart)
        stop_demo
        sleep 1
        start_demo
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}"
        echo ""
        echo "Commands:"
        echo "  start   - Start PostgreSQL and mount TigerFS"
        echo "  stop    - Unmount TigerFS and stop PostgreSQL"
        echo "  status  - Show current status"
        echo "  restart - Stop and start again"
        exit 1
        ;;
esac
