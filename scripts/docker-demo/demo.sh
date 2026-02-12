#!/bin/bash
# TigerFS Docker Demo
# Runs TigerFS and PostgreSQL both in Docker containers

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MOUNTPOINT="/mnt/db"
CONN_STR="postgres://demo:demo@postgres:5432/demo"

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
        error "Docker is not installed."
        exit 1
    fi
    if ! docker info &> /dev/null; then
        error "Docker is not running."
        exit 1
    fi
}

is_mounted() {
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec -T tigerfs \
        mount 2>/dev/null | grep -q "$MOUNTPOINT"
}

start_demo() {
    check_dependencies

    # Check if already mounted
    if docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps --status running 2>/dev/null | grep -q tigerfs; then
        if is_mounted; then
            warn "TigerFS is already mounted at $MOUNTPOINT (inside container)"
            echo "Run './demo.sh stop' first to restart."
            exit 1
        fi
    fi

    # Build and start containers
    info "Building and starting containers..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d --build

    # Wait for PostgreSQL to be ready
    info "Waiting for PostgreSQL to be ready..."
    until docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec -T postgres pg_isready -U demo -d demo &> /dev/null; do
        sleep 1
    done
    info "PostgreSQL is ready"
    info "Connection string: $CONN_STR"

    # Mount TigerFS inside the container
    info "Mounting TigerFS at $MOUNTPOINT (inside container)..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec -T tigerfs \
        tigerfs mount "$CONN_STR" "$MOUNTPOINT" &

    # Wait for mount to be ready
    sleep 2
    if ! is_mounted; then
        error "Failed to mount TigerFS"
        exit 1
    fi

    info "TigerFS mounted successfully!"
    echo ""
    echo "Enter the container to explore:"
    echo "  ./demo.sh shell"
    echo ""
    echo "Or run commands directly:"
    echo "  docker compose exec tigerfs ls -al $MOUNTPOINT/"
    echo "  docker compose exec tigerfs cat $MOUNTPOINT/users/1.json"
    echo ""
    echo "Stop the demo with: ./demo.sh stop"
}

stop_demo() {
    info "Stopping TigerFS demo..."

    # Unmount TigerFS inside container
    if docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps --status running 2>/dev/null | grep -q tigerfs; then
        if is_mounted; then
            info "Unmounting TigerFS..."
            docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec -T tigerfs \
                umount "$MOUNTPOINT" 2>/dev/null || true
        fi
    fi

    # Stop containers
    info "Stopping containers..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down

    info "Demo stopped"
}

show_status() {
    echo "=== TigerFS Docker Demo Status ==="
    echo ""

    # Check containers
    if ! docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps --status running 2>/dev/null | grep -q tigerfs; then
        echo -e "Containers: ${RED}NOT RUNNING${NC}"
        return
    fi

    echo -e "Containers: ${GREEN}RUNNING${NC}"

    # Check mount
    if is_mounted; then
        echo -e "Mount:      ${GREEN}MOUNTED${NC} at $MOUNTPOINT (inside container)"
    else
        echo -e "Mount:      ${RED}NOT MOUNTED${NC}"
    fi

    echo ""
    echo "Container status:"
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps
}

shell_demo() {
    check_dependencies

    if ! docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps --status running 2>/dev/null | grep -q tigerfs; then
        error "Demo is not running. Run './demo.sh start' first."
        exit 1
    fi

    info "Entering TigerFS container..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec -w "$MOUNTPOINT" tigerfs bash
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
    shell)
        shell_demo
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart|shell}"
        echo ""
        echo "Commands:"
        echo "  start   - Build containers, start PostgreSQL, mount TigerFS"
        echo "  stop    - Unmount TigerFS and stop containers"
        echo "  status  - Show current status"
        echo "  restart - Stop and start again"
        echo "  shell   - Enter the TigerFS container"
        exit 1
        ;;
esac
