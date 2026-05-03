#!/bin/bash
#
# Run tigerfs-stress inside a privileged Linux container with FUSE.
#
# Why this exists: the native binary uses NFS on macOS and FUSE on Linux.
# This wrapper lets you exercise the FUSE path from any host (incl. macOS)
# by running both `tigerfs` and `tigerfs-stress` inside a Linux container
# with /dev/fuse and AppArmor escapes pre-set.
#
# Usage:
#   ./scripts/stress-docker.sh                          # 20 iters, random seed
#   ./scripts/stress-docker.sh --seed 42 --iterations 200
#   ./scripts/stress-docker.sh --large-files --many-files --iterations 1000
#   ./scripts/stress-docker.sh --keep-infra ...         # leave infra running on exit
#
# Dumps land on the host at test/stress/docker/host-out/.
# To stop a long run from another terminal:
#   docker compose -f test/stress/docker/docker-compose.yml \
#     exec stress /usr/local/bin/tigerfs-stress stop

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/test/stress/docker/docker-compose.yml"
HOST_OUT="$PROJECT_ROOT/test/stress/docker/host-out"

# Parse our own flags (intercepted before passthrough).
KEEP=0
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --keep-infra)
            KEEP=1
            shift
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

mkdir -p "$HOST_OUT"

cleanup() {
    if [ "$KEEP" = "0" ]; then
        # Best-effort: ask the runner to do its structured teardown first
        # (kill tigerfs, unmount, remove mountpoint). Then bring down the
        # compose stack regardless of whether that succeeded.
        docker compose -f "$COMPOSE_FILE" exec -T \
            -e TIGERFS_STRESS_INFO_FILE=/out/tigerfs-stress.info \
            stress /usr/local/bin/tigerfs-stress stop 2>/dev/null || true
        docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    else
        echo ""
        echo "Infrastructure left running (--keep-infra). Tear down with:"
        echo "  docker compose -f $COMPOSE_FILE down -v"
    fi
}
trap cleanup EXIT INT TERM

cd "$PROJECT_ROOT"

# Always start with clean postgres state. If a previous run left containers
# alive (--keep-infra), reusing that postgres would carry the previous run's
# workspace data into this run and fail at workspace creation.
echo "Bringing up stress + postgres containers (clean start)..."
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" up -d --build

echo ""
echo "Running tigerfs-stress inside the stress container..."
echo "Dumps on host: $HOST_OUT/"
echo ""

# `exec -T`: no TTY, so signal handling and piped output behave correctly.
# Conditional expansion keeps `set -u` happy when no extra args were passed.
docker compose -f "$COMPOSE_FILE" exec -T \
    -e TIGERFS_STRESS_INFO_FILE=/out/tigerfs-stress.info \
    stress /usr/local/bin/tigerfs-stress start \
        --external-conn-str "postgres://testundo:testundo@postgres:5432/testundo" \
        --tigerfs-binary /usr/local/bin/tigerfs \
        --mountpoint-dir /work/mnt \
        --dump-dir /out \
        ${ARGS[@]+"${ARGS[@]}"}
