#!/bin/bash
#
# Run TigerFS integration tests inside a Docker container with native FUSE.
#
# Usage:
#   ./scripts/test-docker.sh                      # Run all integration tests
#   ./scripts/test-docker.sh -run TestPipeline     # Run specific tests
#   ./scripts/test-docker.sh -v -timeout 10m       # Pass extra go test flags
#   ./scripts/test-docker.sh -out results.log      # Custom log file
#

set -euo pipefail

COMPOSE_FILE="test/docker/docker-compose.test.yml"
LOGFILE="/tmp/tigerfs-tests-docker.log"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Parse our flags (strip -out before passing rest to go test)
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        -out)
            LOGFILE="$2"
            shift 2
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

cd "$PROJECT_ROOT"

cleanup() {
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}
trap cleanup EXIT

echo "Logging test output to $LOGFILE"

if [ ${#ARGS[@]} -gt 0 ]; then
    # Extra args provided: pass them as go test flags before the test path
    docker compose -f "$COMPOSE_FILE" run --build --rm test "${ARGS[@]}" ./test/integration/... 2>&1 | tee "$LOGFILE"
else
    # No args: use the default CMD from the Dockerfile
    docker compose -f "$COMPOSE_FILE" up --build --abort-on-container-exit --exit-code-from test 2>&1 | tee "$LOGFILE"
fi
