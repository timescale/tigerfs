#!/bin/bash
#
# Run TigerFS integration tests on macOS with native NFS.
#
# Requires:
#   - macOS (exits with error on other platforms)
#   - Local PostgreSQL (via Homebrew, Postgres.app, etc.)
#   - /sbin/mount_nfs (ships with macOS)
#
# Usage:
#   ./scripts/test-macos.sh                      # Run all integration tests
#   ./scripts/test-macos.sh -run TestWriteDDL     # Run specific tests
#   ./scripts/test-macos.sh -v -timeout 10m       # Pass extra go test flags
#   ./scripts/test-macos.sh -out results.log      # Custom log file
#

set -euo pipefail

if [ "$(uname -s)" != "Darwin" ]; then
    echo "Error: this script only runs on macOS (detected: $(uname -s))" >&2
    exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOGFILE="/tmp/tigerfs-tests-macos.log"

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

export TEST_MOUNT_METHOD=nfs

echo "Logging test output to $LOGFILE"

if [ ${#ARGS[@]} -gt 0 ]; then
    go test "${ARGS[@]}" ./test/integration/... 2>&1 | tee "$LOGFILE"
else
    go test -v -timeout 5m ./test/integration/... 2>&1 | tee "$LOGFILE"
fi
