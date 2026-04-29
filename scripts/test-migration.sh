#!/bin/bash
# test-migration.sh -- Spin up a PostgreSQL container with OLD-format synth data,
# then print the connection string for testing `tigerfs migrate`.
#
# Usage:
#   ./scripts/test-migration.sh          # start container + load data
#   ./scripts/test-migration.sh stop     # stop and remove container
#
# After running, test with:
#   tigerfs migrate "postgres://demo:demo@localhost:5433/demo" --describe
#   tigerfs migrate "postgres://demo:demo@localhost:5433/demo" --dry-run
#   tigerfs migrate "postgres://demo:demo@localhost:5433/demo"
#
# Uses port 5433 to avoid conflicting with any existing PostgreSQL on 5432.

set -e

CONTAINER_NAME="tigerfs-migration-test"
PORT=5433
USER=demo
PASS=demo
DB=demo
CONNSTR="postgres://$USER:$PASS@localhost:$PORT/$DB"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}==>${NC} $1"; }
error() { echo -e "${RED}==>${NC} $1"; }

# ============================================================================
# Stop command
# ============================================================================
if [ "${1:-}" = "stop" ]; then
    info "Stopping container $CONTAINER_NAME..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
    info "Done."
    exit 0
fi

# ============================================================================
# Check if already running
# ============================================================================
if docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
    warn "Container $CONTAINER_NAME already running on port $PORT"
    info "Connection string: $CONNSTR"
    info "To stop: $0 stop"
    exit 0
fi

# Clean up any stopped container with same name
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

# ============================================================================
# Start PostgreSQL container
# ============================================================================
info "Starting PostgreSQL container on port $PORT..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$PORT:5432" \
    -e POSTGRES_USER="$USER" \
    -e POSTGRES_PASSWORD="$PASS" \
    -e POSTGRES_DB="$DB" \
    timescale/timescaledb-ha:pg18 > /dev/null

info "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
    if docker exec "$CONTAINER_NAME" pg_isready -U "$USER" -d "$DB" > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ! docker exec "$CONTAINER_NAME" pg_isready -U "$USER" -d "$DB" > /dev/null 2>&1; then
    error "PostgreSQL did not become ready in 30 seconds"
    docker logs "$CONTAINER_NAME"
    exit 1
fi

info "PostgreSQL is ready."

# ============================================================================
# Load OLD-format synth data (pre-ADR-017)
# ============================================================================
info "Loading old-format synth data..."

docker exec -i "$CONTAINER_NAME" psql -U "$USER" -d "$DB" << 'ENDSQL'

-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create tigerfs schema
CREATE SCHEMA IF NOT EXISTS tigerfs;

-- ============================================================================
-- App 1: docs (markdown with history) -- OLD SCHEMA (no parent_id)
-- ============================================================================

-- Source table: old format (UNIQUE on filename+filetype, no parent_id)
CREATE TABLE tigerfs.docs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    title TEXT,
    author TEXT,
    headers JSONB DEFAULT '{}'::jsonb,
    body TEXT,
    encoding TEXT NOT NULL DEFAULT 'utf8' CHECK (encoding IN ('utf8', 'base64')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
);

-- View with tigerfs comment
CREATE VIEW public.docs AS SELECT * FROM tigerfs.docs;
COMMENT ON VIEW public.docs IS 'tigerfs:md,history';

-- Modified_at trigger
CREATE FUNCTION tigerfs.set_docs_modified_at() RETURNS TRIGGER AS $$
BEGIN NEW.modified_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_docs_modified_at BEFORE UPDATE ON tigerfs.docs
    FOR EACH ROW EXECUTE FUNCTION tigerfs.set_docs_modified_at();

-- History table: old column names (id, _history_id, _operation)
CREATE TABLE tigerfs.docs_history (
    id UUID,
    filename TEXT NOT NULL,
    filetype TEXT,
    title TEXT,
    author TEXT,
    headers JSONB,
    body TEXT,
    encoding TEXT,
    created_at TIMESTAMPTZ,
    modified_at TIMESTAMPTZ,
    _history_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    _operation TEXT NOT NULL
);

-- Old-style archive trigger
CREATE FUNCTION tigerfs.archive_docs_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO tigerfs.docs_history
        (id, filename, filetype, title, author, headers, body, encoding,
         created_at, modified_at, _history_id, _operation)
    VALUES
        (OLD.id, OLD.filename, OLD.filetype, OLD.title, OLD.author, OLD.headers,
         OLD.body, OLD.encoding, OLD.created_at, OLD.modified_at,
         uuidv7(), TG_OP::text);
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_docs_history_archive BEFORE UPDATE OR DELETE ON tigerfs.docs
    FOR EACH ROW EXECUTE FUNCTION tigerfs.archive_docs_history();

-- Old-style log table (history_id, insert/update/delete types)
CREATE TABLE tigerfs.docs_log (
    log_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    user_id TEXT,
    type TEXT NOT NULL CHECK (type IN ('insert', 'update', 'delete', 'undo')),
    file_id UUID NOT NULL,
    filename TEXT NOT NULL,
    history_id UUID,
    description TEXT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'log_id',
    tsdb.chunk_interval = '7 days',
    tsdb.segmentby = 'file_id',
    tsdb.orderby = 'log_id ASC'
);
CREATE INDEX idx_docs_log_by_file ON tigerfs.docs_log (file_id, log_id ASC);

-- Savepoint table
CREATE TABLE tigerfs.docs_savepoint (
    savepoint_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    user_id TEXT,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- ============================================================================
-- Insert data with OLD path-encoded filenames (slashes in filename column)
-- ============================================================================

-- Root-level files
INSERT INTO tigerfs.docs (filename, filetype, title, body)
VALUES ('readme.md', 'file', 'Welcome', '# Welcome to Docs');

-- Directories (old style: full path as filename)
INSERT INTO tigerfs.docs (filename, filetype)
VALUES ('getting-started', 'directory');
INSERT INTO tigerfs.docs (filename, filetype)
VALUES ('reference', 'directory');

-- Nested files (old style: full path as filename)
INSERT INTO tigerfs.docs (filename, filetype, title, author, body)
VALUES ('getting-started/installation.md', 'file', 'Installation Guide', 'Alice',
        '# Installation\n\nFollow these steps to install.');
INSERT INTO tigerfs.docs (filename, filetype, title, author, body)
VALUES ('getting-started/quick-start.md', 'file', 'Quick Start', 'Bob',
        '# Quick Start\n\nGet up and running in 5 minutes.');
INSERT INTO tigerfs.docs (filename, filetype, title, author, body)
VALUES ('reference/configuration.md', 'file', 'Configuration', 'Alice',
        '# Configuration\n\nAll configuration options explained.');
INSERT INTO tigerfs.docs (filename, filetype, title, author, body)
VALUES ('reference/api-reference.md', 'file', 'API Reference', 'Charlie',
        '# API Reference\n\nComplete API documentation.');

-- Simulate some edits to create history entries
UPDATE tigerfs.docs
SET body = '# Installation\n\nUpdated installation instructions for v2.',
    title = 'Installation Guide (v2)'
WHERE filename = 'getting-started/installation.md';

UPDATE tigerfs.docs
SET body = '# Quick Start\n\nRevised quick start guide.'
WHERE filename = 'getting-started/quick-start.md';

-- Insert log entries with OLD type names
INSERT INTO tigerfs.docs_log (file_id, type, filename)
SELECT id, 'insert', filename FROM tigerfs.docs WHERE filetype = 'file';

INSERT INTO tigerfs.docs_log (file_id, type, filename, history_id)
SELECT d.id, 'update', d.filename, h._history_id
FROM tigerfs.docs d
JOIN tigerfs.docs_history h ON h.id = d.id
WHERE d.filename = 'getting-started/installation.md'
LIMIT 1;

INSERT INTO tigerfs.docs_log (file_id, type, filename, history_id)
SELECT d.id, 'update', d.filename, h._history_id
FROM tigerfs.docs d
JOIN tigerfs.docs_history h ON h.id = d.id
WHERE d.filename = 'getting-started/quick-start.md'
LIMIT 1;

-- Create a savepoint
INSERT INTO tigerfs.docs_savepoint (name, description)
VALUES ('before-v2', 'Savepoint before v2 updates');

-- ============================================================================
-- App 2: snippets (plain text, no history) -- OLD SCHEMA
-- ============================================================================

CREATE TABLE tigerfs.snippets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    body TEXT,
    encoding TEXT NOT NULL DEFAULT 'utf8' CHECK (encoding IN ('utf8', 'base64')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
);

CREATE VIEW public.snippets AS SELECT * FROM tigerfs.snippets;
COMMENT ON VIEW public.snippets IS 'tigerfs:txt';

-- Flat files (no hierarchy)
INSERT INTO tigerfs.snippets (filename, body) VALUES ('todo.txt', 'Buy groceries');
INSERT INTO tigerfs.snippets (filename, body) VALUES ('notes.txt', 'Meeting at 3pm');

-- ============================================================================
-- Summary
-- ============================================================================

DO $$
DECLARE
    doc_count INT;
    hist_count INT;
    log_count INT;
    snippet_count INT;
BEGIN
    SELECT count(*) INTO doc_count FROM tigerfs.docs;
    SELECT count(*) INTO hist_count FROM tigerfs.docs_history;
    SELECT count(*) INTO log_count FROM tigerfs.docs_log;
    SELECT count(*) INTO snippet_count FROM tigerfs.snippets;
    RAISE NOTICE 'Loaded: % docs rows, % history entries, % log entries, % snippets',
        doc_count, hist_count, log_count, snippet_count;
END $$;

ENDSQL

echo ""
info "Old-format data loaded successfully."
echo ""
info "Data summary:"
info "  docs: 7 rows (2 directories + 5 files), full-path filenames"
info "  docs_history: 2 entries (old column names: id, _history_id, _operation)"
info "  docs_log: 7 entries (old types: insert, update; old column: history_id)"
info "  snippets: 2 rows (flat, no history)"
echo ""
info "To test migration:"
echo ""
echo "  # Check what needs migrating:"
echo "  tigerfs migrate \"$CONNSTR\" --describe"
echo ""
echo "  # Preview the SQL:"
echo "  tigerfs migrate \"$CONNSTR\" --dry-run"
echo ""
echo "  # Run the migration:"
echo "  tigerfs migrate \"$CONNSTR\""
echo ""
echo "  # Verify (should say 'No pending migrations'):"
echo "  tigerfs migrate \"$CONNSTR\" --describe"
echo ""
echo "  # Mount and verify:"
echo "  tigerfs mount \"$CONNSTR\" /tmp/mig-test"
echo "  ls /tmp/mig-test/public/docs/"
echo "  ls /tmp/mig-test/public/docs/getting-started/"
echo "  cat /tmp/mig-test/public/docs/getting-started/installation.md"
echo ""
info "To stop: $0 stop"
