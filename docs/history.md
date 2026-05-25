# History, Savepoints, and Undo

Automatic versioning for file-first workspaces -- every edit and delete is captured as a timestamped snapshot you can browse, diff, and undo.

## What It Does

When history is enabled on a workspace, every change is automatically tracked. The filesystem exposes four directories for browsing and managing your workspace's history:

```
notes/
├── hello.md                     # Your current files
├── tutorials/
│   └── getting-started.md
├── .history/                    # Browse past versions of any file
│   ├── hello.md/
│   │   ├── .id
│   │   └── 2026-02-24T150000.123Z-abc123def
│   └── .by/                    # Look up history by row UUID
├── .log/                        # Operation log: what changed, when, by whom
│   └── <log_id>/
│       ├── before → .history/...   # Diff symlinks
│       ├── after → .history/...
│       └── current → hello.md
├── .savepoint/                  # Named bookmarks for undo
│   ├── before-refactor/
│   └── auto-agent-7-20260408T143000Z/
└── .undo/                       # Preview and apply undo
    ├── id/                      # Undo a single operation
    ├── to-id/                   # Undo to a log entry
    └── to-savepoint/            # Undo to a savepoint
```

Key properties:
- **Automatic** -- no manual save or commit; every change is captured
- **Reversible** -- undo any change or roll back to any savepoint
- **Attributable** -- each operation records who made it (via `--user-id`)
- **Composable** -- works with any workspace format (markdown, text)
- **Add anytime** -- enable at creation or add to an existing workspace

## Quick Start

```bash
# Create a workspace with history
echo "markdown,history" > /mnt/db/.build/notes

# Create a savepoint before risky work
echo '{"description":"Before refactoring"}' > /mnt/db/notes/.savepoint/before-refactor.json

# Work, explore, make changes...

# Review what changed
diff -ru /mnt/db/notes/.undo/to-savepoint/before-refactor /mnt/db/notes/ -x '.*'

# Undo if needed (atomic, all files at once)
touch /mnt/db/notes/.undo/to-savepoint/before-refactor/.apply
```

Or add history to an existing workspace:

```bash
echo "history" > /mnt/db/.build/notes
```

Both paths store the feature flag in the view comment (`tigerfs:md,history`).

## Browsing History (.history/)

### List files that have history

```bash
ls /mnt/db/notes/.history/
# hello.md/  meeting-notes.md/
```

Each entry is a directory containing past versions of that file.

### List past versions of a file

```bash
ls /mnt/db/notes/.history/hello.md/
# .id  2026-02-24T150000.123Z-abc123def  2026-02-12T021500.456Z-xyz789ghi  2026-02-12T013000.789Z-jkl012mno
```

Returns `.id` (the row UUID) followed by version timestamps, newest first.

### Read a past version

```bash
cat /mnt/db/notes/.history/hello.md/2026-02-12T013000.789Z-jkl012mno
```

Returns the full file content (frontmatter + body) as it existed at that point.

## Version IDs

Version IDs use the UUIDv7 display format: `2026-02-24T150000.123Z-abc123def` (UTC timestamp with millisecond precision + base36 entropy suffix). This format is filesystem-safe, case-insensitive, lossless, and sorts chronologically. Versions are listed newest-first.

## Cross-Rename Tracking

Every row has a stable UUID that persists across renames. If you rename `hello.md` to `intro.md`, the UUID stays the same and all history follows it.

### Discover a file's UUID

```bash
cat /mnt/db/notes/.history/hello.md/.id
# a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

### Browse by UUID

The `.by/` directory lets you look up history by row UUID, which works even after renames:

```bash
ls /mnt/db/notes/.history/.by/
ls /mnt/db/notes/.history/.by/a1b2c3d4-e5f6-7890-abcd-ef1234567890/
cat /mnt/db/notes/.history/.by/a1b2c3d4-e5f6-7890-abcd-ef1234567890/2026-02-12T013000.789Z-jkl012mno
```

`.by/` is only available at the root `.history/` level, not in subdirectory `.history/` directories.

## Subdirectory History

Each directory has its own `.history/` scoped to files in that directory:

```bash
ls /mnt/db/notes/tutorials/.history/
ls /mnt/db/notes/tutorials/.history/intro.md/
```

Subdirectory `.history/` does not include `.by/` (UUID browsing is root-level only).

## Operation Log (.log/)

Every create, edit, rename, and delete is recorded in the `.log/` directory. Each entry has a log_id (UUIDv7), the operation type, affected file, and the user who performed it.

```bash
# Recent operations
ls /mnt/db/notes/.log/.last/10/
cat /mnt/db/notes/.log/.last/10/.export/json

# Filter by user or type
ls /mnt/db/notes/.log/.by/user_id/agent-7/.last/5/
ls /mnt/db/notes/.log/.by/type/edit/.last/10/
```

### Diff Symlinks

Each log entry directory contains `before`, `after`, and `current` symlinks for diffing:

```bash
# What did this edit change?
diff -u --color /mnt/db/notes/.log/<id>/before /mnt/db/notes/.log/<id>/after

# How has the file drifted since this edit?
diff -u --color /mnt/db/notes/.log/<id>/before /mnt/db/notes/.log/<id>/current
```

- **before**: the file content before this operation (from `.history/`). `/dev/null` for creates.
- **after**: the file content after this operation. `/dev/null` for deletes.
- **current**: the live file. `/dev/null` if deleted.

History symlink paths are per-directory: a file at `tutorials/getting-started.md` links to `tutorials/.history/getting-started.md/<version>`.

### Relationship to .history/

The log records *what happened* (operations). History stores *what it looked like* (content snapshots). Each log entry's `version_id` points to the history row that captured the file's state just before that operation.

## Savepoints (.savepoint/)

Named bookmarks in the operation log timeline. Create one before risky work so you can undo back to that point.

```bash
# Create a savepoint
echo '{"description":"Before investigating auth bug"}' > /mnt/db/notes/.savepoint/pre-investigation.json

# List savepoints
ls /mnt/db/notes/.savepoint/

# Read savepoint details
cat /mnt/db/notes/.savepoint/pre-investigation/description

# Delete a savepoint (does not affect history or log)
rm /mnt/db/notes/.savepoint/pre-investigation
```

Savepoint creation requires a format suffix (`.json`, `.tsv`, `.csv`, `.yaml`). If `--user-id` is set, the user identity is auto-injected.

### Auto-Savepoints

TigerFS automatically creates savepoints when it detects an inactivity gap (default 30 minutes). Named `auto-<user>-<timestamp>` or `auto-<timestamp>` for anonymous mounts.

Configure via `--auto-savepoint-interval` (set to `0` to disable).

## Undo (.undo/)

The `.undo/` directory provides a preview-then-apply interface for reversing operations.

### Three Modes

| Mode | Purpose |
|------|---------|
| `.undo/id/<log_id>/` | Undo a single operation |
| `.undo/to-id/<log_id>/` | Undo all operations after a log entry |
| `.undo/to-savepoint/<name>/` | Undo all operations after a savepoint |

### Preview and Apply

```bash
# What would undo do?
cat /mnt/db/notes/.undo/to-savepoint/pre-investigation/.info/summary

# Diff all affected files since savepoint
diff -ru /mnt/db/notes/.undo/to-savepoint/pre-investigation /mnt/db/notes/ -x '.*'

# Apply undo (atomic, all files at once)
touch /mnt/db/notes/.undo/to-savepoint/pre-investigation/.apply

# Undo only a specific agent's changes
touch /mnt/db/notes/.undo/to-savepoint/pre-investigation/.by/user_id/agent-7/.apply

# Diff and undo a single file change
diff -u --color /mnt/db/notes/.log/<id>/before /mnt/db/notes/.log/<id>/current
touch /mnt/db/notes/.undo/id/<id>/.apply
```

### Undo of Undo

Undo operations are themselves logged (with type='undo'). You can undo an undo. Create a savepoint before a major undo for extra safety.

## Recovering Past Versions

**Single file via undo (preferred):** Find the change in the log, then undo it:

```bash
cat /mnt/db/notes/.log/.by/filename/hello.md/.last/5/.export/json
touch /mnt/db/notes/.undo/id/<log_id>/.apply
```

**Multi-file rollback:** Undo all changes since a savepoint:

```bash
touch /mnt/db/notes/.undo/to-savepoint/before-investigation/.apply
```

## Limitations

- **Requires TimescaleDB:** history, log, and savepoint tables use TimescaleDB hypertables for compressed storage. Will not work on vanilla PostgreSQL.
- **File-first only:** data-first tables don't get history. Writing directly to the backing table via `.tables/` bypasses the history trigger.
- **Per-user undo caveat:** if two users interleave edits on the same file, undoing one user's changes also reverts the other user's interleaved edits on that file.
- **Post-migration undo boundary:** workspaces upgraded from v0.6 cannot undo entries created before `tigerfs migrate` ran. Pre-migration history rows have lossy `parent_id` information after the migration, so undo of an old edit could leave the file in the wrong directory with no signal. Pre-migration entries remain readable in `.log/` and `.history/`; only `.undo/` refuses (with `EPERM` and a hint). Fresh v0.7 installs are unaffected.
- **Storage cost:** every edit creates a history row. TimescaleDB compression mitigates this (7-day chunks, automatic compression).

## How It Works

Each history-enabled workspace is backed by four companion tables in the `tigerfs` schema, alongside the main backing table.

The **history table** stores a snapshot of every file version. A PostgreSQL BEFORE trigger fires on every update and delete, copying the old row into history with a UUIDv7 version_id. The trigger detects whether the operation was an edit, rename, or delete by comparing the old and new rows.

The **log table** records each operation (create, edit, rename, delete, undo) with who did it and when. Log entries reference history entries via version_id, connecting "what happened" to "what it looked like."

The **savepoint table** stores named bookmarks. Each savepoint's UUIDv7 timestamp enables efficient "find all operations after this point" queries.

The **metadata table** records non-operational events about the workspace itself -- format-migration markers and future system events. The undo engine consults it to refuse undo across format boundaries (see Limitations).

History, log, and savepoint use TimescaleDB hypertables for time-partitioned storage and automatic compression. The metadata table is a regular table (O(10) rows per database lifetime). The log table is indexed on `(file_id, log_id)` for SkipScan-optimized undo queries.

TigerFS detects history via the view comment (`tigerfs:md,history`) or by checking for a companion history table.

## Quick Reference

| Goal | Path |
|------|------|
| List files with history | `ls workspace/.history/` |
| List versions of a file | `ls workspace/.history/file.md/` |
| Read a past version | `cat workspace/.history/file.md/<timestamp>` |
| Get row UUID | `cat workspace/.history/file.md/.id` |
| Versions by UUID | `ls workspace/.history/.by/<uuid>/` |
| Recent log entries | `cat workspace/.log/.last/10/.export/json` |
| Diff a specific change | `diff -u workspace/.log/<id>/before workspace/.log/<id>/after` |
| Create savepoint | `echo '{"description":"..."}' > workspace/.savepoint/name.json` |
| List savepoints | `ls workspace/.savepoint/` |
| Preview undo to savepoint | `cat workspace/.undo/to-savepoint/name/.info/summary` |
| Diff all since savepoint | `diff -ru workspace/.undo/to-savepoint/name workspace/ -x '.*'` |
| Undo to savepoint | `touch workspace/.undo/to-savepoint/name/.apply` |
| Undo single change | `touch workspace/.undo/id/<log_id>/.apply` |
| Per-user undo | `touch workspace/.undo/to-savepoint/name/.by/user_id/X/.apply` |
