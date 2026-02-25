# History

Automatic versioning for synthesized apps — every edit and delete is captured as a timestamped snapshot you can browse and recover.

## What It Does

When history is enabled on a synthesized app, a PostgreSQL BEFORE trigger automatically copies the old row into a companion history table on every UPDATE and DELETE. Past versions appear as read-only files under a `.history/` directory alongside your current files.

- **Automatic** — no manual save or commit; every change is captured
- **Read-only** — `.history/` cannot be written to (returns EACCES)
- **Composable** — works with any synth format (markdown, text, future formats)
- **Add anytime** — enable at creation or add to an existing app later

## Quick Start

Enable history when creating a new app:

```bash
echo "markdown,history" > /mnt/db/.build/notes
```

Or add history to an existing app:

```bash
echo "history" > /mnt/db/.build/notes
```

Both paths store the feature flag in the view comment (`tigerfs:md,history`).

## Browsing History

### List files that have history

```bash
ls /mnt/db/notes/.history/
# hello.md/  meeting-notes.md/
```

Each entry is a directory containing past versions of that file.

### List past versions of a file

```bash
ls /mnt/db/notes/.history/hello.md/
# .id  2026-02-24T150000Z  2026-02-12T021500Z  2026-02-12T013000Z
```

Returns `.id` (the row UUID) followed by version timestamps, newest first.

### Read a past version

```bash
cat /mnt/db/notes/.history/hello.md/2026-02-12T013000Z
```

Returns the full file content (frontmatter + body) as it existed at that point.

## Version IDs

Version timestamps are extracted from the history row's UUIDv7, formatted as `2006-01-02T150405Z` — filesystem-safe with no colons. Versions are listed newest-first.

Since UUIDv7 encodes millisecond-precision timestamps but the filesystem path uses second precision, sub-second edits may share the same version ID.

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
# List all row UUIDs with history
ls /mnt/db/notes/.history/.by/

# List all versions for a specific UUID
ls /mnt/db/notes/.history/.by/a1b2c3d4-e5f6-7890-abcd-ef1234567890/

# Read a past version by UUID
cat /mnt/db/notes/.history/.by/a1b2c3d4-e5f6-7890-abcd-ef1234567890/2026-02-12T013000Z
```

`.by/` is only available at the root `.history/` level, not in subdirectory `.history/` directories.

## Subdirectory History

Each directory has its own `.history/` scoped to files in that directory:

```bash
# History for files in the tutorials/ directory only
ls /mnt/db/notes/tutorials/.history/

# Versions of a specific file in that directory
ls /mnt/db/notes/tutorials/.history/intro.md/
```

Subdirectory `.history/` does not include `.by/` (UUID browsing is root-level only).

## Recovering a Past Version

Read the old version from `.history/`, then write it back to the current path:

```bash
# Read the version you want to restore
cat /mnt/db/notes/.history/hello.md/2026-02-12T013000Z > /tmp/restore.md

# Write it back to the current file
cp /tmp/restore.md /mnt/db/notes/hello.md
```

The restored content becomes the current version, and the overwritten content is captured as a new history entry.

## Use Cases

### Shared Agent Memory

Multiple AI agents read and write to the same directory. If one agent accidentally overwrites another's work, browse `.history/` to see what changed and recover the lost content.

### Blog Audit Trail

Track every edit to published content. Compare past versions to see what was added, removed, or reworded.

### Knowledge Base Versioning

Maintain a living knowledge base where articles are frequently updated. History provides a full audit trail without manual version management.

## How It Works

- **Companion table** — Each history-enabled app gets a `_<name>_history` table (e.g., `_notes_history`) that mirrors the source table's columns plus history metadata (`_history_id`, `_operation`)
- **Trigger** — A PostgreSQL BEFORE UPDATE/DELETE trigger copies the OLD row into the history table on every change
- **TimescaleDB hypertable** — The history table is partitioned by `_history_id` (UUIDv7) with 1-month chunks, compressed after 1 day, using `segment_by='filename'` and `order_by='_history_id DESC'`
- **Detection** — TigerFS detects history via the view comment (`tigerfs:md,history`) or by checking for a companion `_<name>_history` table

## Requirements

History requires **TimescaleDB** — it will not work on vanilla PostgreSQL. The companion history table uses TimescaleDB hypertables for efficient time-partitioned storage and automatic compression.

## Quick Reference

| Goal | Path |
|------|------|
| List files with history | `ls mount/app/.history/` |
| List versions of a file | `ls mount/app/.history/file.md/` |
| Read a past version | `cat mount/app/.history/file.md/<timestamp>` |
| Get row UUID | `cat mount/app/.history/file.md/.id` |
| List all row UUIDs | `ls mount/app/.history/.by/` |
| Versions by UUID | `ls mount/app/.history/.by/<uuid>/` |
| Read version by UUID | `cat mount/app/.history/.by/<uuid>/<timestamp>` |
| Subdirectory history | `ls mount/app/subdir/.history/` |
| Restore old version | Read from `.history/`, write to current path |
