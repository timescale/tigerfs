# ADR-016: Undo and Recovery

**Status:** Accepted
**Date:** 2026-04-07
**Author:** Mike Freedman

## Context

TigerFS users and agents make changes to files backed by PostgreSQL. When mistakes happen -- a user edits the wrong file, or an agent makes a series of exploratory changes during debugging -- there's no way to revert. Today, recovery depends on either the agent manually undoing its changes or having a clean git commit to fall back on.

TigerFS already has a **history** system for synth apps: a PostgreSQL BEFORE trigger captures the old row state into a companion hypertable on every UPDATE/DELETE. This design builds on that foundation to add structured undo operations, savepoints, and an operation log -- all exposed through the filesystem.

**Scope:** This feature applies only to synth app tables with history enabled. Native/data-first tables and synth apps without history are not affected.

---

## 1. Operation Log

### 1.1 Purpose

The operation log records every data change (INSERT, UPDATE, DELETE, UNDO) made to a synth app table. It provides:

- An audit trail of what changed, when, and by whom
- The ordering information needed to undo operations
- Pointers to the history table for retrieving before-states

### 1.2 Schema

One log table per synth app with history enabled, stored in the `tigerfs` schema (hidden from top-level `ls`):

```sql
CREATE TABLE tigerfs.<app>_log (
    log_id      UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    file_id     UUID NOT NULL,
    type        TEXT NOT NULL CHECK (type IN ('create', 'edit', 'rename', 'delete', 'undo')),
    user_id     TEXT,
    filename    TEXT NOT NULL,
    version_id  UUID,
    description TEXT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'log_id',
    tsdb.chunk_interval = '7 days',
    tsdb.segmentby = 'file_id',
    tsdb.orderby = 'log_id ASC'
);
```

| Column | Description |
|--------|-------------|
| `log_id` | UUIDv7 primary key. Time-ordered, used as hypertable partition key. Timestamp extractable via TimescaleDB's `uuid_v7_to_timestamptz()` (works on PG17; PG18+ has built-in `uuid_extract_timestamp()`). |
| `file_id` | Stable UUID of the affected row (the `id` column in the synth app table). Persists across renames and moves. |
| `type` | Operation type. Filesystem-centric names (ADR-017): `create` (new file/directory), `edit` (content change), `rename` (name or parent_id change), `delete` (removal); `undo` for undo operations. |
| `user_id` | Identity of the user/agent making the change. NULL in single-user/anonymous mode. Set from `.info/user` at the mount root. |
| `filename` | Denormalized full path at the time of the operation (e.g., `projects/web/todo.md`). Computed from the parent-pointer chain at log-write time. Historically correct -- records what the file was called when the operation happened, even if later renamed or moved. Note: the log's `filename` stores the full path; the source table's `filename` stores only the leaf name (ADR-017). |
| `version_id` | Pointer to the history table row (`version_id`) containing the before-state. NULL for `create` operations (no before-state exists). For `edit`/`rename`/`delete`/`undo`, this points to the row captured by the BEFORE trigger. |
| `description` | Optional human-readable note about the operation. |

### 1.3 Hypertable Configuration

The log table uses the modern `CREATE TABLE WITH` syntax (see schema above) which combines hypertable creation, chunk interval, segmentby, and orderby in a single DDL statement. TimescaleDB automatically creates a columnstore compression policy matching the chunk interval.

### 1.4 Indexes

A composite index on `(file_id, log_id)` enables SkipScan for the undo query pattern:

```sql
CREATE INDEX ON tigerfs.<app>_log (file_id, log_id ASC);
```

The `file_id` leading column enables SkipScan for the `DISTINCT ON (file_id) ... ORDER BY file_id, log_id ASC` undo query pattern.

### 1.5 Compression

Compression is configured inline via the `CREATE TABLE WITH` syntax: `tsdb.segmentby = 'file_id'` and `tsdb.orderby = 'log_id ASC'`. TimescaleDB automatically creates a compression policy using the chunk interval.

**Design decisions:**

- **`segmentby = 'file_id'`**: Each file gets its own compressed segment. This aligns with SkipScan on compressed hypertables -- SkipScan jumps between segments by `file_id`, reading only the first matching entry per segment. The trade-off is slightly lower compression ratio for tables with many files (many small segments), but log rows are small (UUIDs and short text), so this is acceptable.

- **`orderby = 'log_id ASC'`**: Matches the `ORDER BY file_id, log_id ASC` in the undo query, allowing SkipScan to work on compressed chunks without reordering.

### 1.6 How Log Entries Are Created

Log entries are created by TigerFS's write path, not by database triggers. When `WriteFile`, `Delete`, or `Rename` executes a DML operation on a history-enabled synth app table, it also inserts a log entry in the same database round-trip (or transaction, for undo operations).

The log entry captures:
- The `file_id` from the row being modified
- The `filename` as the denormalized full path (computed from the parent-pointer chain)
- The `version_id` from the history entry created by the BEFORE trigger (for edit/rename/delete). For `create`, `version_id` is NULL because there is no before-state.

**Determining `version_id` for edit/rename/delete:** The BEFORE trigger inserts a row into the history table with a new UUIDv7 `version_id`. To capture this in the log, the write operation queries the most recent history entry for the file immediately after the DML. Since the trigger fires synchronously before the DML completes, the history row exists by the time the log insert runs.

### 1.7 Relationship Between Log and History

The log and history tables serve complementary purposes:

| | History Table | Log Table |
|---|---|---|
| **What it stores** | Full row state (before-state of every change) | Operation metadata (what happened, who, when, pointer to before-state) |
| **Created by** | PostgreSQL BEFORE trigger (automatic) | TigerFS write path (application-level) |
| **Used for** | Reading old file content, restoring state | Ordering operations, undo sequencing, audit trail |
| **Schema** | Matches the synth app's columns + `version_id`, `operation` | Fixed schema (log_id, file_id, type, user_id, filename, version_id, description) |

The log does not duplicate the file content. It stores a `version_id` pointer to the history row that contains the full before-state. This keeps the log table small (just UUIDs and metadata) while leveraging the history table's existing compressed storage for file content.

### 1.8 SkipScan Optimization

TimescaleDB's SkipScan is a custom executor node that transforms `DISTINCT ON` queries from O(N) to O(K x log N), where K is the number of distinct values and N is total rows. It works by hopping through the B-tree index from one distinct value to the next, rather than scanning every row.

The undo execution query uses `DISTINCT ON (file_id)` to find the first log entry per affected file:

```sql
SELECT DISTINCT ON (file_id) file_id, type, version_id
FROM tigerfs.<app>_log
WHERE log_id > $1
ORDER BY file_id, log_id ASC
```

With the `(file_id, log_id ASC)` index and `segmentby = 'file_id'`, SkipScan activates on both uncompressed and compressed chunks.

The same `DISTINCT ON` query is used for both the undo execution and the preview summary (Section 4.3.8). One query, one code path, SkipScan throughout.

**Requirements:**
- TimescaleDB >= 2.20.0 (for compressed-chunk SkipScan)
- PostgreSQL >= 16
- The `(file_id, log_id ASC)` index with `file_id` as the leading column
- `segmentby = 'file_id'` for compressed chunks

**Verification:** Run `EXPLAIN ANALYZE` on the undo query and look for `Custom Scan (SkipScan)` nodes.

### 1.9 Why No `after_id` / `after_state`

The log stores only a `version_id` pointing to the before-state. There is no `after_id` pointing to the after-state. This is a deliberate design choice:

1. **The after-state doesn't exist as a history entry at log-write time.** When an UPDATE executes, the BEFORE trigger fires and creates a history entry (the before-state) -- we know its `version_id`. But the after-state is just the live row in the current table. It won't become a history entry until the *next* operation's trigger fires. So `after_id` can't be populated at the time the log entry is created without either backfilling it retroactively (fragile) or creating an extra history entry (wasteful).

2. **The before-state is sufficient for undo.** Undo restores the before-state. The after-state is never needed for the restore operation itself.

3. **The after-state is derivable from the chain.** For any log entry L, the after-state is either the current row (if L is the latest operation) or the next log entry's `version_id` (the next operation's before-state IS the previous operation's after-state).

---

## 2. Savepoints

### 2.1 Purpose

Savepoints are named bookmarks in time. They let users mark a point ("before-agent-run") and later say "undo everything after this point." Savepoints are cheap to create and carry no operational overhead.

### 2.2 Schema

Savepoints are stored in a **separate table** from the log (not as log entries with `type = 'savepoint'`). One savepoint table per synth app with history enabled:

```sql
CREATE TABLE tigerfs.<app>_savepoint (
    savepoint_id  UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    user_id       TEXT,
    name          TEXT NOT NULL UNIQUE,
    description   TEXT
);
```

This is a regular PostgreSQL table, not a hypertable. Savepoints are small (tens to hundreds, not millions) and don't need time-series features.

**Why separate from the log:**

1. **Clean schemas.** The log has `file_id`, `version_id`, `filename` (operation-specific). Savepoints have `name`, `description` (bookmark-specific). Combining them means every row wastes half its columns as NULLs.

2. **Natural constraints.** `name UNIQUE` is straightforward on a dedicated table. On the log table, it would require a partial unique index (`WHERE type = 'savepoint'`).

3. **No compression concerns.** The savepoint table is tiny and doesn't need hypertable features. Mixing savepoints into a compressed hypertable segmented by `file_id` is awkward because savepoints have no `file_id`.

4. **Simpler queries.** `SELECT * FROM notes_savepoint WHERE name = 'foo'` vs `SELECT * FROM notes_log WHERE type = 'savepoint' AND name = 'foo'`.

**UUIDv7 ordering is preserved across tables.** Both the log and savepoint tables use `uuidv7()` defaults generated from the same clock. A savepoint created at time T has a `savepoint_id` less than any log entry created after T. So `WHERE log_id > savepoint_id` correctly identifies "operations after this savepoint."

### 2.3 Filesystem Interface

Savepoints are exposed as a data-first directory under each synth app table. The `.savepoint/` path is an alias for the `tigerfs.<app>_savepoint` table, using existing pipeline machinery.

**Creating a savepoint:**
```bash
echo "Starting agent exploration" > notes/.savepoint/before-exploration
# Inserts: savepoint_id=uuidv7(), name='before-exploration', description='Starting agent exploration'

touch notes/.savepoint/quick-mark
# Inserts with NULL description
```

**Listing savepoints:**
```bash
ls notes/.savepoint/
# before-exploration/
# quick-mark/
```

**Reading savepoint data (rows-as-directories):**
```bash
cat notes/.savepoint/before-exploration/description
# Starting agent exploration

cat notes/.savepoint/before-exploration/savepoint_id
# 019590a0-1234-7000-8000-000000000001

cat notes/.savepoint/before-exploration.json
# {"savepoint_id":"019590a0-...","name":"before-exploration","description":"Starting agent exploration","user_id":null}
```

**Updating:**
```bash
echo "Updated description" > notes/.savepoint/before-exploration/description
# Updates the description column

mv notes/.savepoint/before-exploration notes/.savepoint/before-foo
# UPDATE notes_savepoint SET name='before-foo' WHERE name='before-exploration'
# The savepoint_id is unchanged, so undo-to-savepoint references still work
```

**Deleting:**
```bash
rm notes/.savepoint/before-exploration
# DELETE FROM notes_savepoint WHERE name='before-exploration'
# Only removes the bookmark. All log entries remain intact.
# Undo is still possible by log_id, just not by this savepoint name.
```

**Pipeline queries (reusing data-first infrastructure):**
```bash
ls notes/.savepoint/.last/5                    # 5 most recent
ls notes/.savepoint/.by/user_id/agent-7        # by user
cat notes/.savepoint/.export/json              # all as JSON
cat notes/.savepoint/*.json                    # all as JSON (format suffix)
```

### 2.4 Implementation: Reusing Data-First Mode

When the path parser sees `.savepoint/`, it creates an `FSContext` targeting `tigerfs.<app>_savepoint` with `name` as the display filename. All pipeline operations (`.by/`, `.last/`, `.first/`, `.order/`, `.export/`, `.columns/`, format suffixes) are handled by existing data-first pipeline code.

The only special behaviors:
- The directory uses `name` as the row identifier (filename), not the PK (`savepoint_id`)
- Write operations (create, update description, rename, delete) map to INSERT/UPDATE/DELETE on the savepoint table

---

## 3. Undo Operations

### 3.1 Semantics: Rollback, Not Revert

Undo operations use **rollback semantics** (like `git checkout`), not **revert semantics** (like `git revert`).

- **Rollback:** Restore a file (or all files) to a specific prior state. Discards all changes after that point for the affected files.
- **Revert:** Surgically reverse one specific change while preserving later changes. Requires column-level diffing and three-way merge logic.

Rollback is the right model for TigerFS because:
1. History stores full row state, not column-level diffs. Merging partial changes would require complex diff/merge logic.
2. The primary use cases ("undo a mistake," "restore after agent exploration") are rollback operations.
3. Surgical revert can be done at the tool/agent level if needed -- read the history versions, compute the diff, apply a selective edit.

### 3.2 Undo Variants

| Variant | Path | What it does |
|---------|------|-------------|
| Single operation | `.undo/id/<log_id>/` | Undo one operation on one file |
| To a log entry | `.undo/to-id/<log_id>/` | Undo all operations after this log entry |
| To a savepoint | `.undo/to-savepoint/<name>/` | Undo all operations after this savepoint |
| To a savepoint by user | `.undo/to-savepoint/<name>/.by/user_id/<user_id>/` | Undo only this user's operations after the savepoint |
| To a log entry by user | `.undo/to-id/<log_id>/.by/user_id/<user_id>/` | Undo only this user's operations after this log entry |

### 3.3 How Undo Works

**Single operation undo (`.undo/id/<log_id>/`):**

1. Look up the log entry by `log_id`
2. Based on `type`:
   - `create`: DELETE the row (it didn't exist before)
   - `edit`: Fetch the before-state from history (`version_id`), UPDATE the row to that state
   - `rename`: Fetch the before-state from history, UPDATE the row to restore old name/parent
   - `delete`: Fetch the before-state from history, INSERT the row back
   - `undo`: Same as edit/delete -- fetch history state, restore it
3. The BEFORE trigger fires on the restore operation, capturing the current state into history
4. Insert a new log entry with `type = 'undo'`

**Multi-file undo (to savepoint or to log_id):**

The execution query finds the first log entry per affected file after the target point, using `DISTINCT ON` to leverage SkipScan (see Section 1.8):

```sql
SELECT DISTINCT ON (file_id) file_id, type, version_id
FROM tigerfs.<app>_log
WHERE log_id > $1
ORDER BY file_id, log_id ASC
```

Where `$1` is either the `savepoint_id` (for undo-to-savepoint) or the target `log_id` (for undo-to-ID).

**Key insight:** The first log entry's `version_id` for each file IS the state at the target point, by definition. The BEFORE trigger captured the row state just before the first post-target operation.

For each affected file:

| First operation type | Current row state | Action |
|---------------------|-------------------|--------|
| `create` | Exists | DELETE (row didn't exist at target point) |
| `create` | Doesn't exist (deleted later) | No-op (correct -- row didn't exist at target) |
| `edit`/`rename` | Exists | UPSERT from history state |
| `edit`/`rename` | Doesn't exist (deleted later) | INSERT from history state |
| `delete` | Doesn't exist | INSERT from history state |

The UPSERT pattern handles the ambiguity of "does the row currently exist?" for UPDATE entries:

```sql
-- Delete rows inserted after the target point
DELETE FROM public.<app>
WHERE id IN (SELECT file_id FROM affected WHERE first_type = 'create');

-- Restore rows that were edited, renamed/moved, or deleted after the target point.
-- UPSERT must restore parent_id (ADR-017) to reverse moves.
INSERT INTO public.<app> (id, parent_id, filename, title, author, body, ...)
SELECT h.file_id, h.parent_id, h.filename, h.title, h.author, h.body, ...
FROM tigerfs.<app>_history h
JOIN affected a ON a.first_version_id = h.version_id
WHERE a.first_type IN ('edit', 'rename', 'delete')
ON CONFLICT (id) DO UPDATE SET
    parent_id = EXCLUDED.parent_id,
    filename  = EXCLUDED.filename,
    title     = EXCLUDED.title,
    author    = EXCLUDED.author,
    body      = EXCLUDED.body,
    ...;
```

All executed in a **single PostgreSQL transaction**. The BEFORE triggers fire for each restore operation, creating new history entries. New log entries with `type = 'undo'` are also inserted within the transaction.

**By-user variant:** Same query with an additional `WHERE user_id = $2` filter. Only operations by the specified user are undone; other users' changes are preserved.

### 3.4 Undo of Undo

Undo operations are themselves logged (with `type = 'undo'`). Each undo operation fires the BEFORE trigger, which captures the current state into history. The undo log entry's `version_id` points to that captured state.

To undo an undo: look up the undo log entry, fetch its `version_id` from history, restore to that state. This is identical to undoing any other operation -- no special case needed.

**Traced example:**

1. User updates `hello.md` to "v2". Trigger captures "v1" as H1. Log: L1 (type=edit, version_id=H1).
2. User undoes L1. TigerFS restores "v1" from H1. Trigger captures "v2" as H2. Log: L2 (type=undo, version_id=H2).
3. User undoes L2 (undo-of-undo). TigerFS restores "v2" from H2. Trigger captures "v1" as H3. Log: L3 (type=undo, version_id=H3).

Result: `hello.md` is back to "v2". The chain is self-consistent because every write (including undo writes) fires the same trigger.

### 3.5 Idempotent Undo-to-Savepoint

Undoing to the same savepoint twice produces the same data state. On the second undo:

- The query finds all operations after the savepoint, including the undo entries from the first undo
- For each file, the first entry after the savepoint is still the original operation (same `log_id`)
- That original operation's `version_id` still points to the state at the savepoint
- The restore produces the same data (though it creates new log/history entries)

This is correct and expected -- "undo to savepoint S" always means "make the data look like it did at S."

### 3.6 Transaction Safety and Crash Recovery

The entire undo operation (all DELETEs, UPSERTs, and log INSERTs) runs in a single PostgreSQL transaction. If TigerFS crashes mid-undo, PostgreSQL rolls back the transaction automatically. The database stays in the pre-undo state. The user can retry.

For typical agent sessions (tens to hundreds of operations on tens of files), the transaction is small. For very large undo operations (thousands of files), the transaction may hold many row locks and use significant memory, but this is an unusual case.

### 3.7 Concurrency During Undo

If an undo transaction and a concurrent write both modify the same row, PostgreSQL serializes at the row level. The second writer blocks until the first commits.

**Risk:** The undo can overwrite a concurrent change, or a concurrent change can overwrite the restored state. This is inherent in rollback semantics -- the same as any two concurrent writes to the same row.

**Mitigation:** Agents should use separate mounts (see Section 5) so their writes are independently tracked. "Undo to savepoint by user" only reverses one user's operations, preserving others. True conflict detection (optimistic locking) could be added later if needed.

---

## 4. Filesystem Interfaces

### 4.1 `.log/` -- Operation Log

The `.log/` directory exposes the `tigerfs.<app>_log` table through data-first mode. When the path parser sees `.log/`, it creates an `FSContext` targeting the log table and delegates to existing pipeline code.

```bash
# Recent operations (UUIDv7 PKs, so .last gives most recent)
ls notes/.log/.last/10
cat notes/.log/.last/10.json

# Filter by user
ls notes/.log/.by/user_id/agent-7/.last/20

# Filter by file
ls notes/.log/.by/file_id/<uuid>

# Filter by operation type
ls notes/.log/.by/type/delete

# Single entry as directory (column access)
cat notes/.log/<log_id>/type
cat notes/.log/<log_id>/filename
cat notes/.log/<log_id>/version_id

# Bulk export
cat notes/.log/.export/json
```

All pipeline operations (`.by/`, `.last/`, `.first/`, `.order/`, `.export/`, `.columns/`, format suffixes) work through existing data-first pipeline code. No new functionality needed.

#### 4.1.1 Diff Symlinks on Log Entries

Each log entry (as a row-as-directory) exposes three symlinks alongside its data columns:

```
notes/.log/<log_id>/
├── log_id          # column
├── type            # column
├── filename        # column
├── file_id         # column
├── version_id      # column
├── description     # column
├── before -> ../../.history/docs/hello.md/2026-04-07T143000.123Z-zzz0063hd8e5r42   # symlink
├── after  -> ../../.history/docs/hello.md/2026-04-07T150000.456Z-1230deadbeef1z0   # symlink
└── current -> ../../docs/hello.md                               # symlink
```

**Resolution rules:**

| Symlink | Points to |
|---------|-----------|
| `before` | `.history/<filename>/<version_id>` derived from `version_id`. If `version_id` is NULL (INSERT): `/dev/null`. |
| `after` | Query next log entry for this `file_id` after this `log_id`. If found and its `version_id` is non-NULL: `.history/<filename>/<version_id>` from that entry. If found and its `version_id` is NULL (next op was INSERT-like): current file path. If no next entry and file exists: current file path. If no next entry and file deleted: `/dev/null`. |
| `current` | The live file path `../../<filename>`. If the file has been deleted: `/dev/null`. |

The `after` lookup uses the existing `(file_id, log_id ASC)` index -- a single index seek, sub-millisecond even on compressed hypertables.

**Why `/dev/null`:** When a file doesn't exist (before an INSERT, after a DELETE), the symlink points to `/dev/null`. This makes `diff` produce the right output with zero special cases:

```bash
# INSERT: shows entire file as added
diff -u --color notes/.log/<log_id>/before notes/.log/<log_id>/after

# DELETE: shows entire file as removed
diff -u --color notes/.log/<log_id>/before notes/.log/<log_id>/after

# UPDATE: shows the actual changes
diff -u --color notes/.log/<log_id>/before notes/.log/<log_id>/after
```

The script is always the same command. No case statements, no error handling for missing files.

**Scripting examples:**

```bash
# Diff a specific operation
diff -u --color notes/.log/<log_id>/before notes/.log/<log_id>/after

# Diff all recent changes
for id in $(ls notes/.log/.last/10); do
    echo "=== $id ==="
    diff -u --color notes/.log/$id/before notes/.log/$id/after
done

# What changed since this operation vs now?
diff -u --color notes/.log/<log_id>/after notes/.log/<log_id>/current
```

**Implementation note:** TigerFS has no symlink support today. The NFS adapter (go-billy interface) explicitly rejects `Symlink` and `Readlink`. The FUSE adapter doesn't implement `Readlink`. Adding symlink support requires changes in three layers:

1. **Core (`fs/`):** Add `Readlink(ctx, path) (string, *FSError)` to Operations. `Entry.Mode` must support `os.ModeSymlink`. Stat for symlink paths returns mode with `os.ModeSymlink` set.
2. **NFS adapter:** Implement `Readlink()` → delegate to `ops.Readlink()`. `Lstat()` must return symlink entries without following. `opsFileInfo.Mode()` must return `S_IFLNK` for symlinks.
3. **FUSE adapter:** Add `Readlink(ctx) ([]byte, syscall.Errno)` on `OpsNode`. `EntryToAttr` must handle `os.ModeSymlink` → `S_IFLNK`.

Symlinks appear in two contexts: `.log/<id>/` directories (before, after, current diff symlinks) and `.undo/` preview directories (/dev/null symlinks for deleted files). They don't affect existing paths. The NFS and FUSE interfaces already define the symlink operations -- they just need to be wired through.

**Full state matrix:**

| Log entry type | `before` | `after` | `current` |
|---|---|---|---|
| INSERT | `/dev/null` | history or current file | current file or `/dev/null` |
| UPDATE | `.history/` version | history or current file | current file or `/dev/null` |
| DELETE | `.history/` version | `/dev/null` | `/dev/null` or current file |
| UNDO (re-insert) | `/dev/null` | history or current file | current file |
| UNDO (re-delete) | `.history/` version | `/dev/null` | `/dev/null` |
| UNDO (restore) | `.history/` version | history or current file | current file |

### 4.2 `.savepoint/` -- Savepoint Management

See Section 2.3 for the full interface specification.

### 4.3 `.undo/` -- Undo Operations

The `.undo/` directory provides a **preview-then-apply** interface for undo operations. This approach was chosen over two alternatives:

- **DDL-style staging** (`.test/.commit/.abort`): Rejected because DDL staging allows editing the SQL before committing, which is meaningless for undo -- the path fully specifies the operation, there is nothing to edit.
- **Direct write** (one-step trigger): Rejected because multi-file undo is destructive, and a single `echo` with no preview is too easy to execute accidentally.

Each undo path is a virtual directory containing:

- **`.info/summary`**: TSV listing of affected files with actions (supports format suffixes: `summary.json`, `summary.csv`, etc.)
- **Affected files**: Directory tree mirroring the synth app structure, containing only files that would change. `cat` on each file returns the restored content (the before-state from history).
- **`.apply`**: Touch or write to this file to execute the undo

#### 4.3.1 Top-Level Structure

```bash
ls notes/.undo/
id/
to-id/
to-savepoint/
```

Three visible routing directories, each representing a different undo mode:

| Directory | Purpose | Lists |
|---|---|---|
| `id/` | Undo a single operation | Log entries (default 100 most recent) |
| `to-id/` | Undo all operations after a log entry | Log entries (default 100 most recent) |
| `to-savepoint/` | Undo all operations after a savepoint | Savepoints (default 100 most recent) |

The default listing limit (100) is configurable via `--undo-list-limit` or `TIGERFS_UNDO_LIST_LIMIT`.

#### 4.3.2 Pipeline Capabilities

Each sub-directory supports the full data-first pipeline. `id/` and `to-id/` query the log table; `to-savepoint/` queries the savepoint table.

| Capability | Example | Purpose |
|---|---|---|
| `.all/` | `.undo/id/.all/` | Full listing (no limit) |
| `.last/N/` | `.undo/id/.last/20/` | Last N entries |
| `.first/N/` | `.undo/id/.first/20/` | First N entries (oldest) |
| `.sample/N/` | `.undo/to-savepoint/.sample/10/` | Random N entries |
| `.by/<col>/<val>/` | `.undo/id/.by/user_id/agent-7/` | Filter by column |
| `.filter/<col>/<val>/` | `.undo/id/.filter/type/delete/` | Filter by any column |
| `.order/<col>/` | `.undo/to-savepoint/.order/name/` | Sort by column |
| `.columns/<cols>/` | `.undo/id/.columns/log_id,filename,type/` | Project columns |
| `.export/<fmt>` | `.undo/to-savepoint/.export/json` | Export (json, csv, tsv, yaml) |

#### 4.3.3 Single Operation Undo (`id/`)

```bash
ls notes/.undo/id/                       # default 100 most recent log entries
2026-04-08T143015.234Z-i9j0k1l2m3n4b/
2026-04-08T143012.001Z-g7h8i9j0k1l2a/
...
```

Each entry is a preview directory:

```
notes/.undo/id/<log_id>/
├── .info/
│   └── summary
├── docs/                    # directory structure mirrors the synth app
│   └── hello.md             # restored content
└── .apply
```

```bash
cat notes/.undo/id/<log_id>/.info/summary
restore	docs/hello.md

cat notes/.undo/id/<log_id>/.info/summary.json
[{"action":"restore","filename":"docs/hello.md"}]

diff -u --color <(cat notes/.undo/id/<log_id>/docs/hello.md) notes/docs/hello.md
touch notes/.undo/id/<log_id>/.apply
```

#### 4.3.4 Undo to Savepoint (`to-savepoint/`)

```bash
ls notes/.undo/to-savepoint/             # default 100 most recent savepoints
before-exploration/
sprint-start/
auto-agent-7-2026-04-08T143000Z/
...
```

Each entry is a preview directory:

```
notes/.undo/to-savepoint/before-exploration/
├── .info/
│   └── summary
├── docs/
│   └── hello.md
├── bar/
│   └── baz/
│       └── readme.md
├── scratch/
│   └── temp.md -> /dev/null            # file will be deleted
└── .apply
```

```bash
cat notes/.undo/to-savepoint/before-exploration/.info/summary
restore	docs/hello.md
restore	bar/baz/readme.md
delete	scratch/temp.md

touch notes/.undo/to-savepoint/before-exploration/.apply
```

#### 4.3.5 Undo to Log ID (`to-id/`)

Same interface as `to-savepoint/`, but browsing log entries instead of savepoint names:

```bash
ls notes/.undo/to-id/                    # default 100 most recent log entries
2026-04-08T143015.234Z-i9j0k1l2m3n4b/
...

cat notes/.undo/to-id/<log_id>/.info/summary
touch notes/.undo/to-id/<log_id>/.apply
```

#### 4.3.6 Undo by User

Adds a `/.by/user_id/<user_id>/` suffix to `to-savepoint/` or `to-id/` to filter operations:

```bash
cat notes/.undo/to-savepoint/before-exploration/.by/user_id/agent-7/.info/summary
touch notes/.undo/to-savepoint/before-exploration/.by/user_id/agent-7/.apply

cat notes/.undo/to-id/<log_id>/.by/user_id/agent-7/.info/summary
touch notes/.undo/to-id/<log_id>/.by/user_id/agent-7/.apply
```

Only the specified user's operations are undone; other users' changes are preserved.

**Caveat:** If two users interleave edits on the same file, per-user undo restores the file to the state before the specified user's first edit -- which also reverts the other user's interleaved edits on that file. This is inherent in rollback semantics.

#### 4.3.7 Pipeline Filter Composition with `.apply`

Pipeline capabilities within each undo sub-directory narrow which operations are in scope. **What you see in the preview is what gets undone.**

`.apply` is available with all pipeline capabilities **except `.sample/`** (random selection of operations to undo is nonsensical).

| Capability | Affects undo scope? | `.apply` available? |
|---|---|---|
| `.all/` | No -- removes default limit | Yes |
| `.last/N/` | Yes -- limits to last N ops | Yes |
| `.first/N/` | Yes -- limits to first N ops | Yes |
| `.by/<col>/<val>/` | Yes -- narrows by indexed column | Yes |
| `.filter/<col>/<val>/` | Yes -- narrows by any column | Yes |
| `.order/<col>/` | No -- display order only | Yes (ignores order) |
| `.columns/<cols>/` | No -- column projection only | Yes (ignores projection) |
| `.export/<fmt>` | No -- output format only | Yes (ignores format) |
| `.sample/N/` | Yes -- random selection | **No** |

Filters compose with each other and with `.apply`, just as they do in data-first pipeline queries:

```bash
# Undo only delete operations after a savepoint ("bring back deleted files")
cat notes/.undo/to-savepoint/before-refactor/.filter/type/delete/.info/summary
touch notes/.undo/to-savepoint/before-refactor/.filter/type/delete/.apply

# Undo only changes to a specific file
cat notes/.undo/to-savepoint/before-refactor/.filter/filename/hello.md/.info/summary
touch notes/.undo/to-savepoint/before-refactor/.filter/filename/hello.md/.apply

# Undo the last 5 operations after a savepoint
cat notes/.undo/to-savepoint/before-refactor/.last/5/.info/summary
touch notes/.undo/to-savepoint/before-refactor/.last/5/.apply

# Undo only agent-7's delete operations after a savepoint
cat notes/.undo/to-savepoint/before-refactor/.by/user_id/agent-7/.filter/type/delete/.info/summary
touch notes/.undo/to-savepoint/before-refactor/.by/user_id/agent-7/.filter/type/delete/.apply
```

**Algorithm for filtered undo:** For any filtered set of operations, group by `file_id`. For each file, the earliest entry in the filtered set provides the `version_id` (before-state). Apply the same DELETE/UPSERT logic as unfiltered undo, scoped to only the affected files.

#### 4.3.8 Summary Format

The `.info/summary` file is TSV with two columns: action and filename. Format suffixes are supported (`summary.json`, `summary.csv`, etc.):

```
restore	docs/hello.md
restore	bar/baz/readme.md
delete	scratch/temp.md
```

**Actions:**
- `restore`: File will be restored to its state at the target point (exists in the preview directory tree)
- `delete`: File will be deleted (inserted after the target point; appears as `/dev/null` symlink in preview directory tree)

The summary uses the same `DISTINCT ON` query as the undo execution (Section 1.8), so both benefit from SkipScan. No separate `GROUP BY` query is needed. If a user wants per-file operation counts, they can query `.log/.by/file_id/<uuid>`.

#### 4.3.9 Preview Directory Structure

The preview directory tree contains only affected files (not the entire synth app). Only directories that contain affected files appear. Intermediate directories are virtual path components derived from affected filenames.

**Files being restored** (`restore` action): Appear in the directory tree. The **filename comes from the history entry** (the target state), not the log entry. If a file was renamed after the savepoint, the preview shows its name at the savepoint. `cat` returns the restored content -- the before-state from history, rendered through the synth format layer (markdown with frontmatter, plain text, etc.).

**Files being deleted** (`delete` action): Appear in the directory tree as **symlinks to `/dev/null`**. The **filename comes from the log entry** (the current/most-recent name). This enables uniform diffing without conditional logic (see Section 4.3.12).

**Files being re-inserted** (were deleted after the target point): DO appear in the directory tree. The **filename comes from the history entry** (the state being restored). `cat` returns their restored content.

#### 4.3.10 Computing the Preview

The preview is computed lazily -- only materialized when `ls` or `cat` is called on a specific path.

**For `ls` (ReadDir):** Run the undo query to get the list of affected files with their actions. Include all actions (restore and delete). Parse filenames to determine directory structure at the requested depth. Delete entries appear as symlinks to /dev/null.

**For `cat` (ReadFile):** Check if the requested file is in the affected set. If so, fetch the before-state from history via `version_id` and render through the synth format layer. If not affected, return an error (file doesn't exist in the preview).

**For `.info/summary`:** Run the same `DISTINCT ON` undo query (Section 1.8) to get all affected files with their actions. Format as TSV (or JSON/CSV via format suffix).

**Performance:** The undo query (with SkipScan) returns one entry per affected file. For `ls`, no history content is fetched -- just filenames. For `cat`, one history lookup per file. Both are efficient.

#### 4.3.11 Error Handling

- **Invalid log_id in `.undo/id/<log_id>/`:** Returns ENOENT (no such file or directory).
- **Invalid savepoint name in `.undo/to-savepoint/<name>/`:** Returns ENOENT.
- **No operations to undo (e.g., savepoint is the most recent event):** `.info/summary` is empty. `.apply` is a no-op (or returns an error).
- **`.apply` on an already-applied undo:** The undo is idempotent (see Section 3.5), so re-applying produces the same data state with additional log entries.

#### 4.3.12 Combined Diff

Because deleted files appear as `/dev/null` symlinks in the preview tree, diffing all changes is a uniform one-liner with no conditional logic:

```bash
while IFS=$'\t' read action path; do
    diff -u --color "notes/$path" "notes/.undo/to-savepoint/x/$path"
done < notes/.undo/to-savepoint/x/.info/summary
```

Diff direction is **current first, preview second** ("what changes to go from current to undone state"):
- **restore**: `diff current-file restored-preview` -- shows the changes
- **delete**: `diff current-file /dev/null` -- shows the entire file as removed

**Limitation:** For re-inserted files (files that were deleted after the savepoint and will be restored), `notes/$path` does not currently exist on disk, so `diff` would fail. For these entries, use the preview file directly: `cat notes/.undo/to-savepoint/x/$path` to see what will be re-created. The summary action column distinguishes `restore` (file exists currently) from `delete` (file will be removed). Re-inserted files appear with `restore` action in the summary, and the diff script works because the current file path resolves to a non-existent path that `diff` reports as missing.

#### 4.3.13 Finding a Log Entry by Time

With the timestamp+base36 display format, log entries sort chronologically. To find the first entry at or after a specific time:

```bash
LOG_ID=$(cat notes/.log/.export/json | grep -m1 '"2026-04-08T1400' | jq -r '.log_id')
cat notes/.undo/to-id/$LOG_ID/.info/summary
```

#### 4.3.14 Undo Apply Feedback

`touch .apply` returns errno 0 on success or an error code on failure. Detailed feedback is logged at Info level on the TigerFS process:

```go
logging.Info("undo applied",
    zap.String("savepoint", "before-agent"),
    zap.Int("files_restored", 3),
    zap.Int("files_deleted", 1))
```

Agents can verify the undo was applied by re-reading the preview for specific files and confirming the current state matches the target state, or by comparing `.info/summary` output before and after (the actions should remain the same, confirming idempotent application).

### 4.4 Auto-Savepoints

#### 4.4.1 Purpose

The most common failure mode is forgetting to create a savepoint before an agent starts working. Auto-savepoints eliminate this by detecting "session" boundaries based on write inactivity gaps.

#### 4.4.2 Mechanism

When a write occurs on a history-enabled synth app table, TigerFS checks the most recent log entry's timestamp. If the gap exceeds a configurable threshold (default: 30 minutes), an auto-savepoint is created before the write is logged.

```sql
SELECT log_id FROM tigerfs.<app>_log ORDER BY log_id DESC LIMIT 1
```

Single index lookup on the log's PK -- sub-millisecond.

#### 4.4.3 Naming

```
auto-agent-7-2026-04-08T143000Z
auto-agent-7-2026-04-08T160000Z
auto-2026-04-08T143000Z             # anonymous (no user_id)
```

Uses the same timestamp format as UUIDv7 display names (without milliseconds). Greppable by agent:

```bash
ls notes/.savepoint/ | grep "^auto-agent-7"
```

#### 4.4.4 Configuration

- Config: `auto_savepoint_interval: 30m`
- Env: `TIGERFS_AUTO_SAVEPOINT_INTERVAL=30m`
- Flag: `--auto-savepoint-interval 30m`
- Set to `0` to disable

---

## 5. User Identity

### 5.1 Current State

TigerFS has no identity concept today. No `user_id`, no `agent_id`, no way to know who made a change.

### 5.2 Identity Model

Identity is a single string (`user_id`) stored in-memory at the mount level. It is set at mount time and can be modified at runtime.

**Precedence (high to low):**
1. `--user-id` CLI flag
2. `TIGERFS_USER_ID` environment variable
3. NULL (anonymous)

Both the flag and env var set the initial value of `.info/user` at the mount root. The value can be changed at runtime:

```bash
# Set at mount time
tigerfs mount --user-id agent-7 postgres://...

# Read current identity
cat /mnt/db/.info/user
# agent-7

# Change mid-session
echo "agent-9" > /mnt/db/.info/user

# All subsequent logged operations use "agent-9"
```

### 5.3 Storage

The identity is stored in-memory on the `Operations` struct (one per mount). It is ephemeral -- lost on remount. If persistent identity is needed, use `--user-id` at mount time or set `TIGERFS_USER_ID` in the environment.

### 5.4 Why Not Per-Process Identity

Reading the calling process's environment (e.g., `CLAUDE_USER_ID`) from within TigerFS is not feasible across platforms. On Linux FUSE, the caller's PID is available and `/proc/<pid>/environ` can be read (hacky, requires permissions). On macOS NFS, the server only sees UID/GID from RPC credentials -- no PID, no environment. Since TigerFS uses NFS on macOS, per-process identity cannot be reliably implemented. The mount-level `.info/user` approach works on all platforms.

### 5.5 Multi-Agent Architecture

When multiple agents need to work on the same database:

**Use separate mounts, not a shared mountpoint.**

```bash
# Agent 7
TIGERFS_USER_ID=agent-7 tigerfs mount /mnt/db-agent7 postgres://...

# Agent 9
TIGERFS_USER_ID=agent-9 tigerfs mount /mnt/db-agent9 postgres://...
```

This is the design TigerFS was built for. The consistency model explicitly supports multiple mounts to the same database: "A write on one mount must be immediately visible to reads on any other mount." PostgreSQL handles concurrency through MVCC and row-level locking.

Benefits:
- Clean identity -- each mount has its own `user_id`, no race conditions
- Clean isolation -- no shared in-process state
- Independent undo -- each agent's operations are tagged, "undo by user" works correctly

### 5.6 Root-Level `.info/`

This design introduces a root-level `.info/` directory (the mount root, not table-level). Initially it contains only `user`:

```bash
ls /mnt/db/.info/
# user

cat /mnt/db/.info/user
# agent-7
```

The root-level `.info/` can be expanded later with other mount-level metadata.

---

## 6. DDL Limitations

The undo system tracks DML operations only (INSERT, UPDATE, DELETE). It cannot reverse DDL changes:

| DDL Change | Risk |
|------------|------|
| Column added after savepoint | History entries lack the new column. Restore sets it to NULL. |
| Column removed after savepoint | History entries reference a non-existent column. Restore fails. |
| Table dropped | Log and history tables are orphaned. Undo is impossible. |
| Table renamed | Log references may not resolve. |

**Why this is acceptable:**
- Synth app schemas are managed by TigerFS (`.build/`). Schema changes are rare and intentional.
- DDL operations are already staged with `.test/.commit` -- users understand they're structural changes.
- The undo feature targets data changes (agent edited files), not schema changes (someone altered the table).

**Future mitigation (not in Phase 12):** Record a schema fingerprint in the savepoint table. On undo, compare fingerprints and warn if the schema has changed since the savepoint was created.

---

## 7. Log Entry Creation: Integration with Write Path

### 7.1 Where Log Entries Are Created

Every write operation in TigerFS that modifies a history-enabled synth app table must also insert a log entry. The affected code paths:

| Operation | Write Path | Log Entry Type |
|-----------|-----------|---------------|
| Create a file | `writeSynthFile()` / `db.InsertRow()` | `create` |
| Edit a file | `writeSynthFile()` / `db.UpdateRow()` | `edit` |
| Delete a file | `deleteSynthFile()` / `db.DeleteRow()` | `delete` |
| Rename a file/dir | `renameSynthFile()` / `db.UpdateRow()` | `rename` |
| Move a file/dir | `renameSynthFile()` / `db.UpdateRow()` | `rename` |
| Undo operation | New undo handler | `undo` |

### 7.2 Determining `version_id`

For UPDATE and DELETE operations, the BEFORE trigger creates a history entry synchronously. To capture the `version_id` for the log:

**Option A:** Query the history table for the most recent entry matching the `file_id` immediately after the DML.

**Option B:** Modify the DML to use a CTE or RETURNING clause that captures the trigger's output.

**Option C:** Use a PostgreSQL function that performs the DML and returns the generated `version_id`.

The exact mechanism is an implementation detail to be determined during development. All options are correct; they differ in complexity and round-trip count.

---

## 8. Table Setup: DDL for Log and Savepoint Tables

When a synth app with history is created (via `.build/`), the setup must additionally create:

1. The log hypertable (`tigerfs.<app>_log`)
2. The composite index on `(file_id, log_id ASC)`
3. The compression policy
4. The savepoint table (`tigerfs.<app>_savepoint`)

This extends the existing DDL generation in `synth/build.go` which already creates the backing table, view, history table, history trigger, and hypertable/compression configuration.

---

## 9. Performance Characteristics

### 9.1 Write Overhead

Each write to a history-enabled table now requires one additional INSERT (the log entry) in addition to the existing DML + trigger. This is a small overhead -- one extra row insert into a hypertable.

### 9.2 Undo-to-Savepoint Query Performance

| Scenario | Log entries after savepoint | Unique files | Expected time |
|----------|---------------------------|-------------|---------------|
| Agent did 20 edits, recent savepoint | ~20 | 5-15 | < 100ms |
| Agent did 500 edits, recent savepoint | ~500 | 50-200 | < 1s |
| Old savepoint, months of history | 10,000+ | 1,000+ | seconds (decompression) |

The common case (recent savepoint, agent cleanup) is fast because:
1. **Chunk exclusion:** `WHERE log_id > savepoint_id` prunes compressed chunks before the savepoint
2. **SkipScan:** Hops across `file_id` values instead of scanning every entry
3. **History lookups:** One PK lookup per affected file (not per log entry)

### 9.3 Preview Computation

The preview (`.info/summary` and affected file listing) uses the same query as the undo itself. No additional cost beyond the undo query + history lookups for file content (only when `cat` is called on a specific file).

---

## 10. Naming Conventions

### 10.1 Why "Undo" (Not "Rollback")

- **"Undo"** maps to the Ctrl+Z mental model -- "take back what I just did." This matches the primary use cases.
- **"Rollback"** in PostgreSQL means "discard uncommitted work inside a transaction." Here, changes are already committed and we're creating new operations to restore old state. "Rollback" borrows the word but changes the semantics.
- One word, one concept, one directory (`.undo/`). The path structure communicates scope: `.undo/id/<id>` (one operation) vs `.undo/to-id/<id>` (everything after this entry) vs `.undo/to-savepoint/<name>` (everything after a checkpoint).

### 10.2 Directory Summary

| Path | Purpose | Implementation |
|------|---------|---------------|
| `<app>/.log/` | Operation log (read-only) | Data-first pipeline on `tigerfs.<app>_log` |
| `<app>/.savepoint/` | Savepoint management (read/write) | Data-first pipeline on `tigerfs.<app>_savepoint` |
| `<app>/.undo/` | Undo operations (preview + apply) | Custom handler with preview directory tree |
| `/.info/user` | User identity (read/write) | In-memory on Operations struct |

---

## 11. UUIDv7 Display Format

### 11.1 Problem

UUIDv7 values displayed as standard hex (`019590a0-1234-7fff-8000-a1b2c3d4e5f6`) are opaque. A directory listing of log entries or history versions tells you nothing about timing without reading each entry. Since UUIDv7 embeds a millisecond timestamp, the display format should surface it.

### 11.2 Format

Display UUIDv7 values as `<timestamp>-<base36 entropy>`:

```
2026-04-07T143000.123Z-zzz0063hd8e5r42
├── timestamp (ms) ────┘    └── entropy (base36)
```

**Timestamp portion:** Millisecond-precision, UTC, filesystem-safe (no colons). Format: `2006-01-02T150405.000Z`. Extracted from UUIDv7 bits 0-47.

**Entropy portion:** The 74 non-timestamp, non-fixed bits (12 bits rand_a + 62 bits rand_b), encoded as base36 (0-9a-z). ~15 characters.

**Total length:** ~40 characters (vs 36 for standard UUID hex).

### 11.3 Why Base36

- **Case-insensitive safe.** Only 0-9a-z. No collisions on macOS APFS (which defaults to case-insensitive). Base62 and base64url use mixed case and are NOT safe.
- **Familiar charset.** Lowercase alphanumeric -- nothing surprising.
- **Compact.** 15 chars for 74 bits (vs 19 for hex, 13 for base62).
- **Trivial to implement.** `big.Int.Text(36)` encodes, `big.Int.SetString(s, 36)` decodes. No libraries needed.

### 11.4 Encoding and Decoding

```go
func UUIDv7ToDisplayName(id uuid.UUID) string {
    // Extract ms timestamp from bits 0-47
    b := id[:]
    msec := int64(binary.BigEndian.Uint16(b[0:2]))<<32 |
        int64(binary.BigEndian.Uint32(b[2:6]))
    ts := time.UnixMilli(msec).UTC()

    // Extract 74 entropy bits: rand_a (bits 52-63) + rand_b (bits 66-127)
    // Pack into 10 bytes, encode as base36
    entropy := packEntropy(id)
    var n big.Int
    n.SetBytes(entropy)
    
    return fmt.Sprintf("%s-%s", ts.Format("2006-01-02T150405.000Z"), n.Text(36))
}

func DisplayNameToUUIDv7(name string) (uuid.UUID, error) {
    // Split on last '-' that separates timestamp from entropy
    // Parse timestamp → reconstruct bits 0-47
    // Parse base36 entropy → reconstruct bits 52-63, 66-127
    // Set version bits (48-51 = 0111) and variant bits (64-65 = 10)
    // → Full UUID reconstructed, 1:1 reversible, no lookup needed
}
```

**Fully reversible.** The display name encodes all 122 meaningful bits of the UUIDv7. The 4 version bits and 2 variant bits are fixed constants and reconstructed on decode.

### 11.5 Scope of Change

The display format applies **globally** wherever UUIDv7 values are used as filenames in directory listings. UUIDv7 is detected by checking the version bits (bits 48-51 = `0111`), which is reliable per RFC 9562. Non-v7 UUIDs (v4, v1, etc.) continue to display as standard hex.

| Context | Format | Why |
|---|---|---|
| Directory entry name (PK as filename) | Display format for v7, hex for others | Filenames should be human-readable |
| Column value in file content (`cat .../col`) | Always hex | Standard UUID text representation in data formats |
| `.by/<column>/<value>` input path | Accept both formats | User might type either |
| `.export/json` output | Always hex | JSON data should use standard UUID format |

**Implementation point:** The conversion is applied in `scanAndEncodePK()` (`db/query.go`), where PK values become filenames. It detects UUIDv7 and applies the display format. The generic `ConvertValueToText()` in `format/convert.go` is unchanged (it handles content display, not filenames).

**Affected contexts:**

| Context | Current format | New format |
|---|---|---|
| `.history/<file>/<version>` | `2026-04-07T143000Z` (second precision, lossy) | `2026-04-07T143000.123Z-zzz0063hd8e5r42` (lossless) |
| `.log/<entry>` | hex UUID (new code) | timestamp+base36 |
| `.undo/id/<log_id>`, `.undo/to-id/<log_id>` paths | hex UUID (new code) | timestamp+base36 |
| Data-first tables with UUIDv7 PK | hex UUID | timestamp+base36 (auto-detected) |
| Data-first tables with UUIDv4 PK | hex UUID | hex UUID (unchanged) |

**No database migration needed.** The display format is a pure presentation-layer conversion -- version IDs are computed on-the-fly from the UUIDv7 bytes stored in the `_version_id` column, never stored as strings. Changing the conversion function immediately produces the new format for all existing data. The only breakage is external scripts that cached old-format paths (e.g., `2026-04-07T143000Z`), which is acceptable.

### 11.6 Visual Comparison

```bash
# Current .history/ listing
ls notes/.history/hello.md/
2026-04-07T100000Z
2026-04-07T143000Z
2026-04-07T150000Z

# New .history/ listing
ls notes/.history/hello.md/
2026-04-07T100000.000Z-a230b1c2d3e4f5x
2026-04-07T143000.123Z-zzz0063hd8e5r42
2026-04-07T150000.456Z-1230deadbeef1z0

# New .log/ listing
ls notes/.log/.last/3
2026-04-07T143000.123Z-zzz0063hd8e5r42
2026-04-07T143001.456Z-a230b1c2d3e4f5x
2026-04-07T143005.789Z-1230deadbeef1z0
```

---

## 12. Skills and Documentation Updates

### 12.1 Key Behavioral Guidance (SKILL.md)

The top-level skill must establish savepoints as a core agent workflow, not an optional feature:

- **Before making multiple or risky edits, always create a savepoint.** This is cheap and provides a clean revert path.
- **Never manually reverse edits across files to "undo."** Use `touch .undo/to-savepoint/name/.apply` instead. TigerFS tracks exact before-states; manual reversal is error-prone and may miss files or introduce inconsistencies.
- **When to savepoint:** Before investigating/debugging, before refactoring, before multi-file operations, before anything uncertain.
- **When to undo:** User asks to revert, agent realizes approach isn't working, changes were made to wrong files.

Add to the anti-patterns table: "Manually reverse edits to undo" -> "Create savepoints, use `.undo/` to revert atomically."

Add to the "What you can build" section: pointer to undo workflow when asked to revert or roll back.

### 12.2 File-First Reference (files.md)

Add three new sections after the existing "Versioned History" section:

1. **Operation Log (`.log/`):** Browsing recent operations, filtering by user/file/type, diff symlinks (before/after/current).
2. **Savepoints (`.savepoint/`):** Creating, listing, renaming, deleting, auto-savepoints.
3. **Undo (`.undo/`):** Single operation undo, undo to savepoint, undo by user, preview + apply workflow, combined diff one-liner.

Update the existing history section to reflect the new UUIDv7 display format.

### 12.3 Recipes (recipes.md)

Add two new recipes:

- **Recipe 5: Safe Agent Exploration.** Auto-savepoints, manual savepoints, agent self-revert pattern.
- **Recipe 6: Selective Undo in Multi-Agent Workflows.** Separate mounts per agent, per-user undo, preserving other agents' work.

### 12.4 Operations Reference (ops.md)

Add `--user-id` flag, `TIGERFS_USER_ID` env var, and `--auto-savepoint-interval` flag.

### 12.5 Quick Reference Updates (SKILL.md)

Add to the quick reference table:

| Goal | Tool Call |
|------|-----------|
| Create savepoint | `Bash "touch mount/app/.savepoint/name"` |
| Preview undo | `Read "mount/app/.undo/to-savepoint/name/.info/summary"` |
| Apply undo | `Bash "touch mount/app/.undo/to-savepoint/name/.apply"` |
| View log | `Read "mount/app/.log/.last/10/.export/json"` |
| Diff a change | `Bash "diff -u --color mount/app/.log/<id>/before mount/app/.log/<id>/after"` |

---

## 13. Open Design Questions (Deferred)


### 13.1 Time-Travel Snapshots

The undo preview shows only affected files. A more powerful feature would be full time-travel snapshots -- browse the entire synth app as it existed at any point in time. This could live under `.snapshot/` or as an extension to `.history/`. Deferred to a future phase.

### 13.2 Schema Fingerprinting for Savepoints

Record the table schema at savepoint creation time. Warn on undo if the schema has changed. Deferred -- synth app schema changes are rare.

### 13.3 Optimistic Concurrency for Undo

Detect when a concurrent write conflicts with an undo operation and abort rather than silently overwriting. Deferred -- the current "last writer wins" behavior matches standard PostgreSQL semantics.

### 13.4 Cross-Table Undo

Undo operations across multiple tables in a single operation. Deferred -- the common case is single-table undo (one synth app = one mini-filesystem). Per-table undo is sufficient for Phase 12.

### 13.5 CLI Commands (`tigerfs undo`, `tigerfs savepoint`)

Thin CLI wrappers around the filesystem operations for human ergonomics (`tigerfs undo --to-savepoint x --preview`, `tigerfs savepoint create x`). Would resolve mount points from the mount registry. Deferred -- the filesystem interface is complete and agents use it directly.

### 13.6 Bulk Operations in the Log

Should `.import/` operations create one log entry per row, or one log entry for the entire import? Per-row is correct but could create thousands of log entries. Deferred to implementation.

---

## 14. Summary of Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Log scope | Per-table | Matches history tables. Simpler queries. No cross-table transaction complexity. |
| Log stores | Pointer to history (`version_id`), not copies | No data duplication. History already has full row state. Log stays small. |
| Savepoint storage | Separate table | Clean schema. Natural UNIQUE on name. No compression concerns. |
| Undo semantics | Rollback (git checkout), not revert (git revert) | Full row state in history. No column-level diff/merge needed. |
| Undo interface | Preview + apply (stateless) | Preview directory tree shows affected files. `touch .apply` triggers execution. No session management. |
| `after_id` in log | Not stored | After-state doesn't exist as a history entry at log-write time. Before-state is sufficient for undo. After-state is derivable from the chain. |
| `filename` in log | Denormalized | Avoids JOINs in display queries. Preserves historically-correct filename. Keeps hypertable optimizations. |
| Type column | TEXT with CHECK constraint | Matches existing `operation` in history. Easy to extend without ALTER TYPE migration. |
| User identity | Mount-level `.info/user` | Set at mount time via --user-id or TIGERFS_USER_ID. Modifiable at runtime. In-memory storage. |
| Multi-agent | Separate mounts | One mount per agent. Concurrency through PostgreSQL. Clean identity and isolation. |
| Naming | "Undo" not "rollback" | Ctrl+Z mental model. "Rollback" implies uncommitted transaction. One concept, one directory. |
| Cross-table undo | Deferred | Single-table covers the common case. Cross-table adds significant transaction complexity. |
| Compression segmentby | `file_id` | Enables SkipScan on compressed chunks. Matches undo query pattern. |
| Compression orderby | `log_id ASC` | Aligns with undo query ORDER BY. Enables SkipScan within compressed segments. |
| UUIDv7 display format | Timestamp+base36, global | `2026-04-07T143000.123Z-zzz0063hd8e5r42`. Case-insensitive safe, lossless, 40 chars. Applied globally for UUIDv7 PKs via `scanAndEncodePK()`. |
| Diff symlinks | before/after/current on log entries | Symlinks to .history/ versions or /dev/null. Enables `diff` with no special cases. |
| Deleted files in preview | /dev/null symlinks | Appear in preview tree as symlinks to /dev/null. Enables uniform one-liner diff across all action types. |
| Auto-savepoints | Session-based, on inactivity gap | Auto-create savepoint when gap > threshold (default 30m). Named `auto-<user>-<timestamp>`. Eliminates "forgot to checkpoint" failure mode. |
| Apply trigger | `touch .apply` (not `.commit`) | "Apply this undo" reads naturally. Distinct from DDL's `.commit`. Triggered via Create or Write (both `touch` and `echo` work). |
| Undo feedback | `logging.Info` + verify state | Log at Info level on TigerFS process. Agents verify by checking files match target state. |
