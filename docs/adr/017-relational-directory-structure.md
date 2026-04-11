# ADR-017: Relational Directory Structure for Synth Apps

**Status:** Accepted
**Date:** 2026-04-10
**Author:** Mike Freedman

## Context

Synth apps encode directory structure in the `filename` column using slashes (`projects/web/todo.md`). Directory renames use `RenameByPrefix` -- a single SQL UPDATE that modifies N rows' filename columns. This creates a fundamental problem for the undo log (ADR-016):

- The undo log is per-file (one `file_id` per entry with a `version_id` pointing to the before-state)
- A directory rename is a batch operation affecting N files
- Multiple approaches were evaluated (single rename-dir entry, JSONB arrays, batch_id grouping, N independent entries). All have correctness gaps: partial undo risk, DISTINCT ON incompatibility, ordering dependencies, or filter blind spots.

The relational parent-pointer model eliminates the problem: renaming a directory is a single-row update (change the directory row's `filename` column), which naturally fits the per-file undo log model.

Beyond undo, the relational model also improves ReadDir performance (O(children) vs O(all_rows)) and eliminates prefix-collision risks in LIKE queries.

## Decision

Replace path-encoded filenames with a parent-pointer model: `filename` stores only the leaf name, and `parent_id` references the parent directory row.

### Source table schema

```sql
CREATE TABLE tigerfs.<app> (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id UUID REFERENCES tigerfs.<app>(id) DEFERRABLE INITIALLY IMMEDIATE,
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    title TEXT,                    -- markdown only
    author TEXT,                   -- markdown only
    headers JSONB DEFAULT '{}'::jsonb,  -- markdown only
    body TEXT,
    encoding TEXT NOT NULL DEFAULT 'utf8' CHECK (encoding IN ('utf8', 'base64')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE
)
```

Changes from ADR-011:
- `filename`: now stores leaf name only (e.g., `todo.md`), not full path (`projects/web/todo.md`). No slashes.
- `parent_id UUID`: self-referencing FK (DEFERRABLE INITIALLY IMMEDIATE) to parent directory row. NULL for root-level entries. `DEFERRABLE` is needed so undo transactions can `SET CONSTRAINTS ALL DEFERRED` to DELETE/INSERT rows in any order. `INITIALLY IMMEDIATE` (not DEFERRED) because PostgreSQL's ON CONFLICT clause does not support deferrable constraints as arbiters -- `InsertIfNotExists` for parent directory creation needs immediate constraint checking for normal operations.
- `UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE`: uniqueness is per-directory, not global. `NULLS NOT DISTINCT` (PostgreSQL 15+) ensures uniqueness at the root level where parent_id is NULL. `DEFERRABLE` allows undo transactions to defer checking via `SET CONSTRAINTS ALL DEFERRED`. `INITIALLY IMMEDIATE` for the same ON CONFLICT reason as above; note that `InsertIfNotExists` uses plain INSERT with unique-violation error handling (SQLSTATE 23505) rather than ON CONFLICT, because even `INITIALLY IMMEDIATE` deferrable constraints are not valid ON CONFLICT arbiters in PostgreSQL.
- Self-referencing FK uses ON DELETE RESTRICT (default) -- cannot delete a directory that has children.

Additional DDL:

```sql
-- Index for ReadDir and path resolution
CREATE INDEX idx_<app>_parent ON tigerfs.<app>(parent_id, filename);
```

### History table schema

```sql
CREATE TABLE tigerfs.<app>_history (
    file_id UUID,
    parent_id UUID,
    filename TEXT NOT NULL,
    filetype TEXT CHECK (filetype IN ('file', 'directory')),
    title TEXT,
    author TEXT,
    headers JSONB,
    body TEXT,
    encoding TEXT CHECK (encoding IN ('utf8', 'base64')),
    created_at TIMESTAMPTZ,
    modified_at TIMESTAMPTZ,
    version_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    operation TEXT NOT NULL CHECK (operation IN ('UPDATE', 'DELETE'))
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'version_id',
    tsdb.chunk_interval = '7 days',
    tsdb.segmentby = 'file_id',
    tsdb.orderby = 'version_id DESC'
)
```

Changes from previous:
- `id` renamed to `file_id` (references source table's `id`; clearer in context)
- `filename` stores leaf name (matching source table)
- `parent_id` added (captures the file's directory at the time of the operation)
- `_history_id` renamed to `version_id` (more descriptive; no underscore prefix)
- `_operation` renamed to `operation` (drop underscore convention)
- CHECK constraints on `filetype`, `encoding`, and `operation`
- Uses modern `CREATE TABLE WITH` syntax for hypertable + columnstore
- `segmentby = 'file_id'` (was `'filename'`; better for relational model where leaf names can collide across directories)
- `orderby = 'version_id DESC'` (was `'_history_id DESC'`)

### Log table schema

```sql
CREATE TABLE tigerfs.<app>_log (
    log_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    file_id UUID NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('create', 'edit', 'rename', 'delete', 'undo')),
    user_id TEXT,
    filename TEXT NOT NULL,
    version_id UUID,
    description TEXT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'log_id',
    tsdb.chunk_interval = '7 days',
    tsdb.segmentby = 'file_id',
    tsdb.orderby = 'log_id ASC'
)
```

Changes from ADR-016 Task 12.3:
- `history_id` renamed to `version_id` (matches history table)
- Operation types renamed from DB-centric to filesystem-centric:
  - `insert` -> `create`
  - `update` (content) -> `edit`
  - `update` (rename/move) -> `rename`
  - `delete` -> `delete`
  - `undo` -> `undo`
- Log table retains the `(file_id, log_id ASC)` composite index for SkipScan on undo queries:
  `CREATE INDEX idx_<app>_log_by_file ON tigerfs.<app>_log (file_id, log_id ASC);`
- `filename` column stores the **denormalized full path** (e.g., `projects/web/todo.md`), computed at log-write time by walking the parent chain. This is for human-readable log display; the authoritative structure is in the source table's `parent_id` references. Note: the log's `filename` has different semantics from the source table's `filename` (leaf name only). The log stores the full path for display; the source table stores the leaf name for structure.

### Path resolution

A PL/pgSQL function resolves path segments to a row ID, with an optional starting parent for cache integration. Returns a table of `(depth, id, filename)` so the Go layer can populate the path cache for all intermediate segments.

```sql
CREATE FUNCTION tigerfs.resolve_path(tbl REGCLASS, start_parent UUID, segments TEXT[])
RETURNS TABLE(depth INT, resolved_id UUID, resolved_name TEXT) AS $$
DECLARE
    current_id UUID := start_parent;
    i INT := 0;
    seg TEXT;
BEGIN
    FOREACH seg IN ARRAY segments LOOP
        i := i + 1;
        -- EXECUTE required because PL/pgSQL doesn't support variables as table names.
        -- format('%s', tbl) produces the properly schema-qualified name from REGCLASS.
        EXECUTE format('SELECT id FROM %s WHERE filename = $1 AND parent_id IS NOT DISTINCT FROM $2', tbl)
        INTO current_id
        USING seg, current_id;
        IF current_id IS NULL THEN RETURN; END IF;
        depth := i;
        resolved_id := current_id;
        resolved_name := seg;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
```

Usage: `SELECT * FROM tigerfs.resolve_path('tigerfs.app', NULL, ARRAY['projects','web','todo.md'])`

Returns 3 rows: `(1, uuid-1, 'projects')`, `(2, uuid-2, 'web')`, `(3, uuid-3, 'todo.md')`. The Go layer caches each `(parent_id, name) → id` pair. If any segment doesn't resolve, the function returns fewer rows than segments (indicating a missing path component).

With a cached start_parent: `SELECT * FROM tigerfs.resolve_path('tigerfs.app', uuid-1, ARRAY['web','todo.md'])` -- skips the already-cached root resolution.

Performance: One network round-trip (~10ms). N index lookups server-side (~0.01ms each). For typical depth (2-5 levels), server-side cost is negligible vs network latency.

### Path cache (Go-level)

A Go-level cache maps `(parent_id, filename)` -> `id` with the same 2-second TTL as the existing stat cache. This avoids calling resolve_path for repeated access to the same directory subtree.

Cache invalidation: directory renames/moves invalidate entries for the renamed directory. The 2-second TTL provides a bounded staleness window matching the existing consistency model ("Stat may use caches").

For the multi-agent task board use case (file moves between fixed directories), the path cache is never stale because directory names don't change.

### Key operations

| Operation | Current (path-encoded) | New (parent-pointer) |
|---|---|---|
| Rename directory | `RenameByPrefix` (N rows) | `UPDATE SET filename='new' WHERE id=dir_id` (1 row) |
| Move directory | `RenameByPrefix` with new prefix (N rows) | `UPDATE SET parent_id=new_parent WHERE id=dir_id` (1 row) |
| Rename file | `UpdateColumnCAS` on filename (full path) | `UPDATE SET filename='new' WHERE id=file_id` (1 row) |
| Move file | `UpdateColumnCAS` on filename (full path) | `UPDATE SET parent_id=new_dir, filename='new' WHERE id=file_id` (1 row) |
| ReadDir | `GetAllRows` + in-memory prefix filter O(all_rows) | `SELECT * WHERE parent_id = dir_id` O(children) |
| Stat/ReadFile | `WHERE filename = 'full/path'` O(1) | Path cache hit: O(0) network + 1 query for leaf. Cache miss: `resolve_path` O(1 round-trip) |
| mkdir | `INSERT (filename='full/path', filetype='directory')` | `INSERT (filename='dirname', parent_id=X, filetype='directory')` |
| Ensure parents | Split path, InsertIfNotExists per ancestor | Same but chain parent_id values |
| Delete check | `LIKE prefix/%` | `WHERE parent_id = dir_id` |
| Undo of rename | Broken (batch problem) | Standard single-row UPSERT from history |

### ReadFile / Stat with resolve_path and cache

The Go-level path cache short-circuits already-resolved ancestor segments. Only unresolved segments are sent to the DB via resolve_path. The resolve_path function returns intermediate results so the cache can be populated in one call.

**Go-level resolution flow for `projects/web/todo.md`:**

1. Walk path segments, checking cache at each level:
   - `(NULL, "projects")` → cache hit → uuid-1
   - `(uuid-1, "web")` → cache miss → stop
2. Remaining segments: `["web", "todo.md"]`
3. One DB call: `resolve_path(tbl, uuid-1, ARRAY['web','todo.md'])` → returns rows: `(1, uuid-2, "web"), (2, uuid-3, "todo.md")`
4. One network round-trip (~10ms). Server does 2 index lookups.
5. Cache populated from returned rows: `(uuid-1,"web")→uuid-2`, `(uuid-2,"todo.md")→uuid-3`

**Next access to `projects/web/notes.md`:**

1. Cache: `(NULL,"projects")→uuid-1` hit, `(uuid-1,"web")→uuid-2` hit
2. Cache: `(uuid-2,"notes.md")` → miss
3. Remaining: `["notes.md"]` with start_parent=uuid-2
4. For ReadFile, combine the last resolution step with the row fetch:
   `SELECT * FROM t WHERE parent_id = uuid-2 AND filename = 'notes.md'`
5. One round-trip (~10ms). Returns the full row (content + metadata).

After the first access to any file in a directory, the path prefix is fully cached. Subsequent accesses to siblings only query the leaf.

**For ReadFile** (consistency model: "ReadFile must always hit the DB"):
- Parent path resolution uses the cache (parent_id mappings are structural, not content)
- The leaf row is fetched with a combined resolve + fetch query:
  `SELECT * FROM t WHERE parent_id = $cached_parent AND filename = $leaf_name`
- One round-trip (~10ms) -- same as the current model's `WHERE filename = 'full/path'`

**For Stat** (consistency model: "Stat may use caches"):
- Both parent resolution AND leaf stat can use the cache
- Potentially zero DB round-trips if fully cached

**Cold path (first access, nothing cached):**
- `resolve_path(NULL, ARRAY['projects','web','todo.md'])` → one round-trip (~10ms), returns all intermediate IDs
- Then fetch full row: `SELECT * FROM t WHERE id = uuid-3` → one round-trip (~10ms)
- Total: ~20ms. Subsequent accesses: ~10ms (ancestors cached, only leaf fetch needed)

**Path cache TTL:** 2 seconds, matching the existing stat cache. Directory structure changes (renames/moves) are visible to other mounts within 2 seconds.

### .history/ navigation

`.history/` follows the same parent_id structure as the live table:

- `ls .history/`: `SELECT DISTINCT file_id, filename FROM history WHERE parent_id IS NULL`
- `ls .history/projects/`: Resolve `projects` to its `id`, then `SELECT DISTINCT file_id, filename FROM history WHERE parent_id = <projects_id>`
- `ls .history/projects/web/todo.md/`: Resolve full path to `file_id`, then `SELECT version_id FROM history WHERE file_id = <todo_id> ORDER BY version_id DESC`

File renames are handled correctly: `.history/<current_name>/` resolves by file_id. All versions of that file (under any past name or directory) are accessible via `.history/.by/<uuid>/`.

### Rename and move operations

**FUSE `Rename(oldParent, oldName, newParent, newName)`** maps to:
- **Same parent (rename):** `UPDATE SET filename = newName WHERE id = fileID` (1 row)
- **Different parent (move):** `UPDATE SET parent_id = newParentID, filename = newName WHERE id = fileID` (1 row)
- **Directory rename/move:** Same as above -- the directory row is updated, children are unaffected

Both cases are single-row updates. The BEFORE trigger fires once, creating one history entry. One log entry is created with `type = 'rename'`.

The current `UpdateColumnCAS` pattern (compare-and-swap on filename) is replaced by a simpler `UPDATE ... WHERE id = X`. Concurrent renames of the same file are serialized by PostgreSQL's row-level locking.

### Delete behavior

The self-referencing FK uses `ON DELETE RESTRICT` (PostgreSQL default). The Go layer checks for children before attempting delete:

```go
// Check: SELECT EXISTS(SELECT 1 FROM t WHERE parent_id = dir_id)
// If children exist → return ENOTEMPTY
// If no children → DELETE FROM t WHERE id = dir_id
```

The FK constraint is a safety net -- if the Go check races with a concurrent insert, the DELETE fails at the DB level rather than orphaning children.

### PostgreSQL version requirement

This ADR requires PostgreSQL 15+ for `UNIQUE NULLS NOT DISTINCT`. Combined with ADR-016's requirement of PostgreSQL 16+ for SkipScan, the effective minimum is **PostgreSQL 16**.

### BEFORE trigger

The archive trigger copies the OLD row (including `parent_id`) to the history table:

```sql
CREATE OR REPLACE FUNCTION tigerfs.archive_<app>_history() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO tigerfs.<app>_history
        (file_id, parent_id, filename, filetype, title, author, headers,
         body, encoding, created_at, modified_at,
         version_id, operation)
    VALUES
        (OLD.id, OLD.parent_id, OLD.filename, OLD.filetype, OLD.title, OLD.author, OLD.headers,
         OLD.body, OLD.encoding, OLD.created_at, OLD.modified_at,
         uuidv7(), TG_OP::text);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

The trigger fires BEFORE UPDATE OR DELETE, capturing the complete row state including `parent_id`. When a directory is renamed (filename changes) or moved (parent_id changes), the history entry records the old values.

**Note:** The trigger SQL above shows markdown-specific columns (title, author, headers). Plain text tables omit these columns. The DDL generator in `build.go` produces format-specific triggers, same as the current implementation.

### Impact on undo

With the relational model, every operation (including directory rename and move) is a single-row change:

| Operation | Log type | What changes | Undo mechanism |
|---|---|---|---|
| Create file | `create` | New row | DELETE the row |
| Edit file | `edit` | body/title/etc columns | UPSERT from version (history entry) |
| Rename file/dir | `rename` | filename column | UPSERT from version |
| Move file/dir | `rename` | parent_id column | UPSERT from version |
| Delete file/dir | `delete` | Row removed | INSERT from version |

All use the standard DISTINCT ON + UPSERT undo pattern. No special cases for directory operations.

**UPSERT must restore `parent_id`.** The undo UPSERT (ADR-016 Section 3.3) must include `parent_id` in the restored columns, along with `filename`, `body`, `title`, etc. This ensures that file moves and directory moves are correctly reversed.

**Filtered undo FK failure.** Any filtered undo (`.by/user_id/`, `.filter/`, `.last/N/`) can fail at commit if the restored `parent_id` references a directory that was deleted by an operation OUTSIDE the filter scope. The undo transaction rolls back entirely (PostgreSQL ACID guarantee) and reports a descriptive error. The database is unchanged. Specific scenarios:

- **Undo of directory creation:** Agent-1 creates a directory, agent-2 adds files. Per-user undo of agent-1 tries to DELETE the directory. FK fails because agent-2's files reference it. Same semantics as `rmdir` on non-empty directory (ENOTEMPTY).

- **Undo of file move:** Agent-1 moves a file from `docs/` to `archive/`. Agent-2 deletes `docs/`. Per-user undo of agent-1 tries to restore `parent_id=uuid-docs`. FK fails because `uuid-docs` no longer exists.

- **Undo of file deletion:** A file was deleted, and its parent directory was later deleted by someone else. Undo tries to INSERT the file with its old `parent_id`. FK fails.

**Unfiltered undo-to-savepoint always succeeds** because ALL affected rows (including deleted parent directories) are restored within the same transaction. The DEFERRABLE FK allows intermediate states; all constraints are satisfied at COMMIT.

**UNIQUE constraint at commit.** Both the FK and UNIQUE constraints are `DEFERRABLE INITIALLY IMMEDIATE`. In normal operations, they check immediately. Undo transactions explicitly call `SET CONSTRAINTS ALL DEFERRED` at the start of the transaction, deferring all constraint checks to COMMIT. This allows intermediate violations within the undo transaction (e.g., restoring a filename before deleting the row that currently holds it). At COMMIT, PostgreSQL checks all deferred constraints. If the final state has a UNIQUE violation (e.g., two rows with the same name in the same directory that weren't both handled by the undo), the transaction rolls back. This is a genuine conflict requiring manual resolution, but it can only happen with filtered undos -- unfiltered undo-to-savepoint restores the savepoint state exactly, which was valid.

### Deprecated code (kept for backward compatibility)

The following are superseded by the parent-pointer model but retained behind `info.Roles.ParentID == ""` guards for pre-migration databases:

- `RenameByPrefix` query and callers (replaced by single-row `UPDATE WHERE id`)
- `HasChildrenWithPrefix` query (replaced by `WHERE parent_id = X`)
- `filterHierarchicalChildren` (replaced by `GetRowsByParent` queries)
- Path-based prefix matching logic in synth_ops.go

These code paths are exercised only when a synth app lacks the `parent_id` column (old schema). After running `tigerfs migrate`, all apps use the parent-pointer model and these paths become dead code. They can be removed once backward compatibility with pre-ADR-017 databases is no longer needed.

### Files requiring changes

| File | Changes |
|---|---|
| `synth/build.go` | Source table schema: add parent_id, change UNIQUE constraint. History table: file_id, parent_id, version_id, operation. Create resolve_path function. |
| `synth/cache.go` | ColumnRoles: add ParentID role |
| `synth/markdown.go` | GetMarkdownFilename uses leaf filename |
| `synth/plaintext.go` | GetPlainTextFilename uses leaf filename |
| `fs/synth_ops.go` | All write/read/stat/rename/delete operations use parent_id model. Remove filterHierarchicalChildren. ReadDir queries by parent_id. Path resolution via resolve_path. |
| `fs/history.go` | History queries use file_id + parent_id. version_id column name. |
| `db/query.go` | Remove RenameByPrefix, HasChildrenWithPrefix. Add GetRowsByParent, resolve_path caller. |
| `db/interfaces.go` | Update HierarchyWriter interface |
| All synth tests | Rewrite for new model |

### Migration

The `tigerfs migrate` command includes a `relational-directories` migration that handles existing databases. TigerFS creates new apps with the new schema automatically; migration is only needed for databases created before ADR-017. Run `tigerfs migrate <connection> --describe` to check for pending migrations, or `tigerfs migrate <connection>` to execute.

The `relational-directories` migration in `cmd/migrate.go` performs these steps per app (in a single transaction):

1. **Add parent_id column** to source table
2. **Populate parent_id** via PL/pgSQL DO block: processes rows with "/" in filename, shallowest first. For each row, looks up the parent directory by its old full-path filename (which still exists at this point) and sets parent_id to that directory's UUID
3. **Strip filenames** to leaf names (`split_part` on last "/")
4. **Add FK constraint** (DEFERRABLE INITIALLY IMMEDIATE)
5. **Replace UNIQUE constraint** with `UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE`
6. **Create parent index** on `(parent_id, filename)`
7. **Recreate view** -- PostgreSQL views with `SELECT *` snapshot columns at creation time; `ALTER TABLE ADD COLUMN` does NOT update existing views. The migration drops and recreates the view, preserving the tigerfs comment
8. **Migrate history table** (if exists): add parent_id, rename columns (`id` -> `file_id`, `_history_id` -> `version_id`, `_operation` -> `operation`), populate parent_id from source table, strip filenames, recreate BEFORE trigger with new column names
9. **Migrate log table** (if exists): rename `history_id` -> `version_id`, rename type values (`insert` -> `create`, `update` -> `edit`), update CHECK constraint. Order matters: drop old CHECK, rename values, then add new CHECK
10. **Create resolve_path function** (idempotent, shared across all apps)

**Note:** The history parent_id migration uses the source table's CURRENT parent_id for each file_id. This is correct for files that haven't moved, but for files that were moved, earlier history entries will have the current parent_id rather than the historical one. This is an acceptable trade-off since the full path was also not preserved in the old history model.

## Sequencing

Implement ADR-017 as **Phase 13** before continuing Phase 12 undo tasks (12.5+).

### Phase 13: Relational Directory Structure

**13.1 Schema and DDL changes**
- Update `synth/build.go`: source table with `parent_id`, new UNIQUE constraint, `resolve_path` function
- Update history table DDL: `file_id`, `parent_id`, `version_id`, `operation` columns
- Update log table DDL: `version_id`, filesystem-centric type names
- Update BEFORE trigger to copy `parent_id` and use new column names
- Unit tests for new DDL generation

**13.2 Path resolution**
- Create `resolve_path` PL/pgSQL function in `synth/build.go` DDL
- Add Go wrapper in `db/query.go`: `ResolvePath(ctx, schema, table, segments []string) (string, error)`
- Add Go-level path cache: `pathCache` with 2-second TTL, keyed on `(parent_id, filename)`
- Unit tests for path resolution and cache behavior

**13.3 ReadDir by parent_id**
- Replace `GetAllRows` + `filterHierarchicalChildren` with `SELECT * WHERE parent_id = X`
- Add `GetRowsByParent(ctx, schema, table, parentID)` to DB layer
- Remove `filterHierarchicalChildren`
- Update `readDirSynthView` and `readDirSynthHierarchical`
- Unit and integration tests

**13.4 Write path updates**
- `writeSynthFile`: resolve parent_id from path segments, insert/update with parent_id
- `ensureSynthParentDirs`: create parent chain with parent_id linking
- `mkdirSynth`: insert with parent_id
- Unit and integration tests

**13.5 Rename and move**
- `renameSynthFile`: directory rename is `UPDATE SET filename='new' WHERE id=dir_id` (1 row)
- File rename: `UPDATE SET filename='new' WHERE id=file_id`
- File/directory move: `UPDATE SET parent_id=new_parent WHERE id=X`
- Remove `RenameByPrefix`, `HasChildrenWithPrefix`
- Log entries use filesystem-centric types: `rename` for rename/move
- Unit and integration tests

**13.6 Delete path updates**
- `deleteSynthFile`: check children via `WHERE parent_id = dir_id` instead of LIKE prefix
- Remove `HasChildrenWithPrefix` usage
- Unit and integration tests

**13.7 History and .history/ updates**
- Update history queries for new column names (`file_id`, `version_id`, `operation`)
- `.history/` navigation uses parent_id traversal (same as live table)
- Update `historyIDToVersionID` for `version_id` column name
- Unit and integration tests

**13.8 Log entry updates**
- Rename operation types: create/edit/rename/delete/undo
- `logSynthOp`: compute denormalized full path by walking parent chain at log-write time
- Update `InsertLogEntry` for `version_id` column name
- Unit tests

**13.9 Migration**
- `relational-directories` migration in `tigerfs migrate` framework (`cmd/migrate.go`)
- Test with existing demo data

**13.10 Update existing tests**
- Rewrite all synth hierarchy tests for parent_id model
- Update integration tests
- Run full test suite

**13.11 Update ADR-016 and Phase 12 tasks**
- Rewrite ADR-016 sections affected by schema changes: log table schema (version_id, filesystem-centric types), history references, column naming
- Update Phase 12 tasks 12.5-12.12 in implementation-tasks.md to reference the new schema (version_id, type names, parent_id model)
- Verify consistency between ADR-016, ADR-017, and implementation tasks

**13.12 Documentation**
- Write ADR-017 (this document) to `docs/adr/`
- Update `docs/spec.md` with new schema and directory model
- Update skills if needed

After Phase 13 is complete, resume Phase 12 from task 12.5.

## Verification

### Unit and integration test coverage

All tests should cover both markdown and plain text formats.

**Basic operations (unit + integration):**
1. Create file at root level
2. Create file in a nested directory (auto-create parents)
3. Edit file content
4. Rename file (same directory)
5. Move file between directories (change parent_id)
6. Delete file
7. Create directory (mkdir)
8. Rename directory
9. Move directory between parents
10. Delete empty directory
11. Delete non-empty directory → ENOTEMPTY

**Path resolution (unit + integration):**
12. resolve_path for root-level file
13. resolve_path for deeply nested file (5 levels)
14. resolve_path for nonexistent path → returns NULL
15. resolve_path with cached start_parent (partial cache hit)
16. Path cache TTL expiry
17. Path cache invalidation on directory rename

**ReadDir (integration):**
18. ReadDir at root level → only immediate children
19. ReadDir in subdirectory → only that directory's children
20. ReadDir on empty directory → empty list
21. ReadDir performance: fewer DB rows than GetAllRows approach

**Complex undo scenarios (integration -- these are critical):**
22. Savepoint → edit file → undo to savepoint → file restored
23. Savepoint → create file → undo → file deleted
24. Savepoint → delete file → undo → file re-created
25. Savepoint → rename file → undo → old name restored
26. Savepoint → move file → undo → file back in original directory
27. Savepoint → rename directory → undo → directory and all children restored to old path
28. Savepoint → move directory → undo → directory back in original parent
29. Savepoint → edit + rename dir + edit + move dir + delete + create → undo all → exact savepoint state restored
30. Undo-of-undo: savepoint → edit → undo → undo-of-undo → post-edit state restored
31. Undo of delete then undo-of-that-undo (re-delete)
32. Idempotent: undo to same savepoint twice → same result
33. Filtered undo: per-user undo leaves other users' changes intact
34. Filtered undo FK failure: agent-1 creates dir, agent-2 adds file, undo agent-1 → error, DB unchanged
35. Filtered undo FK failure: agent-1 moves file to dir, agent-2 deletes dir, undo agent-1 → error, DB unchanged
36. UNIQUE conflict: rename A→B, create new A, undo → handled by DEFERRABLE constraint

**History navigation (integration):**
37. `.history/file.md/` shows versions by file_id
38. `.history/` after rename → shows under new name, history under old name via .by/uuid/
39. `.history/` with directory structure mirrors live parent_id hierarchy
40. File moved between directories → history entries accessible from both old and new locations via .by/uuid/

**Multi-agent (integration):**
41. Two agents: file moves between fixed directories (task board pattern) → no cache staleness
42. Two agents: one renames directory, other sees updated paths within 2 seconds

**Demo verification:**
43. Run demo seed script → log tables populated for all operations
44. Rename a directory in the demo → one log entry created
45. Undo to savepoint in demo → all files restored correctly
