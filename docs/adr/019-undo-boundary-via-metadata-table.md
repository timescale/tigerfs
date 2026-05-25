# ADR-019: Undo boundary via per-app metadata table

**Status:** Accepted
**Date:** 2026-05-24
**Author:** Mike Freedman

## Context

ADR-017's relational-directory migration rewrites the per-app history table so each row's `parent_id` reflects the file's *current* source state, not its historical state. Concretely, the migration runs:

```sql
UPDATE <app>_history h SET parent_id = (SELECT parent_id FROM <app> s WHERE s.id = h.file_id)
```

For files that were moved between directories before the migration, this destroys the historical parent_id information. Pre-migration log entries are structurally valid against the new schema -- they have the renamed columns, the leaf filename, the new `parent_id` -- but they are *semantically* unsafe to undo: a pre-migration "edit" that was actually a directory rename would silently restore content while leaving the row at its current directory location, with no signal to the user.

We need a hard cutover for undo: pre-migration log entries remain readable in `.log/` and `.history/`, but `.undo/.../.apply` refuses with `EPERM` and a hint.

## Decision

Introduce a new per-app table `tigerfs.<app>_metadata` that records non-operational events about the workspace. It is a sibling of `<app>_history`, `<app>_log`, and `<app>_savepoint`.

```sql
CREATE TABLE tigerfs.<app>_metadata (
    entry_id    UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    subject     TEXT NOT NULL,
    user_id     TEXT,
    description TEXT,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX idx_<app>_metadata_subject ON tigerfs.<app>_metadata (subject, entry_id);
```

The 0.6→0.7 migration appends one row with `subject = 'history-format-migration'` after all the schema rewrites complete. Because the row's `entry_id` is UUIDv7 (same clock domain as `log_id`), and it is inserted *after* every pre-migration log entry, lexical UUID comparison gives us correct temporal ordering across tables.

The undo engine (`internal/tigerfs/fs/undo.go`) maintains a hard-coded set of *blocking subjects*. On every `ExecuteUndoSingle`, `ExecuteUndoToLogID`, and `ExecuteUndoToSavepoint`, the engine consults the cached metadata rows for the app and refuses the operation if any blocking-subject entry has `entry_id > target_log_id`. The error carries `Code: ErrPermission` and `Hint = entry.description`, surfaced to users via tigerfs logging.

Metadata is cached at mount time in `ViewInfo.Metadata` (mount-lifetime, no TTL). For fresh installs with no metadata rows, the boundary check is a length-zero slice check -- effectively free on the hot path.

## Considered alternatives

**Marker on the log table itself** (new `type='info'` row, or new `pre_v07_migration BOOLEAN` column). Rejected for two reasons. First, mixing operations and metadata makes the log table a junk drawer over time: every consumer that means "operations" gains a filter that's easy to forget. Second, a metadata row needs a `file_id`; using a sentinel UUID (e.g. all-zeros) breaks the schema's contract that every log row references a file.

**Reserved-name savepoint** (e.g., `_v07_baseline`). Rejected because (a) the savepoint table didn't exist in pre-0.7 workspaces, so the migration would have to create it anyway, and (b) users can `rm` savepoints from the filesystem, silently disabling the boundary check.

**Lossless migration of historical parent_id**. The history table's `parent_id` could in principle be populated from a temporal walk of the directory's *own* history at the snapshot's timestamp. Rejected as high complexity for a one-time event: it requires correct handling of renamed directories, deleted-then-recreated directories, and cross-rename-tracking edge cases, with a test surface that grows with every directory-rename scenario.

**`blocks_undo` BOOLEAN column on metadata**. Considered making each row carry a flag indicating whether the undo engine should treat it as a boundary. Rejected: this couples the engine's policy to data written by arbitrary writers. The current design separates *facts* (rows in the metadata table) from *policy* (the engine's hard-coded blocking set). Adding a new blocking category becomes a deliberate engineering decision reviewed in a PR, not a column flag a writer can flip.

## Consequences

- **Generic mechanism.** The metadata table is suitable for future migration markers, schema-version notes, deprecation events, or other system metadata. New subjects don't require schema changes -- only new built-in blocking subjects require an undo-engine edit.
- **Hard cutover semantics.** Pre-migration entries are readable via `.log/`/`.history/` but not undoable. The error message points users to those alternatives.
- **Fresh installs unaffected.** An empty metadata table means `checkBoundary` returns nil immediately. Zero per-undo overhead.
- **One SQL fact, one Go constant.** Subject names and the `_metadata` table suffix live exactly once in `internal/tigerfs/fs/synth/metadata.go`; every consumer (migration, build, undo engine, tests) references them.
- **No filesystem surface yet.** Reading metadata from userspace would require a `.metadata/` capability. Purely additive when needed.

## See also

- ADR-016: Undo and Recovery
- ADR-017: Relational Directory Structure
- `docs/spec.md` § Migration Boundaries
- `docs/history.md` § Limitations (post-migration undo boundary)
