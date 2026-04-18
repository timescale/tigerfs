# ADR-018: Stress Test for File-First Workspaces

**Status:** Accepted
**Date:** 2026-04-18
**Author:** Mike Freedman

## Context

TigerFS file-first workspaces support a rich set of operations -- file creation, editing, renaming, moving, deletion, directory management -- plus undo capabilities via savepoints and operation logs. These features interact in complex ways: undo-of-undo chains, rename-as-replace with history tracking, savepoint rollback across many files, concurrent state in the log/history tables.

Integration tests cover individual operations and specific edge cases, but do not exercise long sequences of mixed operations with interleaved undos. Real-world usage involves unpredictable operation sequences that may expose ordering bugs, state tracking errors, or data corruption that targeted tests miss.

We need a stress test that:
- Exercises all operation types in randomized but reproducible sequences
- Verifies correctness after every operation (not just at the end)
- Tests undo at multiple granularities (single op, multi-op to log_id, to savepoint)
- Scales to large files and dense directories
- Is self-contained (no external infrastructure dependencies beyond Docker)

## Decision

Build `tigerfs-stress`, a standalone Go binary in `test/stress/` that:

1. **Manages its own infrastructure**: spins up Docker PostgreSQL (TimescaleDB), builds and mounts TigerFS, creates a workspace, tears down on exit
2. **Runs deterministic randomized operations**: seeded PRNG drives all operation selection, target selection, and content generation -- same seed always produces identical run
3. **Tracks expected state in-memory**: maintains a `WorkspaceState` (path-to-hash map) updated after each operation, with a push/pop stack for undo rollback
4. **Validates against the real filesystem**: reads files back from the NFS mount, computes md5 hashes, compares to expected state. Detects missing files, unexpected files, and content mismatches
5. **Operates as a pure filesystem client**: all operations use `os.WriteFile`, `os.Rename`, `os.Remove`, etc. against the mounted filesystem. No tigerfs internal imports. Undo is triggered via `os.WriteFile` to `.undo/id/<id>/.apply`

### Architecture

```
                                     NFS mount
  tigerfs-stress ──── os.WriteFile ────────────> TigerFS ──> PostgreSQL
       │                                            │
       │  in-memory expected state                  │  actual data
       │  (path -> md5 hash)                        │  (rows in DB)
       │                                            │
       └── ValidateWorkspace() ── os.ReadFile ──────┘
                                  + md5 compare
```

The test loop:
1. Select random operation (weighted by type, respecting preconditions)
2. Push current expected state onto stack
3. Execute operation on real filesystem
4. Update expected state
5. Validate: walk filesystem, hash files, compare to expected
6. Repeat

For undo operations:
1. Read log entries from `.log/.last/N/.export/json`
2. Apply undo via `.undo/id/<id>/.apply` or `.undo/to-savepoint/<name>/.apply`
3. Restore expected state from stack (undo_single pops one; undo_to_savepoint restores to saved index)
4. Validate

### Operations tested

| Operation | Description |
|-----------|-------------|
| create_file | New markdown file with PRNG-generated content |
| edit_file | Modify existing file body |
| rename_file | Rename within same directory |
| move_file | Move to different directory |
| delete_file | Remove existing file |
| create_dir | New subdirectory |
| rename_dir | Rename directory |
| create_savepoint | Named savepoint with snapshot |
| undo_single | Undo most recent operation |
| undo_to_id | Undo all operations after a log entry |
| undo_to_savepoint | Undo all operations after a savepoint |

### Scale modes

- **Default**: files up to 100KB, max 10 files per directory
- **`--large-files`**: files up to 10MB, log-normal size distribution
- **`--many-files`**: up to 1000 files per directory

File sizes follow a log-normal distribution to model real-world file size patterns (many small files, few large ones).

### Determinism

All randomness flows through a single `math/rand.Rand` instance initialized with the provided seed. No goroutines, no time-dependent operations in the test loop. The seed is printed at startup; any failing run can be exactly reproduced with `--seed N`.

### State tracking

Only md5 hashes are stored in memory, not file content. Actual data lives in PostgreSQL via TigerFS. This keeps memory bounded even with `--large-files` mode -- the stress tester's heap is proportional to file count, not file size.

### Not in scope

- Concurrent multi-client testing (single sequential client)
- Data-first (table) mode testing (file-first only)
- Performance benchmarking (correctness only)
- NFS/FUSE adapter testing (goes through the real mount but doesn't test adapter-specific behavior)

## Location

`test/stress/` -- part of the root Go module but not in `cmd/`, so it is not packaged with releases. Built on demand:

```bash
go build -o bin/tigerfs-stress ./test/stress
```

## Consequences

- Every filesystem operation and undo mode has automated coverage via randomized sequences
- Regressions in undo, rename-as-replace, history tracking, or state management are caught by hash mismatches
- Seed-based reproducibility means any failure can be debugged deterministically
- The test is self-contained -- no shared infrastructure, no flaky external dependencies
- Unit tests for the stress tester itself (state tracking, validation, content generation) provide confidence in the test harness

## Implementation

See Phase 14 in `docs/implementation/implementation-tasks.md` (Tasks 14.1-14.5).
