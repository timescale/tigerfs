# tigerfs-stress

Comprehensive stress test for TigerFS file-first workspaces.

## What It Is

`tigerfs-stress` is a self-contained, deterministic stress test that exercises TigerFS file-first workspaces through randomized sequences of filesystem operations and undo rollbacks. It verifies correctness after every operation by comparing the actual mounted filesystem against an in-memory expected state model.

### Why

Integration tests cover individual operations and specific edge cases, but real-world usage involves long, unpredictable sequences of mixed operations with interleaved undos. This stress test catches ordering bugs, state tracking errors, undo corruption, and data loss that targeted tests miss.

### Architecture

The stress tester is a standalone Go binary that operates as a **pure filesystem client** -- all operations use `os.WriteFile`, `os.Rename`, `os.Remove`, etc. against the real NFS mount. It has no tigerfs internal imports. Undo is triggered by writing to `.undo/id/<log_id>/.apply` through the filesystem.

```
                                  NFS mount
tigerfs-stress ── os.WriteFile ─────────────> TigerFS ──> PostgreSQL
     │                                           │
     │  in-memory expected state                 │  actual data
     │  (path -> md5 hash)                       │  (rows in DB)
     │                                           │
     └── ValidateWorkspace() ── os.ReadFile ─────┘
                                + md5 compare
```

### Infrastructure Lifecycle

1. **Start**: Build tigerfs binary, spin up Docker PostgreSQL (TimescaleDB), mount TigerFS to a temp directory, create a workspace with versioned history
2. **Test**: Run N iterations of randomized operations with verification
3. **Teardown**: Kill tigerfs, unmount, docker compose down, remove temp dir

All infrastructure is managed automatically. Teardown happens on normal exit, on Ctrl-C (SIGINT/SIGTERM), or via the `stop` command from another terminal.

### Operation Loop

Each iteration:
1. **Select** a random operation from a weighted table (respecting preconditions)
2. **Push** the current expected state onto a stack (for undo rollback)
3. **Execute** the operation on the real mounted filesystem
4. **Update** the in-memory expected state
5. **Validate**: walk the real filesystem, hash every file, compare to expected

Operations are weighted to produce realistic mixes:

| Operation | Weight | Description |
|-----------|--------|-------------|
| create_file | 25 | New markdown file with random content |
| edit_file | 25 | Modify existing file body |
| rename_file | 10 | Rename within same directory |
| move_file | 10 | Move to different directory |
| delete_file | 10 | Remove existing file |
| create_dir | 5 | New subdirectory |
| rename_dir | 5 | Rename directory (cascades to contents) |
| create_savepoint | 5 | Named savepoint with snapshot |
| undo_single | 3 | Undo most recent operation |
| undo_to_id | 2 | Undo all operations after a log entry |
| undo_to_savepoint | 2 | Undo all operations after a savepoint |

### State Tracking

The expected state is a map of `relative_path -> md5_hash`. Only hashes are stored in memory -- actual file content lives in PostgreSQL via TigerFS. This keeps memory bounded even with `--large-files` mode.

Before every operation, the current state is pushed onto a stack:
- **undo_single**: pops one entry (reverts to before the last operation)
- **undo_to_id**: restores to the stack entry at that log position
- **undo_to_savepoint**: restores to the state when the savepoint was created

### Determinism

All randomness flows through a single `math/rand.Rand` instance initialized with the provided seed. No goroutines, no time-dependent operations in the test loop, no global random. Given the same seed and flags, every run produces identical operations, identical file content, identical file sizes. The seed is printed at startup for replay.

### File Size Distribution

File sizes follow a **log-normal distribution**, which models real-world file size patterns (many small files, few large ones):

| Mode | Max Size | Typical | Range |
|------|----------|---------|-------|
| Default | 100KB | ~5KB | 64B - 100KB |
| `--large-files` | 10MB | ~22KB | 64B - 10MB |

### Directory Density

| Mode | Max Files/Dir | Max Subdirs/Dir |
|------|--------------|-----------------|
| Default | 10 | 3 |
| `--many-files` | 1000 | 20 |

## Prerequisites

- Docker (for PostgreSQL with TimescaleDB)
- Go 1.22+
- macOS or Linux

## Build

```bash
go build -o bin/tigerfs-stress ./test/stress
```

## Usage

```bash
# Run with defaults (random seed, 20 iterations)
bin/tigerfs-stress start

# Reproducible run
bin/tigerfs-stress start --seed 42 --iterations 50

# Large-scale stress test
bin/tigerfs-stress start --large-files --many-files --iterations 100 --validate-every 5

# Debug mode (verbose tigerfs logging to tigerfs.log)
bin/tigerfs-stress start --debug --iterations 10

# Keep infrastructure running after test for manual inspection
bin/tigerfs-stress start --keep --seed 42

# Kill a running test from another terminal
bin/tigerfs-stress stop
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--seed N` | random | PRNG seed for reproducibility |
| `--iterations N` | 20 | Number of operation rounds |
| `--debug` | false | Pass `--log-level debug` to tigerfs |
| `--keep` | false | Don't tear down on exit |
| `--workspace NAME` | testws | Workspace name |
| `--validate-every N` | 1 | Validate every N ops (undo always validates) |
| `--large-files` | false | Enable large files up to 10MB (default max: 100KB) |
| `--many-files` | false | Enable dense directories up to 1000 files/dir (default: 10) |

## Output

- **stdout**: Step-by-step progress (`[STEP 1/20] create_file docs/intro.md (4.8KB)`)
- **stderr**: Errors with seed for replay (`[ERROR] Seed=42 Step=5: ...`)
- **tigerfs.log**: TigerFS stdout/stderr (written to working directory)
- **Exit code**: 0 = pass, 1 = verification failure, 2 = infrastructure failure

## Stopping a Run

- **Ctrl-C**: Triggers clean teardown (SIGINT trapped)
- **`bin/tigerfs-stress stop`**: Reads `/tmp/tigerfs-stress.info` and tears down from another terminal
- **`--keep`**: Skips teardown; infrastructure stays running for manual inspection

## Replaying Failures

The seed is printed at the start of every run:

```
=== tigerfs-stress ===
Seed:       1713490823456
```

To replay the exact same sequence:

```bash
bin/tigerfs-stress start --seed 1713490823456 --iterations 20
```

## Unit Tests

The stress tester's own logic is unit tested (no Docker or TigerFS required):

```bash
go test ./test/stress/...
```

Tests cover: state deep copy, stack push/pop/savepoint, ValidateWorkspace against temp dirs, content generation determinism, operation selection weights, precondition checks.

## See Also

- [ADR-018: Stress Test](../../docs/adr/018-stress-test.md)
- Phase 14 in [Implementation Tasks](../../docs/implementation/implementation-tasks.md)
