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

All randomness flows through a single `math/rand.Rand` instance initialized with the provided seed. No goroutines, no time-dependent operations in the test loop, no global random. Given the same seed and flags, runs produce the same operation sequence in most cases.

**Caveat**: determinism is best-effort, not strict. `canExecute(undo_single, ...)` consults `MostRecentLogIsAtomic()` which depends on `LogCount` -- the number of log entries an op produced -- and that count is read back over NFS via `.log/.last/N/.export/json`. NFS-write commit timing (go-nfs fabricates Open/Write/Close per WRITE RPC, and the test process and NFS server share a process) can change whether the read returns N or N+1 entries on identical inputs. When `canExecute`'s verdict flips, the PRNG is consumed at a different rate and the trace diverges. Most runs of the same seed produce identical traces; pathological cases produce different ones. **For non-reproducible failures, rely on the failure dump (below) rather than seed-based replay.**

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

# Capture diagnostic snapshots at specific iterations (run continues)
bin/tigerfs-stress start --seed 42 --iterations 1000 --dump-at 100,500,778

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
| `--dump-at LIST` | (none) | Comma-separated iteration numbers to write a snapshot dump after (e.g., `100,250,778`) |

## Output

- **stdout**: Step-by-step progress (`[STEP 1/20] create_file docs/intro.md (4.8KB)`)
- **stderr**: Errors with seed and the full replay command (all workload-affecting flags included)
- **tigerfs.log**: TigerFS stdout/stderr (written to working directory)
- **Failure dump**: `/tmp/tigerfs-stress-failure-<seed>-<iteration>-<unixtime>/` (validation failures only; see below)
- **Exit code**: 0 = pass, 1 = verification failure, 2 = infrastructure failure

## Stopping a Run

- **Ctrl-C**: Triggers clean teardown (SIGINT trapped)
- **`bin/tigerfs-stress stop`**: Reads `/tmp/tigerfs-stress.info` and tears down infrastructure (PostgreSQL, mount, mountpoint dir, info file). Does **not** delete failure dumps -- those live in a separate sibling directory and persist until you remove them manually.
- **`--keep`**: Skips teardown; infrastructure stays running for manual inspection

A validation failure also implicitly keeps the infrastructure running so you can inspect live state alongside the dump. Run `bin/tigerfs-stress stop` when you're done.

## Diagnostic Dumps

Two ways a diagnostic dump gets written:

1. **Auto** -- on validation failure (any kind: per-iteration validation, post-undo, or final). The runner writes a `failure` dump and leaves infrastructure running so you can correlate against the live database and mount.
2. **Manual** -- via `--dump-at N[,M,...]`. Writes a `snapshot` dump after the listed iterations (post-validation, before the next op). The run continues. Useful for forensics on non-reproducible runs (compare snapshots across runs of the same seed) or for narrowing down where divergence first creeps in.

Both kinds use the same machinery and produce the same set of files. They differ only in the directory prefix (`failure-` vs `snapshot-`), the `kind` field in `summary.json`, and the `summary.txt` heading. `find /tmp -name 'tigerfs-stress-failure-*'` keeps returning real failures only.

Dump location: `/tmp/tigerfs-stress-<kind>-<seed>-<iteration>-<unixtime>/`

Files in the dump:

| File | Contents |
|------|----------|
| `analysis.txt` | Pre-computed cross-references and anomaly findings (workspace status, stack island structure, log_count distribution, op counts, and any flagged regressions). **Open this first.** |
| `summary.txt` | Human-readable overview: seed, iteration, failing op, issue counts, replay command, dump path, mountpoint, postgres URL, grouped issue summary. |
| `summary.json` | Same as `summary.txt`, machine-readable for downstream tooling. |
| `expected_state.json` | The stress tester's `WorkspaceState` at the moment of failure (path -> md5 hash, plus dirs). |
| `actual_state.json` | Live filesystem snapshot (every file md5'd, every dir listed). |
| `diff.txt` | Sorted, grouped issues: missing files, unexpected files, hash mismatches, missing dirs, unexpected dirs. Each group shows the count and lists every entry. |
| `diff.json` | Same diff as structured `[]ValidationIssue` (kind/path/expected_hash/actual_hash). |
| `stack.json` | Every `StackEntry` with `LogID`, `LogCount`, captured `Files`/`Dirs`. `LogCount > 1` indicates an op that fanned out into multiple log entries (large files, NFS multi-chunk writes). |
| `operations.log` | Line-per-step trace of the entire run, mirroring stdout but kept structured (op name, description, log-entry fan-out marker). |
| `operations.json` | Same trace as `[]OpRecord`, for programmatic analysis. |
| `db_state.json` | Snapshot of the four undo-related tables: workspace rows, log, savepoints, last 200 history versions. Captured via a fresh pgx connection (separate from tigerfs's pool). |
| `db_error.txt` | Written instead of `db_state.json` if the DB capture failed (e.g., conn refused). The rest of the dump is still complete. |

The dump is best-effort: an I/O error on one file doesn't abort the rest. A warning is printed to stderr and the dump continues.

### Anomaly detection

`analysis.txt` includes automated checks that catch issues invisible to validation. Heuristics currently encoded:

| Heuristic | Catches |
|-----------|---------|
| `log_count` exceeds `ceil(write_size / 128KB) + 2` for create/edit | `lastLogID` regression after a heavy undo (the original iter-107 bug pattern) |
| `create_savepoint` with `log_count > 0` | savepoint logging changed (regression in expected behavior) |
| Stack entry's `LogID` < earlier entry's `LogID` (UUIDv7 lexicographic) | Stack bookkeeping crossed iterations in the wrong order |
| `MissingFile` + `UnexpectedFile` with the same content hash | Rename / move applied by TigerFS but not WorkspaceState (or vice versa) |

Bias is toward false positives -- a noisy flag is cheap to dismiss; a missed regression hides until the next 1000-iter run also passes silently.

### Inspecting a dump

```bash
# Read the auto-generated analysis first
cat /tmp/tigerfs-stress-failure-<seed>-<iter>-<ts>/analysis.txt

# Then the failure overview
cat /tmp/tigerfs-stress-failure-<seed>-<iter>-<ts>/summary.txt

# Look at the structured diff
cat /tmp/tigerfs-stress-failure-<seed>-<iter>-<ts>/diff.txt

# Cross-reference an expected vs. actual hash for a specific file
jq '.["path/to/file.md"]' .../expected_state.json
jq '.["path/to/file.md"]' .../actual_state.json

# Walk the stack to see what state each iteration captured
jq '.entries[] | {iteration, log_id, log_count, file_count: (.files | length)}' .../stack.json

# See the DB-side view of what TigerFS recorded
jq '.log[] | {log_id, type, filename}' .../db_state.json
```

Because the infrastructure is still running, you can also `psql` directly into the test PostgreSQL using the `conn_str` from `summary.json`, or `ls`/`cat` the live mountpoint to compare against the dump's actual_state.

## Replaying Failures

The full replay command is printed both at startup (in the failure message) and inside `summary.txt`:

```
Replay with: bin/tigerfs-stress start --seed 1713490823456 --iterations 100 --validate-every 1 --large-files
```

Replays include every workload-affecting flag (`--seed`, `--iterations`, `--validate-every`, `--large-files`, `--many-files`, `--workspace`) so the run is bit-for-bit comparable to the original. See the determinism caveat above: NFS-timing-sensitive failures may not reproduce on the same seed; in those cases the failure dump is the durable record.

## Unit Tests

The stress tester's own logic is unit tested (no Docker or TigerFS required):

```bash
go test ./test/stress/...
```

Tests cover: state deep copy, stack push/pop/savepoint, ValidateWorkspace against temp dirs, content generation determinism, operation selection weights, precondition checks.

## See Also

- [ADR-018: Stress Test](../../docs/adr/018-stress-test.md)
- Phase 14 in [Implementation Tasks](../../docs/implementation/implementation-tasks.md)
