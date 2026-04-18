# tigerfs-stress

Comprehensive stress test for TigerFS file-first workspaces.

## What It Is

`tigerfs-stress` is a self-contained stress test that exercises TigerFS file-first workspaces through randomized sequences of filesystem operations and undo rollbacks. It verifies correctness after every operation by comparing the actual filesystem state against an in-memory expected state model.

**Architecture:**

1. **Infrastructure**: The test manages its own lifecycle -- it spins up a Docker PostgreSQL (TimescaleDB) container, builds and mounts TigerFS, creates a workspace with versioned history, and tears everything down on exit.

2. **Operation loop**: Each iteration selects a random filesystem operation (create, edit, rename, move, delete files and directories) or an undo operation (single undo, multi-op undo to log_id, undo to savepoint). Operations are weighted to produce realistic mixes.

3. **State tracking**: An in-memory model tracks the expected state of the workspace as a map of file paths to md5 hashes. Before every operation, the current state is pushed onto a stack. Undo operations pop or restore from this stack.

4. **Verification**: After each operation (or every N operations with `--validate-every`), the test walks the real mounted filesystem, computes md5 hashes of all files, and compares against the expected state. Any mismatch -- missing files, unexpected files, wrong content -- is a test failure.

5. **Determinism**: All randomness flows through a single seeded PRNG. Given the same seed and flags, every run produces identical operations. Failing runs can be exactly reproduced with `--seed N`.

All operations go through the real NFS mount -- `os.WriteFile`, `os.Rename`, `os.Remove`, etc. The stress tester has no tigerfs internal imports; it is a pure filesystem client.

## Prerequisites

- Docker (for PostgreSQL)
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

- **stdout**: Step-by-step progress
- **stderr**: Errors with seed for replay
- **tigerfs.log**: TigerFS debug output (in working directory)
- **Exit code**: 0 = pass, 1 = verification failure, 2 = infrastructure failure

## Replaying Failures

The seed is printed at the start of every run. To replay:

```bash
bin/tigerfs-stress start --seed <SEED_FROM_OUTPUT> --iterations <N>
```

## Unit Tests

```bash
go test ./test/stress/...
```

## See Also

- [ADR-018: Stress Test](../../docs/adr/018-stress-test.md)
- Phase 14 in [Implementation Tasks](../../docs/implementation/implementation-tasks.md)
