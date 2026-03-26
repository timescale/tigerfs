# CLAUDE.md

Guidance for Claude Code when working with this repository.

## Project Overview

TigerFS is a FUSE-based filesystem that exposes PostgreSQL databases as mountable directories. Users interact with tables, rows, and columns using Unix tools (`ls`, `cat`, `grep`, `rm`) instead of SQL.

**Primary Goal:** Enable Claude Code to explore and manipulate database data using Read/Glob/Grep operations.

**Full specification:** See `docs/spec.md` for complete technical details including filesystem structure, data formats, SQL patterns, and error codes.

## Development Commands

```bash
# Build
go build -o bin/tigerfs ./cmd/tigerfs

# Test
go test ./...                    # All tests
go test -race ./...              # With race detection
go test -run TestName ./path     # Specific test

# Synth app tests
go test ./internal/tigerfs/fs/synth/...                                      # Unit (synth package)
go test -run "^TestSynth_" ./internal/tigerfs/fs/... ./test/integration/...  # Unit (synth_ops) + integration

# Before committing (required)
go fmt ./... && go vet ./... && go test ./... && go mod tidy
```

## Architecture

### Code Style

This is primarily a Go codebase. Follow Go idioms: error handling with explicit returns, table-driven tests, and gofmt formatting.

### Key Packages

| Package | Purpose |
|---------|---------|
| `cmd/tigerfs/` | Entry point with signal handling |
| `internal/tigerfs/cmd/` | Cobra commands (pure functional builders) |
| `internal/tigerfs/fs/` | Shared filesystem logic (path parsing, operations, stat cache) |
| `internal/tigerfs/fuse/` | Linux FUSE adapter |
| `internal/tigerfs/nfs/` | macOS NFS adapter |
| `internal/tigerfs/db/` | PostgreSQL client and queries |
| `internal/tigerfs/backend/` | Cloud backend resolution (Tiger, Ghost, postgres://) |
| `internal/tigerfs/mount/` | Mount registry for tracking active mounts |
| `internal/tigerfs/config/` | Viper-based configuration |
| `internal/tigerfs/logging/` | Zap structured logging |
| `internal/tigerfs/format/` | Data serialization (TSV, CSV, JSON) |

### Mount Backends

macOS uses an in-process NFS v3 server (`go-nfs`); Linux uses FUSE (`go-fuse`). Both delegate to `fs.Operations` for all filesystem logic -- adapters are thin translation layers. New filesystem behavior goes in `fs/`, not in the adapters. Note: `fuse/` still contains legacy direct-to-DB code paths; the active default path routes through `fs/`.

**go-nfs write model:** NFS v3 is stateless -- `go-nfs` fabricates Open/Write/Close per WRITE RPC. Each Close commits the buffer to the DB. Multi-chunk writes cause repeated commits until the final Close. Stat must return accurate file sizes for NFS reads (GETATTR before READ).

### Pipeline Paths

Capability directories (`.filter`, `.order`, `.first`, `.last`, `.export`, `.columns`) accumulate query state as users navigate deeper. The path `products/.filter/in_stock/true/.order/price/.first/10/.export/json` builds a single SQL query. State is tracked in `FSContext` and parsed by `fs/path.go`.

### Synth Apps

Tables whose names start with `_` (e.g., `_blog`, `_docs`) can be exposed as synthesized views -- markdown files, task lists, or plain text snippets. The synth layer (`fs/synth/`) maps filesystem operations to table rows using format-specific conventions. See `docs/markdown-app.md` and `docs/tasks-app.md`.

### SQL Identifier Quoting

**Always use `db.QuoteIdent()` and `db.QuoteTable()` when interpolating identifiers into SQL strings.** Never use `fmt.Sprintf` with `"%s"` to quote identifiers -- it does not escape embedded double quotes, which can cause SQL syntax errors or injection. Within the `db` package, use the short aliases `qi()` and `qt()`.

| Helper | Purpose | Example |
|--------|---------|---------|
| `db.QuoteIdent(name)` / `qi(name)` | Quote a single identifier (column, table, or schema) | `QuoteIdent("email")` -> `"email"` |
| `db.QuoteTable(schema, table)` / `qt(schema, table)` | Quote a schema-qualified table reference | `QuoteTable("public", "users")` -> `"public"."users"` |

**When to use:** Any column, table, or schema name interpolated into SQL via `fmt.Sprintf`. From outside the `db` package, use the exported names. Do not use for SQL keywords, parameterized values (`$1`), or pre-built clause strings.

### Command Architecture: Pure Functional Builder Pattern

**Critical pattern:** All commands use functional builders with zero global state. See any existing `buildXxxCmd()` function for the pattern.

**Rules:**
- No global variables for commands, flags, or state
- Every command built by a `buildXxxCmd()` function
- Flag variables declared locally within builder functions
- Bind flags to viper in `PersistentPreRunE`/`PreRunE`, not at build time

## Configuration

**Precedence (low to high):** Defaults → Config file (`~/.config/tigerfs/config.yaml`) → Environment (`TIGERFS_*`) → Flags

**TLS enforcement:** TigerFS enforces `sslmode=require` for non-localhost connections. The `--insecure-no-ssl` flag or `InsecureNoSSL` config disables this. Localhost (127.0.0.1, ::1, Unix sockets) defaults to `sslmode=disable` when no sslmode is specified; users can override with an explicit `?sslmode=require`.

**Rules:**
1. Always use the `Config` struct - never read from viper directly
2. Load config once at command start, pass down to functions
3. Bind flags in `PreRunE`, not at build time

**Full configuration options:** See `docs/spec.md` § Configuration System

## Consistency Model

**Design principle:** A write on one mount must be immediately visible to reads on any other mount of the same database. PostgreSQL is the single source of truth.

Multiple TigerFS mounts may connect to the same database concurrently. This means:

- **Never cache content:** Row values, export data, column values -- anything returned by `ReadFile`/`cat`/`read()` must always query the DB fresh.
- **Cache metadata only:** Sizes, permissions, directory entries, column names/types, primary keys, table lists. These change rarely and have short TTLs.
- **Stat cache keys must be unique per path:** Different pipeline paths (e.g., `.export/json` vs `.filter/active/true/.export/json`) must not share cache entries, even on the same table.
- **Pattern:** `Stat` may use caches. `ReadFile` must always hit the DB.

## Logging

Uses zap with two modes:
- **Production (default):** Warn+ level, minimal output
- **Debug (`--debug`):** All levels, colored, with timestamps and caller info

```go
logging.Debug("message", zap.String("key", "value"))
logging.Info("message", zap.Int("count", 42))
logging.Warn("message", zap.Error(err))
logging.Error("message", zap.Error(err))
```

## User Feedback via Logging

FUSE can only return errno codes, not messages. **Log detailed errors before returning errno** - output goes to stderr and user sees it:

```go
logging.Error("invalid task number",
    zap.String("number", number),
    zap.String("hint", "must be positive integers (e.g., 1, 1.2)"))
return syscall.EINVAL
```

```
$ mv 1-foo-o.md a-foo-o.md
{"message":"invalid task number","number":"a","hint":"must be positive integers (e.g., 1, 1.2)"}
mv: cannot move '1-foo-o.md' to 'a-foo-o.md': Invalid argument
```

- `logging.Error()` for failures returning errno
- `logging.Warn()` for successful operations user should know about
- Include `zap.String("hint", "...")` for guidance

See `internal/tigerfs/fuse/control_files.go` for examples.

## Testing Requirements

For each implementation task:
1. **Write unit tests** for all new functions (required)
2. **Propose integration tests** for workflows crossing package boundaries (ask before implementing)

Integration tests use testcontainers-go for PostgreSQL. See `test/integration/` for examples.

### Test Naming Convention

Integration tests that mount the filesystem work with **both** FUSE (Linux) and NFS (macOS) — the mount method is auto-detected. Name tests by what they test, not the mount method:

| Prefix | Meaning | When to use |
|--------|---------|-------------|
| `TestMount_`, `TestWrite_`, `TestDDL_`, etc. | Generic — works with any mount method | Default for all mount-based tests |
| `TestNFS_` | NFS-only — skipped on FUSE/Linux | Only if the test exercises NFS-specific behavior that cannot work with FUSE |
| `TestFUSE_` | FUSE-only — skipped on NFS/macOS | Only if the test exercises FUSE-specific behavior that cannot work with NFS |

**Do not** prefix tests with a mount method name unless they are truly specific to that method.

#### Synth Test Naming

All synth-related unit tests (in `internal/tigerfs/fs/synth/` and `internal/tigerfs/fs/`) use the `TestSynth_` prefix. This enables running all synth tests across packages with a single filter:

```bash
go test -run "^TestSynth_" ./internal/tigerfs/fs/... ./test/integration/...
```

## Code Documentation Standards

- **Core packages** (`db`, `fs`, `fuse`, `nfs`): Full function docs, field comments, "why" explanations, edge cases
- **Commands, config, utilities**: Function docs with params/returns
- **Tests**: Brief function comments only
- **Comment-code consistency:** Always check for mismatches between comments and code. Stale comments mislead -- flag to user before changing. When modifying files, update comments for code you touch but don't document the entire file.

## Development Workflow

Before implementing any feature, confirm the spec/requirements with me first. Ask clarifying questions about edge cases and expected behavior before writing code.

### Adding a Command

1. Create `internal/tigerfs/cmd/<command>.go` with `buildXxxCmd()` function
2. Add to root: `cmd.AddCommand(buildXxxCmd())` in `root.go`
3. Write unit tests for pure functions
4. Test: `go run ./cmd/tigerfs <command> --help`

### Adding Configuration

1. Add field to `Config` struct in `config/config.go`
2. Set default in `Init()`
3. Document in `docs/spec.md` § Configuration System

### Adding a FUSE Operation

1. Implement method on `FS` struct in `internal/tigerfs/fuse/`
2. Add SQL query generation in `internal/tigerfs/db/query.go`
3. Add format conversion in `internal/tigerfs/format/`
4. Write unit tests for each layer
5. Propose integration tests with recommendation (ask user before implementing)

## Git Operations

**Keep git operations simple:**
- **Add files explicitly by name** - never use `git add -A` or `git add .`
- **Make simple commits** - just `git add <files>` and `git commit`
- **No complex git operations** - avoid `git rebase`, `git commit --amend`, `git reset`, `git push --force`, or any destructive commands
- **If commits need fixing**, let the user handle it manually

## Plan File Naming

After exiting plan mode, rename the plan file from its auto-generated name to a short descriptive name (e.g., `stat-cache-pipeline-isolation.md`). Do this before starting implementation. Periodically delete completed plan files to avoid clutter.

## Commit Guidelines

```bash
# Always run before committing
go fmt ./... && go vet ./... && go test ./... && go mod tidy
```

**Commit message format:**
```
feat: add CSV format support
fix: handle NULL values in JSONB columns
docs: update configuration examples
test: add integration tests for index navigation
```

## Releasing a New Version

See `docs/release-process.md` for the full release checklist (tests, CHANGELOG, goreleaser, tagging, GitHub release notes).

## Reference Documentation

| Topic | Location |
|-------|----------|
| Filesystem structure & paths | `docs/spec.md` § Filesystem Structure |
| Data formats (TSV, CSV, JSON) | `docs/spec.md` § Data Representation |
| SQL query patterns | `docs/spec.md` § Appendix D |
| Error codes (errno mapping) | `docs/spec.md` § Error Handling |
| CLI commands & flags | `docs/spec.md` § CLI Interface |
| Tiger Cloud integration | `docs/spec.md` § Tiger Cloud Integration |
| Implementation tasks | `docs/implementation/implementation-tasks.md` |
| Task checklist | `docs/implementation/implementation-tasks-checklist.md` |

## Implementation Tasks Checklist

Keep `docs/implementation/implementation-tasks-checklist.md` in sync with `docs/implementation/implementation-tasks.md`:

- **Task completed:** Mark ⬜ → ✅ and update Summary table
- **Tasks modified:** New tasks added, tasks removed, or tasks renumbered
