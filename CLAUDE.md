# CLAUDE.md

Guidance for Claude Code when working with this repository.

## Project Overview

TigerFS is a FUSE-based filesystem that exposes PostgreSQL databases as mountable directories. Users interact with tables, rows, and columns using Unix tools (`ls`, `cat`, `grep`, `rm`) instead of SQL.

**Primary Goal:** Enable Claude Code to explore and manipulate database data using Read/Glob/Grep operations.

**Full specification:** See `specs/spec.md` for complete technical details including filesystem structure, data formats, SQL patterns, and error codes.

## Development Commands

```bash
# Build
go build -o bin/tigerfs ./cmd/tigerfs

# Test
go test ./...                    # All tests
go test -race ./...              # With race detection
go test -run TestName ./path     # Specific test

# Before committing (required)
go fmt ./... && go vet ./... && go test ./... && go mod tidy
```

## Architecture

### Key Packages

| Package | Purpose |
|---------|---------|
| `cmd/tigerfs/` | Entry point with signal handling |
| `internal/tigerfs/cmd/` | Cobra commands (pure functional builders) |
| `internal/tigerfs/fuse/` | FUSE filesystem operations |
| `internal/tigerfs/db/` | PostgreSQL client and queries |
| `internal/tigerfs/mount/` | Mount registry for tracking active mounts |
| `internal/tigerfs/config/` | Viper-based configuration |
| `internal/tigerfs/logging/` | Zap structured logging |
| `internal/tigerfs/format/` | Data serialization (TSV, CSV, JSON) |

### Command Architecture: Pure Functional Builder Pattern

**Critical pattern:** All commands use functional builders with zero global state. This ensures test isolation and clean dependency management.

**Rules:**
- No global variables for commands, flags, or state
- Every command built by a `buildXxxCmd()` function
- Flag variables declared locally within builder functions
- Bind flags to viper in `PersistentPreRunE`/`PreRunE`, not at build time

**Consolidated example:**

```go
func buildMountCmd(ctx context.Context) *cobra.Command {
    // Declare flag variables locally (NEVER globally)
    var readOnly bool
    var maxLsRows int

    cmd := &cobra.Command{
        Use:   "mount [CONNECTION] MOUNTPOINT",
        Short: "Mount a PostgreSQL database as a filesystem",
        Args:  cobra.RangeArgs(1, 2),
        RunE: func(cmd *cobra.Command, args []string) error {
            cmd.SilenceUsage = true

            // Load config once, pass to functions that need it
            cfg, err := config.Load()
            if err != nil {
                return err
            }

            // Use flag variables - they're in scope via closure
            if readOnly {
                cfg.ReadOnly = true
            }

            return doMount(ctx, cfg, args)
        },
    }

    // Flags bound to local variables
    cmd.Flags().BoolVar(&readOnly, "read-only", false, "mount as read-only")
    cmd.Flags().IntVar(&maxLsRows, "max-ls-rows", 10000, "large table threshold")

    return cmd
}

// In root.go - build complete tree
func buildRootCmd(ctx context.Context) *cobra.Command {
    cmd := &cobra.Command{Use: "tigerfs"}
    cmd.AddCommand(buildMountCmd(ctx))
    cmd.AddCommand(buildUnmountCmd())
    cmd.AddCommand(buildStatusCmd())
    // ... all subcommands added here
    return cmd
}
```

## Configuration

**Precedence (low to high):** Defaults → Config file (`~/.config/tigerfs/config.yaml`) → Environment (`TIGERFS_*`) → Flags

**Rules:**
1. Always use the `Config` struct - never read from viper directly
2. Load config once at command start, pass down to functions
3. Bind flags in `PreRunE`, not at build time

**Full configuration options:** See `specs/spec.md` § Configuration System

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

## Code Documentation Standards

### Comment Levels

| Level | Use For | What to Include |
|-------|---------|-----------------|
| **Level 2** | Tests, simple utilities | Brief function comment, minimal inline |
| **Level 3** | Most production code | Full function docs with params/returns, field comments, "why" explanations |
| **Level 4** | Core packages | Package overview with examples, design rationale, edge cases |

### Level Assignment

| Code Type | Level |
|-----------|-------|
| Core packages (`db`, `fuse`, `mount`) | 3-4 |
| Commands, config, utilities | 3 |
| Tests | 2 |

### Comment-Code Consistency

**Always check for mismatches** between comments and code. Stale comments mislead. When found:
1. Flag to user before changing
2. Propose fixing either comment or code
3. Ask if unclear which is correct

### Documentation Checklist

When writing new code, ensure:
- [ ] Package has doc comment explaining its purpose (Level 3+)
- [ ] All exported types have doc comments
- [ ] All exported functions document parameters and return values (Level 3+)
- [ ] All struct fields have comments explaining their purpose (Level 3+)
- [ ] Non-obvious logic has inline comments explaining "why"
- [ ] Error conditions are documented
- [ ] Edge cases and assumptions are noted (Level 4)

### Note on Existing Code

Existing code may not meet these standards. When modifying files, update comments for code you touch but don't document the entire file.

## Development Workflow

### Adding a Command

1. Create `internal/tigerfs/cmd/<command>.go` with `buildXxxCmd()` function
2. Add to root: `cmd.AddCommand(buildXxxCmd())` in `root.go`
3. Write unit tests for pure functions
4. Test: `go run ./cmd/tigerfs <command> --help`

### Adding Configuration

1. Add field to `Config` struct in `config/config.go`
2. Set default in `Init()`
3. Document in `specs/spec.md` § Configuration System

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

## Reference Documentation

| Topic | Location |
|-------|----------|
| Filesystem structure & paths | `specs/spec.md` § Filesystem Structure |
| Data formats (TSV, CSV, JSON) | `specs/spec.md` § Data Representation |
| SQL query patterns | `specs/spec.md` § Appendix D |
| Error codes (errno mapping) | `specs/spec.md` § Error Handling |
| CLI commands & flags | `specs/spec.md` § CLI Interface |
| Tiger Cloud integration | `specs/spec.md` § Tiger Cloud Integration |
| Implementation tasks | `docs/implementation-tasks.md` |
| Task checklist | `docs/implementation-tasks-checklist.md` |

## Implementation Tasks Checklist

Keep `docs/implementation-tasks-checklist.md` in sync with `docs/implementation-tasks.md`:

- **Task completed:** Mark ⬜ → ✅ and update Summary table
- **Tasks modified:** New tasks added, tasks removed, or tasks renumbered
