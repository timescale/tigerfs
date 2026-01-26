# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TigerFS is a FUSE-based filesystem that exposes PostgreSQL database contents as mountable directories. Users interact with tables, rows, and columns using standard Unix tools (`ls`, `cat`, `grep`, `rm`) instead of SQL queries.

**Primary Goal:** Enable Claude Code and developer tools to explore and manipulate database-backed data using familiar Read/Glob/Grep filesystem operations.

## Development Commands

### Building

```bash
# Build the main CLI binary
go build -o bin/tigerfs ./cmd/tigerfs

# Build all packages (verify compilation)
go build ./...

# Build with version information
go build -ldflags="-X github.com/timescale/tigerfs/internal/tigerfs/cmd.Version=v0.1.0" -o bin/tigerfs ./cmd/tigerfs
```

### Testing

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests with race detection
go test -race ./...

# Run tests with coverage
go test -coverprofile=coverage.txt ./...
go tool cover -html=coverage.txt -o coverage.html

# Run specific test
go test -run TestSpecificFunction ./internal/tigerfs/...
```

### Running Locally

```bash
# After building, run the CLI
./bin/tigerfs --help

# Or run directly with go
go run ./cmd/tigerfs --help

# Run with debug logging
go run ./cmd/tigerfs --debug version

# Test mount command (will fail until FUSE implemented)
go run ./cmd/tigerfs mount --help
```

### Code Formatting and Validation

Always run before committing:

```bash
# Format code
go fmt ./...

# Run linter
go vet ./...

# Tidy dependencies
go mod tidy

# Verify go.mod is clean
git diff --exit-code go.mod go.sum
```

## Architecture Overview

TigerFS is a Go-based FUSE filesystem that bridges PostgreSQL databases with Unix filesystem operations.

### Key Components

- **Entry Point**: `cmd/tigerfs/main.go` - Minimal main with signal handling
- **Command Structure**: `internal/tigerfs/cmd/` - Cobra-based command definitions (pure functional builders)
- **FUSE Layer**: `internal/tigerfs/fuse/` - Filesystem operations (currently stubbed)
- **Database Layer**: `internal/tigerfs/db/` - PostgreSQL client with connection pooling
- **Format Layer**: `internal/tigerfs/format/` - Data serialization (TSV, CSV, JSON) - to be created
- **Configuration**: `internal/tigerfs/config/` - Viper-based configuration management
- **Logging**: `internal/tigerfs/logging/` - Structured logging with zap

### Technology Stack

- **Go 1.23+** - Implementation language
- **Cobra** - CLI framework
- **Viper** - Configuration management (supports env vars, config files, flags)
- **Zap** - Structured logging
- **pgx/v5** - PostgreSQL driver and connection pooling
- **FUSE library** - TBD (bazil.org/fuse vs github.com/hanwen/go-fuse/v2)

## Project Structure

```
tigerfs/
├── cmd/tigerfs/              # Main CLI entry point
│   └── main.go               # Signal handling, context propagation
├── internal/tigerfs/         # Internal packages
│   ├── cmd/                  # Cobra commands (pure functional builders)
│   │   ├── root.go           # Root command with buildRootCmd()
│   │   ├── mount.go          # Mount command (default)
│   │   ├── unmount.go        # Unmount command
│   │   ├── status.go         # Status command
│   │   ├── list.go           # List mounts command
│   │   ├── test_connection.go # Test DB connection
│   │   ├── version.go        # Version command
│   │   └── config.go         # Config subcommands
│   ├── fuse/                 # FUSE filesystem implementation
│   │   ├── fs.go             # Main filesystem struct (currently stubbed)
│   │   ├── dir.go            # Directory operations (to be created)
│   │   ├── file.go           # File operations (to be created)
│   │   ├── metadata.go       # Metadata handling (to be created)
│   │   └── cache.go          # Metadata caching (to be created)
│   ├── db/                   # Database operations
│   │   ├── client.go         # PostgreSQL client (currently stubbed)
│   │   ├── query.go          # SQL query generation (to be created)
│   │   ├── schema.go         # Schema introspection (to be created)
│   │   ├── types.go          # Type handling (to be created)
│   │   └── pool.go           # Connection pooling (to be created)
│   ├── format/               # Data format conversion (to be created)
│   │   ├── tsv.go            # TSV serialization
│   │   ├── csv.go            # CSV serialization
│   │   ├── json.go           # JSON serialization
│   │   └── convert.go        # Type conversion helpers
│   ├── config/               # Configuration management
│   │   └── config.go         # Viper-based config struct
│   ├── logging/              # Structured logging
│   │   └── logging.go        # Zap logger (debug/production modes)
│   └── util/                 # Utilities (to be created)
│       ├── path.go           # Path parsing/manipulation
│       ├── permissions.go    # Permission mapping (PG → filesystem)
│       └── errors.go         # Custom errors with errno codes
├── docs/                     # Documentation
│   └── .plans/               # Design plans and specifications
├── specs/                    # Specifications
│   └── spec.md               # Complete TigerFS specification
├── scripts/                  # Build/install scripts (to be created)
│   ├── install.sh            # Unix installer
│   └── install.ps1           # Windows installer
├── .github/workflows/        # CI/CD workflows
│   ├── test.yml              # Test workflow
│   └── release.yml           # Release workflow
├── test/                     # Integration tests (to be created)
│   └── integration/
│       └── mount_test.go     # Integration test suite
├── .goreleaser.yaml          # GoReleaser configuration
├── Dockerfile                # Multi-stage Docker build
├── README.md                 # User-facing documentation
└── CLAUDE.md                 # This file - developer guidance
```

## Command Architecture: Pure Functional Builder Pattern

TigerFS uses a pure functional builder pattern with **zero global command state**, following the pattern established in tiger-cli.

### Philosophy

- **No global variables** - All commands, flags, and state are locally scoped
- **Functional builders** - Every command is built by a dedicated function
- **Complete tree building** - `buildRootCmd()` constructs the entire CLI structure
- **Perfect test isolation** - Each test gets completely fresh command instances
- **Self-contained commands** - All dependencies passed explicitly via parameters

### Root Command Builder

The root command builder creates the complete CLI structure:

```go
func buildRootCmd(ctx context.Context) *cobra.Command {
    // Declare ALL flag variables locally within this function
    var configDir string
    var debug bool
    // ... other flag variables

    cmd := &cobra.Command{
        Use:   "tigerfs",
        Short: "TigerFS - Mount PostgreSQL databases as filesystems",
        PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
            // Bind persistent flags to viper at execution time
            if err := errors.Join(
                viper.BindPFlag("debug", cmd.Flags().Lookup("debug")),
                // ... bind remaining flags
            ); err != nil {
                return fmt.Errorf("failed to bind flags: %w", err)
            }

            // Setup configuration and initialize logging
            // ... rest of initialization
        },
    }

    // Set up persistent flags
    cmd.PersistentFlags().BoolVar(&debug, "debug", false, "enable debug logging")
    // ... add remaining persistent flags

    // Add all subcommands (complete tree building)
    cmd.AddCommand(buildMountCmd(ctx))
    cmd.AddCommand(buildUnmountCmd())
    // ... add remaining subcommands

    return cmd
}
```

See `internal/tigerfs/cmd/root.go` for the complete implementation.

### Simple Command Pattern

For commands without flags:

```go
func buildVersionCmd() *cobra.Command {
    return &cobra.Command{
        Use:   "version",
        Short: "Show version information",
        Run: func(cmd *cobra.Command, args []string) {
            fmt.Printf("TigerFS %s\n", Version)
            // ... version output
        },
    }
}
```

### Commands with Local Flags

For commands that need their own flags:

```go
func buildMyFlaggedCmd() *cobra.Command {
    // Declare flag variables locally (NEVER globally!)
    var myFlag string
    var enableFeature bool

    cmd := &cobra.Command{
        Use:   "my-command",
        Short: "Command with local flags",
        RunE: func(cmd *cobra.Command, args []string) error {
            if len(args) < 1 {
                return fmt.Errorf("argument required")
            }

            cmd.SilenceUsage = true

            // Use flag variables (they're in scope)
            fmt.Printf("Flag: %s, Feature: %t\n", myFlag, enableFeature)
            return nil
        },
    }

    // Add flags - bound to local variables
    cmd.Flags().StringVar(&myFlag, "flag", "", "My flag description")
    cmd.Flags().BoolVar(&enableFeature, "enable", false, "Enable feature")

    return cmd
}
```

### Application Entry Point

The main application uses a single builder call:

```go
func Execute(ctx context.Context) error {
    // Build complete command tree fresh each time
    rootCmd := buildRootCmd(ctx)
    return rootCmd.ExecuteContext(ctx)
}
```

### No init() Functions Needed

With this pattern, commands don't need init() functions because the root command builder handles complete tree construction.

## Configuration System

TigerFS uses a layered configuration approach following Viper best practices.

### Configuration Hierarchy (Precedence: Low to High)

1. **Default values** (in code)
2. **Config file** (`~/.config/tigerfs/config.yaml`)
3. **Environment variables** (`TIGERFS_*` prefix)
4. **Command-line flags** (highest precedence)

### Configuration Struct

See `internal/tigerfs/config/config.go` for the complete `Config` struct with all available options.

**Key configuration groups:**
- **Connection** - Database connection parameters (host, port, user, database, pool settings)
- **Filesystem** - FUSE behavior (max_ls_rows, timeouts)
- **Metadata** - Schema refresh intervals
- **Logging** - Log level, format, file path
- **Formats** - Default data format (TSV/CSV/JSON), binary encoding

### Environment Variables

**PostgreSQL Standard:**
- `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD`

**TigerFS-Specific:**
- `TIGERFS_*` - All config options (e.g., `TIGERFS_MAX_LS_ROWS`, `TIGERFS_LOG_LEVEL`)
- `TIGER_SERVICE_ID` - Tiger Cloud service ID

### Configuration Best Practices

**IMPORTANT:** Follow these rules when working with configuration:

1. **Always use the Config struct** - Never read configuration values directly from the global viper instance. Always load a `Config` struct and use its fields.

2. **Load once, pass down** - Load the config once at the start of a command or operation, then pass it down to functions that need it. Do not reload the config if one is already available higher in the call chain.

3. **Bind flags in PreRunE** - Flags must be bound to viper in `PersistentPreRunE` (for persistent flags) or `PreRunE` (for command-specific flags), not at build time. This ensures correct precedence and avoids binding conflicts.

**Example:**
```go
// ✅ Good: Load config once and pass it down
func buildMountCmd(ctx context.Context) *cobra.Command {
    cmd := &cobra.Command{
        RunE: func(cmd *cobra.Command, args []string) error {
            cfg, err := config.Load()
            if err != nil {
                return err
            }
            return doMount(ctx, cfg)
        },
    }
    return cmd
}

// ❌ Bad: Reading from viper directly
func badMount() {
    maxRows := viper.GetInt("max_ls_rows") // Don't do this
}
```

### Configuration Documentation

**Important:** All configuration options must be documented in `specs/spec.md` in the Configuration System section. This is the single source of truth for:
- Config file format and all available options
- Environment variables
- Default values
- Option descriptions

When adding new config options:
1. Add the field to `internal/tigerfs/config/config.go`
2. Add the default in `Init()`
3. Document in `specs/spec.md` with a comment explaining the option

## Logging Architecture

TigerFS uses zap for structured logging with two modes:

### Production Mode (Default)
- Warn level and above
- Minimal output
- Clean formatting (no timestamps, levels in stderr)

### Debug Mode (`--debug` flag)
- Debug level and all above
- Full development logging
- Colored output
- Includes timestamps, levels, caller information

### Usage

```go
import (
    "github.com/timescale/tigerfs/internal/tigerfs/logging"
    "go.uber.org/zap"
)

logging.Debug("Debug message", zap.String("key", "value"))
logging.Info("Info message", zap.Int("count", 42))
logging.Warn("Warning message", zap.Error(err))
logging.Error("Error message", zap.String("path", "/mnt/db"))
logging.Fatal("Fatal error", zap.Error(err)) // Exits program
```

## FUSE Implementation Guidelines

### Current State

The FUSE layer is currently **stubbed** in `internal/tigerfs/fuse/`. The actual implementation will be added during Phase 1 of development.

### FUSE Library Decision (Pending)

Two main options under consideration:

**Option 1: bazil.org/fuse**
- Pure Go implementation
- Simpler API
- Well-tested and stable
- Good documentation

**Option 2: github.com/hanwen/go-fuse/v2**
- Higher performance
- More complex API
- Lower-level control
- Production-proven in large projects

**Decision Point:** Task 1.1 - Evaluate both libraries with basic benchmarks.

### FUSE Operations to Implement

Key filesystem operations that need implementation:

**Directory Operations:**
- `Lookup` - Resolve path component to inode
- `ReadDir` - List directory contents
- `Mkdir` - Create directory (for row creation)
- `Rmdir` - Remove directory (for row deletion)

**File Operations:**
- `Open` - Open file for reading/writing
- `Read` - Read file contents
- `Write` - Write file contents
- `Create` - Create new file
- `Unlink` - Delete file

**Attribute Operations:**
- `Getattr` - Get file/directory attributes (size, permissions, timestamps)
- `Setattr` - Set file/directory attributes

**Special Files:**
- Handle dotfiles (`.schema`, `.columns`, `.indexes`, `.count`)
- Handle index navigation paths (`.email/`, `.last_name.first_name/`)
- Handle pagination paths (`.first/N/`, `.sample/N/`)

### Path Parsing Pattern

Path structures follow these patterns:

```
/mount/table_name/123              # Row-as-file (PK=123)
/mount/table_name/123.json         # Row-as-file, JSON format
/mount/table_name/123/email        # Row-as-directory, column file
/mount/table_name/.email/foo@x.com # Index-based lookup
/mount/table_name/.first/100/      # First 100 rows
/mount/table_name/.sample/100/     # Random 100 rows
/mount/table_name/.count           # Row count file
/mount/table_name/.schema          # Table DDL
```

### Caching Strategy

- **Metadata caching** - Cache table schemas, column lists, indexes (refresh every 30s by default)
- **Attribute caching** - Use FUSE kernel cache with configurable timeout (1s default)
- **No data caching** - Always fetch fresh data from PostgreSQL for consistency

## Database Operations

### SQL Query Generation

All filesystem operations translate to SQL queries. Examples:

**Read row:**
```sql
-- cat /mnt/db/users/123
SELECT * FROM users WHERE id = 123;
```

**Read column:**
```sql
-- cat /mnt/db/users/123/email
SELECT email FROM users WHERE id = 123;
```

**Update column:**
```sql
-- echo 'new@example.com' > /mnt/db/users/123/email
UPDATE users SET email = 'new@example.com' WHERE id = 123;
```

**Insert row:**
```sql
-- echo '{"email":"foo@x.com","name":"Foo"}' > /mnt/db/users/123.json
INSERT INTO users (id, email, name) VALUES (123, 'foo@x.com', 'Foo');
```

**Delete row:**
```sql
-- rm /mnt/db/users/123
DELETE FROM users WHERE id = 123;
```

**Indexed lookup:**
```sql
-- cat /mnt/db/users/.email/foo@example.com/name
SELECT name FROM users WHERE email = 'foo@example.com';
```

### Query Generation Principles

1. **Parameterized queries** - Always use placeholders to prevent SQL injection
2. **Respect indexes** - Use WHERE clauses that leverage existing indexes
3. **Minimal data transfer** - SELECT only needed columns when possible
4. **Connection pooling** - Use pgx connection pool for concurrent operations
5. **Error mapping** - Map PostgreSQL errors to appropriate errno values

### Type Handling

**NULL Values:**
- TSV/CSV: Empty field
- JSON: Omit key or explicit `null`
- Column files: Empty file (0 bytes)

**Special Types:**
- **JSONB/JSON** - Compact JSON string
- **Arrays** - JSON array format
- **BYTEA** - Raw binary bytes
- **TIMESTAMP** - ISO 8601 format
- **NUMERIC** - Plain text with full precision

See `specs/spec.md` for complete type handling specifications.

## Data Format Conversion

TigerFS supports three data formats: TSV (default), CSV, and JSON.

### Format Selection

- **No extension** or `.tsv` - TSV format
- `.csv` - CSV format
- `.json` - JSON format

### Format Characteristics

**TSV (Tab-Separated Values):**
- Columns in schema order
- Tab (`\t`) delimiter
- **No header row** (prevents "header as data" bugs)
- Empty fields = NULL
- Default format for row-as-file access

**CSV (Comma-Separated Values):**
- Columns in schema order
- Comma delimiter
- **No header row**
- Empty fields = NULL
- Standard CSV quoting (RFC 4180)

**JSON:**
- Compact (single line), not pretty-printed
- All columns included
- `null` for NULL values or omit key
- Keys in any order

### NULL Handling Consistency

All formats handle NULL consistently:
- TSV/CSV: Empty field (consecutive delimiters)
- JSON: Omit key or explicit `null`
- Column files: Empty file (0 bytes)

## Testing Strategy

### Test Categories

1. **Unit Tests** - Test individual functions in isolation
2. **Integration Tests** - Test with real PostgreSQL database (testcontainers-go)
3. **FUSE Tests** - Test filesystem operations
4. **End-to-End Tests** - Test complete workflows

### Unit Testing

Place unit tests in the same package as the code being tested:

```go
// internal/tigerfs/format/tsv_test.go
package format

func TestTSVSerialize(t *testing.T) {
    // Test TSV serialization
}
```

### Integration Testing

Use testcontainers-go to spin up PostgreSQL for integration tests:

```go
func setupTestDB(t *testing.T) (string, func()) {
    ctx := context.Background()

    pgContainer, err := testcontainers.GenericContainer(ctx, ...)
    require.NoError(t, err)

    // Get connection string
    connStr := "postgres://..."

    // Seed test data
    seedTestData(t, connStr)

    cleanup := func() { pgContainer.Terminate(ctx) }
    return connStr, cleanup
}
```

### Testing Best Practices

- **Use table-driven tests** for multiple similar test cases
- **Clean up resources** in defer or cleanup functions
- **Test error cases** not just happy paths
- **Mock external dependencies** (database, FUSE) when appropriate
- **Test concurrent access** with `-race` flag

### Running Tests

```bash
# All tests
go test ./...

# Specific package
go test ./internal/tigerfs/format

# With coverage
go test -coverprofile=coverage.txt ./...

# With race detection
go test -race ./...

# Integration tests only (when implemented)
go test -tags=integration ./test/integration
```

## Tiger Cloud Integration

TigerFS integrates with Tiger Cloud for seamless database access.

### Service ID Parameters

**Command-line flag:** `--tiger-service-id=<id>`
**Environment variable:** `TIGER_SERVICE_ID=<id>`
**Config file option:** `tiger_service_id: <id>`

### Implementation

When `--tiger-service-id` is provided:
1. Call `tiger db connection-string --with-password --service-id=<id>` as subprocess
2. Parse stdout to get PostgreSQL connection string
3. Use connection string for database access

### Prerequisites

- Tiger CLI must be installed (`tiger` in PATH)
- User must be authenticated (`tiger auth login`)
- Service must exist and be running

### Error Handling

- Tiger CLI not found → Clear error message with installation link
- Not authenticated → Suggest `tiger auth login`
- Service not found → Suggest `tiger service list`
- Service not running → Suggest `tiger service start <id>`

## Error Handling

### Filesystem Errors (errno)

Map database errors to appropriate errno values:

| Database Error | errno | Description |
|---|---|---|
| Row not found | ENOENT | No such file or directory |
| Permission denied | EACCES | Permission denied |
| NOT NULL constraint | EACCES | Permission denied |
| Foreign key violation | EACCES | Permission denied |
| Connection error | EIO | Input/output error |
| Timeout | EIO | Input/output error |

### Error Logging

- **User-visible errors** - Return appropriate errno
- **Detailed errors** - Log to stderr/log file with full context
- **Structured logging** - Use zap fields for searchable logs

Example:
```go
logging.Error("Failed to execute query",
    zap.String("sql", sql),
    zap.Error(err),
    zap.String("table", "users"),
    zap.Int("row_id", 123),
)
return syscall.EIO
```

## Development Workflow

### Adding a New Command

1. Create builder function in `internal/tigerfs/cmd/<command>.go`:
   ```go
   func buildMyCmd() *cobra.Command { ... }
   ```

2. Add command to root in `internal/tigerfs/cmd/root.go`:
   ```go
   cmd.AddCommand(buildMyCmd())
   ```

3. Test the command:
   ```bash
   go run ./cmd/tigerfs my-command --help
   ```

### Adding Configuration Options

1. Add field to `Config` struct in `internal/tigerfs/config/config.go`
2. Set default in `Init()` function
3. Bind environment variable if needed
4. Document in README.md and specs/spec.md

### Adding a FUSE Operation

1. Implement method on `FS` struct in `internal/tigerfs/fuse/`
2. Add SQL query generation in `internal/tigerfs/db/query.go`
3. Add format conversion in `internal/tigerfs/format/`
4. Write unit tests for each layer
5. Write integration test for end-to-end flow

## Code Documentation Standards

This project follows specific commenting standards to ensure code is maintainable and self-documenting. Comments should explain "why" not just "what" - the code itself shows what it does.

### Comment Levels

We define four levels of documentation verbosity:

| Level | Description | Use Case |
|-------|-------------|----------|
| **Level 1 - Minimal** | Exported symbols only, brief one-line descriptions | Internal utilities, obvious code |
| **Level 2 - Standard** | Adds parameter/return docs, brief inline comments | Tests, simple internal code |
| **Level 3 - Thorough** | Full function docs, all fields documented, explains "why" | Most production code |
| **Level 4 - Comprehensive** | Package docs with examples, design rationale, edge cases | Core packages, public APIs |

### Level Assignment by Code Type

| Code Type | Level | Notes |
|-----------|-------|-------|
| Core packages (`db`, `fuse`, `mount`) | Level 3-4 | These are complex and need full documentation |
| Utility packages (`format`, `util`) | Level 3 | Important but simpler logic |
| Commands (`cmd`) | Level 3 | User-facing, needs clear documentation |
| Configuration (`config`) | Level 3 | Must document all options |
| Unit tests | Level 2 | Test names should be self-documenting |
| Integration tests | Level 2-3 | May need more context for complex setup |

### Level Examples

**Level 2 (Tests):**
```go
// TestRegistryUnregisterIdempotent verifies that unregistering a non-existent
// mountpoint doesn't cause an error.
func TestRegistryUnregisterIdempotent(t *testing.T) {
    tmpDir := t.TempDir()
    registry, err := NewRegistry(filepath.Join(tmpDir, "mounts.json"))
    if err != nil {
        t.Fatalf("NewRegistry failed: %v", err)
    }

    // Unregister something that doesn't exist - should not error
    if err := registry.Unregister("/mnt/nonexistent"); err != nil {
        t.Errorf("Unregister of non-existent entry should not error, got: %v", err)
    }
}
```

**Level 3 (Production Code):**
```go
// Registry manages the collection of active TigerFS mounts.
//
// The registry persists mount information to a JSON file, enabling commands
// like `unmount`, `status`, and `list` to discover and interact with running
// TigerFS instances.
//
// Registry is safe for concurrent use.
type Registry struct {
    // path is the filesystem path to the JSON registry file.
    path string

    // mu protects concurrent access to the registry file.
    // RLock for read operations, Lock for write operations.
    mu sync.RWMutex
}

// NewRegistry creates a new Registry instance.
//
// Parameters:
//   - path: The filesystem path for the registry file. If empty, uses DefaultRegistryPath().
//
// The function creates the parent directory if it doesn't exist (with mode 0700
// for privacy, since the registry may contain connection details).
//
// Returns an error if:
//   - path is empty and DefaultRegistryPath() fails
//   - The parent directory cannot be created
func NewRegistry(path string) (*Registry, error) {
    // ... implementation
}
```

**Level 4 (Core Package Header):**
```go
// Package mount provides mount state tracking for TigerFS filesystem instances.
//
// TigerFS mounts run as long-lived processes. To support commands like `unmount`,
// `status`, and `list`, we need to track which mounts are active. This package
// provides a Registry that persists mount information to a JSON file in the
// user's config directory (typically ~/.config/tigerfs/mounts.json).
//
// The registry stores:
//   - Mountpoint path (the directory where the filesystem is mounted)
//   - Process ID (PID) of the TigerFS process serving the mount
//   - Database connection info (sanitized - no passwords)
//   - Start time of the mount
//
// Typical usage:
//
//     registry, err := mount.NewRegistry("")
//     if err != nil {
//         return err
//     }
//     err = registry.Register(mount.Entry{
//         Mountpoint: "/mnt/db",
//         PID:        os.Getpid(),
//         Database:   "postgres://localhost/mydb",
//         StartTime:  time.Now(),
//     })
package mount
```

### Documentation Checklist

When writing new code, ensure:

- [ ] Package has a doc comment explaining its purpose (Level 3+)
- [ ] All exported types have doc comments
- [ ] All exported functions document parameters and return values (Level 3+)
- [ ] All struct fields have comments explaining their purpose (Level 3+)
- [ ] Non-obvious logic has inline comments explaining "why"
- [ ] Error conditions are documented
- [ ] Edge cases and assumptions are noted (Level 4)

### Comment-Code Consistency

**When reading or writing code, always check for inconsistencies between comments and code.** Stale or incorrect comments are worse than no comments because they actively mislead readers.

Common inconsistencies to watch for:
- Function comments that don't match the actual parameters or return values
- Comments describing behavior that the code no longer implements
- TODO comments for work that has been completed
- Example code in comments that doesn't match the current API
- Inline comments that describe what the *previous* line did (after refactoring)

**If you find inconsistencies:**
1. Flag them to the user before making changes
2. Propose correcting either the comment or the code (depending on which is wrong)
3. If unclear which is correct, ask for clarification

This applies when:
- Writing new code (ensure comments match implementation)
- Modifying existing code (update comments to reflect changes)
- Reviewing code (flag any mismatches found)

### Note on Existing Code

Existing code in this repository may not yet meet these standards. Backfilling documentation is a separate task. When modifying existing files, update comments for the code you touch but don't feel obligated to document the entire file.

## Contribution Guidelines

### Before Committing

Always run:
```bash
go fmt ./...
go vet ./...
go test ./...
go mod tidy
```

### Commit Messages

Follow conventional commit format:
```
feat: add CSV format support
fix: handle NULL values in JSONB columns
docs: update configuration examples
test: add integration tests for index navigation
```

### Pull Request Process

1. Create feature branch from `main`
2. Implement feature with tests
3. Update documentation (README.md, CLAUDE.md, specs/spec.md)
4. Ensure all tests pass and code is formatted
5. Submit PR with clear description

## Documentation Files

- **README.md** - User-facing documentation (installation, usage, examples)
- **CLAUDE.md** - This file - developer guidance for Claude Code
- **specs/spec.md** - Complete technical specification
- **docs/.plans/** - Design documents and architecture decisions

Keep all documentation files in sync when making changes to commands, configuration, or architecture.

## Useful Resources

### External Documentation

- **FUSE**: https://www.kernel.org/doc/html/latest/filesystems/fuse.html
- **PostgreSQL**: https://www.postgresql.org/docs/
- **pgx**: https://github.com/jackc/pgx
- **Cobra**: https://github.com/spf13/cobra
- **Viper**: https://github.com/spf13/viper
- **Zap**: https://github.com/uber-go/zap

### Internal Documentation

- **Complete Specification**: `specs/spec.md` (2,900+ lines)
- **Implementation Tasks**: `docs/implementation-tasks.md` (step-by-step tasks for Claude Code)
- **Design Plans**: `docs/.plans/`
- **Tiger CLI Reference**: `/Users/mfreed/Documents/tmp/tiger-cli/` (for patterns)

## Current Implementation Status

🚧 **Early Development** - Core structure established, implementation in progress.

**Completed:**
- ✅ Repository structure and build system
- ✅ CLI command framework (Cobra)
- ✅ Configuration system (Viper)
- ✅ Logging infrastructure (Zap)
- ✅ CI/CD workflows (GitHub Actions)
- ✅ Release automation (GoReleaser)

**In Progress:**
- 🔨 FUSE filesystem implementation
- 🔨 Database client and query generation
- 🔨 Data format conversion (TSV, CSV, JSON)
- 🔨 Integration tests with testcontainers

**Planned:**
- 📋 Install scripts (curl installer)
- 📋 Documentation expansion
- 📋 Performance optimization
- 📋 Production deployment patterns

See `docs/implementation-tasks.md` for the complete implementation plan.
