# ADR-013: Backend Prefix Scheme for CLI Arguments

**Status:** Accepted
**Date:** 2026-02-25
**Author:** Mike Freedman

## Context

TigerFS mounts PostgreSQL databases as directories, but currently has no way to create,
fork, or inspect backing cloud services. Users must switch to `tiger` or `ghost` CLIs
separately. The mount registry tracks connection strings but not service IDs, so there's
no way to map a filesystem back to its cloud service.

Adding `tigerfs create`, `tigerfs fork`, and `tigerfs info` commands requires a unified
scheme for referencing cloud services across commands. The underlying CLIs differ in
their interfaces:

- **Tiger CLI:** `tiger db connection-string [service-id] --with-password`,
  `tiger service get [service-id] --output json`, `tiger service create --name <n>`,
  `tiger service fork [service-id] --now --name <n>`. Create/fork wait by default (30m
  timeout, opt-out via `--no-wait`).
- **Ghost CLI:** `ghost connect <id>` (password from ~/.pgpass, no `--with-password`),
  `ghost list --json`, `ghost create --name <n> --wait --json`,
  `ghost fork <id> --name <n> --wait --json`. Create/fork require explicit `--wait`.

A single positional argument could be a service name, a service ID, or a filesystem
path — we need a consistent rule to disambiguate.

## Decision

### Prefix-Based Argument Detection

All commands use a prefix scheme to identify the backend and argument type:

```
tiger:abcde12345          # Tiger Cloud service (ID or name)
ghost:fghij67890          # Ghost database (ID or name)
postgres://user@host/db    # Direct connection string
```

**Detection rule** applied consistently across all commands:

1. **`tiger:` or `ghost:` prefix** → backend reference. The part after the colon is
   an ID or name, passed through to the backend CLI for resolution. An empty part
   (e.g., `tiger:`) means auto-generate a name.
2. **Starts with `.` or `/`** → local filesystem path. Covers `.`, `..`, `./foo`,
   `../foo`, `/tmp/foo`.
3. **`postgres://` prefix** → direct connection string (no backend).
4. **Everything else** → bare name. Backend resolved via `default_backend` config.
   Names may contain `/` (e.g., `foo/bar`) — only a *leading* `.` or `/` triggers
   path interpretation.

### Resolve Function

A single `Resolve(ref string)` function parses any reference:

```go
Resolve("tiger:abc123")     → (TigerBackend, "abc123", nil)
Resolve("ghost:mydb")       → (GhostBackend, "mydb", nil)
Resolve("postgres://...")    → (nil, "postgres://...", nil)   // direct connection
Resolve("my-db")            → (nil, "my-db", nil)            // bare name, no backend
```

When `Resolve` returns a nil backend and a non-`postgres://` string, the caller
resolves the backend via `default_backend` config or returns an error.

### Per-Command Argument Semantics

Position determines interpretation:

| Command | Arg 1 | Arg 2 |
|---------|-------|-------|
| `tigerfs mount CONNECTION MOUNTPOINT` | Detection rule | Always a path |
| `tigerfs create [BACKEND:]NAME [MOUNTPOINT]` | Detection rule (name) | Always a path |
| `tigerfs fork SOURCE [DEST]` | Detection rule (ID, name, or path) | Detection rule (name or path) |
| `tigerfs info [MOUNTPOINT]` | Always a path | — |

The returned string from `Resolve` is interpreted differently by each command: mount
and fork-source treat it as an ID to look up, create treats it as a name to create
with. The parsing is shared; the semantics are caller-defined.

### Backend Selection

**`default_backend`** is a tigerfs configuration setting:

```yaml
# ~/.config/tigerfs/config.yaml
default_backend: tiger  # "tiger", "ghost", or "" (none)
```

Env: `TIGERFS_DEFAULT_BACKEND`

| Scenario | Backend source |
|----------|---------------|
| `tigerfs create tiger:my-db` | `tiger:` prefix |
| `tigerfs create my-db` | `default_backend` config → error if unset |
| `tigerfs mount tiger:ID` | `tiger:` prefix via `Resolve()` |
| `tigerfs mount postgres://...` | Direct connection (no backend) |
| `tigerfs info /mnt/x` | Registry entry's `cli_backend` field |
| `tigerfs fork /mnt/x` | Registry entry's `cli_backend` field |
| `tigerfs fork tiger:ID` | `tiger:` prefix |
| `tigerfs fork my-db` | `default_backend` config → error if unset |

No `--backend` flag — the prefix is the mechanism.

### Commands Without a Backend

`mount` and `info` work without a backend (direct `postgres://` connections).
`create` and `fork` always require a backend.

| Command | Without backend |
|---------|----------------|
| `mount postgres://user@host/db /mnt` | Works. Direct connection. |
| `info` (on raw mount) | Local info only (mountpoint, PID, uptime). |
| `create` (no prefix, no default) | Error: "no backend specified." |
| `fork` (source is raw mount) | Error: "cannot fork — no cloud service." |

### Interface-Based Backend Package

A unified `internal/tigerfs/backend/` package replaces the existing
`internal/tigerfs/tigercloud/` package:

```go
type Backend interface {
    Name() string
    GetConnectionString(ctx context.Context, id string) (string, error)
    GetInfo(ctx context.Context, id string) (*ServiceInfo, error)
    Create(ctx context.Context, opts CreateOpts) (*CreateResult, error)
    Fork(ctx context.Context, sourceID string, opts ForkOpts) (*ForkResult, error)
}
```

Implementations shell out to the respective CLIs via `exec.CommandContext()`.
Backend-specific differences are encapsulated:

| Behavior | Tiger | Ghost |
|----------|-------|-------|
| Get connection string | `tiger db connection-string ID --with-password` | `ghost connect ID` (password from pgpass) |
| Get info | `tiger service get ID --output json` | `ghost list --json` (filter by ID) |
| Create (wait) | Waits by default (30m). Pass `--no-wait` to skip. | Requires explicit `--wait`. |
| Fork (wait) | Waits by default (30m). Pass `--no-wait` to skip. | Requires explicit `--wait`. |
| Fork strategy | `--now` (required) | None (always forks current state) |

### Name Validation

Delegated entirely to the backend CLI. TigerFS does not validate name format,
length, or character set — errors are surfaced from the CLI response.

## Alternatives Considered

1. **Heuristic detection** ("contains `/`" = path): simpler rule but prevents service
   names with `/`. Rejected to keep name format flexible.

2. **`--backend` flag alongside prefix**: more flexible but adds redundancy. Two ways
   to specify the same thing invites confusion. Rejected to keep the interface minimal.

3. **Separate commands per backend** (`tigerfs tiger-create`, `tigerfs ghost-create`):
   explicit but doubles the command surface. Rejected — the prefix scheme gives
   explicitness without duplication.

4. **Shell-level quoting for disambiguation** (e.g., `"/foo"` = name): not viable
   because the shell strips quotes before the program sees them.

## Consequences

### Benefits

1. **Consistent rule** — same prefix scheme across mount, create, fork, info
2. **Extensible** — new backends (e.g., `neon:`) require only a new Backend implementation
3. **Explicit** — `tiger:my-db` is unambiguous; no hidden defaults without opt-in config
4. **Minimal flags** — no `--backend`, no per-backend connection flags

### Limitations

1. **Names starting with `.` or `/`** cannot be specified as positional args (interpreted
   as paths). Fork's `--name` flag provides an escape hatch.
2. **Auto-generated names require prefix or config** — `tigerfs create tiger:` or a
   configured `default_backend`.
3. **Ghost has no `get` command** — `GetInfo` must filter `ghost list --json`, which
   fetches all databases. Acceptable for now; revisit if Ghost adds a `get` command.

### Tradeoffs

| Decision | Benefit | Cost |
|----------|---------|------|
| Prefix over flag | Single mechanism, no redundancy | Must configure `default_backend` for bare names |
| Shared `Resolve()` | One parsing path for all commands | Callers must handle nil-backend + bare-name case |
| CLI exec (no API) | No dependency on SDK versions | Subprocess overhead, output format coupling |
| Name validation deferred | Simpler tigerfs code | Error messages come from CLI, not tigerfs |
