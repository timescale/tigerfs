# Session Variable Scoping via SET LOCAL

> **Status: Implemented** on branch `feature/session-var-scoping`

## Summary

Per-query PostgreSQL session variable scoping using `SET LOCAL` inside
transactions. Enables multi-tenant RLS from a single shared connection
pool — compatible with **all** PostgreSQL deployment topologies:

- Direct PostgreSQL
- PgBouncer session mode
- PgBouncer transaction mode
- RDS Proxy (no connection pinning)

Session variables flow through `context.Context` using the standard Go
idiom. A `DBTX` interface abstracts over `*pgxpool.Pool` and `pgx.Tx`,
allowing transparent transaction wrapping when session vars are present —
with zero overhead when they're not.

## Problem

TigerFS creates one connection pool per mount. For multi-tenant RLS, the
only option was encoding session variables in the connection string:

```
postgres://user:pass@host/db?options=-c%20app.user_id=42%20-c%20app.tenant_id=acme
```

This forces one pool per distinct RLS identity → N users × pool_size
backend connections. PgBouncer and RDS Proxy can't help because session
state is set at connection time and either causes pinning (RDS Proxy) or
gets lost on connection swap (PgBouncer transaction mode).

## Design

### Why SET LOCAL (not session-scoped set_config)

An earlier proposal used `set_config($1, $2, false)` (session-scoped) in
`BeforeAcquire` hooks with surgical `RESET` in `AfterRelease`. This approach:

- **Does not work with PgBouncer transaction mode** — session state gets
  lost when the pooler reassigns the backend connection
- **Causes pinning in RDS Proxy** — any `SET` statement pins the connection,
  defeating the pooler's multiplexing
- **Requires tracking and cleanup** — a `sync.Map` to track which keys
  were set per connection, and `RESET` for each on release

`SET LOCAL` (via `set_config($1, $2, true)`) avoids all of these problems.
Variables exist only for the duration of the transaction, then vanish
automatically. No cleanup, no tracking, no pinning.

### DBTX Interface

The core abstraction is a `DBTX` interface satisfied by both
`*pgxpool.Pool` and `pgx.Tx`:

```go
type DBTX interface {
    Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
    Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
    QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
```

All ~60 package-level db functions were refactored from `pool *pgxpool.Pool`
to `q DBTX`. This is a mechanical, zero-behavior-change refactor since
`*pgxpool.Pool` satisfies `DBTX`.

### acquireDBTX Pattern

The `Client.acquireDBTX(ctx)` method is the decision point:

- **No session vars**: returns `c.pool` directly with a no-op cleanup
  function. Zero overhead — no transaction started.
- **Session vars present**: begins a transaction, runs `SET LOCAL` for
  each variable via `set_config($1, $2, true)`, returns the `pgx.Tx` as
  a `DBTX`. The cleanup function commits on success, rolls back on error.

```go
func (c *Client) acquireDBTX(ctx context.Context) (DBTX, doneFunc, error) {
    vars := c.effectiveSessionVars(ctx)
    if len(vars) == 0 {
        return c.pool, func(error) {}, nil  // zero overhead
    }
    tx, err := c.pool.Begin(ctx)
    // SET LOCAL each var via applySessionVars
    // return tx + done func
}
```

Client methods use the pattern:

```go
func (c *Client) GetRow(ctx context.Context, ...) (result *Row, retErr error) {
    q, done, err := c.acquireDBTX(ctx)
    if err != nil { return nil, err }
    defer func() { done(retErr) }()
    return GetRow(ctx, q, ...)
}
```

### No Nested Transaction Risk

Package-level functions like `GetFullDDL` and `GetRowCountSmart` call
other package-level functions, threading the same `DBTX` through. Only
the top-level Client method starts a transaction via `acquireDBTX`. All
sub-calls share the same `DBTX` instance — no nested `BEGIN`.

### Baseline + Context Merge

Session variables come from two sources:

1. **Baseline vars** — mount-level, from `--session-var` flag or
   `session_variables` config. Stored on `Client.baselineVars`.
2. **Context vars** — per-request, from `db.WithSessionVars(ctx, vars)`.

`effectiveSessionVars(ctx)` merges them: baseline first, context overrides.

### Existing Transaction Methods

Methods that already manage their own transactions (`ImportOverwrite`,
`ImportSync`, `ImportAppend`, `ExecInTransaction`) inject `SET LOCAL`
at the start of their existing transaction via `applySessionVars()` —
no double-wrapping needed.

## API

### Library

```go
// Attach session vars to a context
ctx = db.WithSessionVars(ctx, db.SessionVars{
    "app.user_id":   userID,
    "app.tenant_id": tenantID,
})

// All Client methods respect session vars from context
row, err := client.GetRow(ctx, schema, table, pk)  // SET LOCAL applied
count, err := client.GetRowCount(ctx, schema, table)  // SET LOCAL applied
```

### CLI

```bash
tigerfs mount --session-var app.user_id=42 --session-var app.tenant_id=acme \
    postgres://host/db /mnt/db
```

Uses pflag's `StringToStringVar` — no custom parser.

### Config

```yaml
session_variables:
  app.user_id: "42"
  app.tenant_id: "acme"
```

No separate `session_scoping: true` flag — if `session_variables` is
non-empty, session scoping is active.

## Files

### New

| File | Purpose |
|------|---------|
| `db/dbtx.go` | `DBTX` interface + `applySessionVars` helper |
| `db/session_vars.go` | `SessionVars` type, `WithSessionVars`, `SessionVarsFromContext` |
| `db/dbtx_test.go` | Interface satisfaction tests |
| `db/session_vars_test.go` | Context API + effectiveSessionVars + acquireDBTX unit tests |
| `test/integration/session_vars_test.go` | RLS isolation, leak prevention, import, override tests |

### Modified

| File | Change |
|------|--------|
| `db/client.go` | `baselineVars` field, `effectiveSessionVars`, `acquireDBTX`, `Exec`/`ExecInTransaction` updated |
| `db/schema.go` | `*pgxpool.Pool` → `DBTX` in 23 functions; acquireDBTX in 23 Client methods |
| `db/query.go` | `*pgxpool.Pool` → `DBTX` in 12 functions; acquireDBTX in 22 Client methods; `queryRows` takes `DBTX` param |
| `db/indexes.go` | `*pgxpool.Pool` → `DBTX` in 11 functions; acquireDBTX in 10 Client methods |
| `db/export.go` | `*pgxpool.Pool` → `DBTX` in 3 functions; acquireDBTX in 5 Client methods |
| `db/keys.go` | `*pgxpool.Pool` → `DBTX` in 3 functions; acquireDBTX in 3 Client methods |
| `db/permissions.go` | `*pgxpool.Pool` → `DBTX` in 2 functions; acquireDBTX in 2 Client methods |
| `db/pipeline.go` | `*pgxpool.Pool` → `DBTX` in 2 functions; acquireDBTX in 2 Client methods |
| `db/constraints.go` | `*pgxpool.Pool` → `DBTX` in 4 functions |
| `db/import.go` | `applySessionVars` injected into 3 existing transaction methods; `truncateTable` uses acquireDBTX |
| `config/config.go` | `SessionVariables map[string]string` field added |
| `cmd/mount.go` | `--session-var` flag via `StringToStringVar` |

### Unchanged

- `db/mocks.go` — mocks implement role interfaces, not `DBTX`
- `db/interfaces.go` — `DBClient` interface signatures unchanged
- All callers of `DBClient` (fs, fuse, nfs packages) — zero changes

## Tests

### Unit (12 tests in `db/session_vars_test.go`)

- `TestWithSessionVars_NilReturnsOriginal`
- `TestWithSessionVars_EmptyReturnsOriginal`
- `TestSessionVarsFromContext_MissingReturnsNil`
- `TestSessionVarsFromContext_RoundTrip`
- `TestSessionVarsFromContext_OverwritesPrevious`
- `TestEffectiveSessionVars_NoVars`
- `TestEffectiveSessionVars_BaselineOnly`
- `TestEffectiveSessionVars_ContextOnly`
- `TestEffectiveSessionVars_ContextOverridesBaseline`
- `TestEffectiveSessionVars_Merge`
- `TestAcquireDBTX_NilPoolReturnsError`
- `TestAcquireDBTX_NoVarsDoneFuncIsNoOp`

### Unit (2 tests in `db/dbtx_test.go`)

- `TestDBTX_PoolSatisfiesInterface`
- `TestDBTX_TxSatisfiesInterface`

### Unit (1 test in `cmd/mount_test.go`)

- `TestBuildMountCmd_SessionVarFlag`

### Integration (6 tests in `test/integration/session_vars_test.go`)

- `TestSessionVars_SetLocalApplied` — verifies SET LOCAL is visible within queries
- `TestSessionVars_SetLocalDoesNotLeak` — verifies variables don't persist after tx
- `TestSessionVars_ContextOverridesBaseline` — baseline + context merge
- `TestSessionVars_NoVarsNoOverhead` — operations work without session vars
- `TestSessionVars_RLSIsolation` — end-to-end RLS with non-superuser role,
  shared pool serving alice and bob contexts, zero rows without vars
- `TestSessionVars_ImportWithVars` — import operations apply session vars

### RLS Test Note

PostgreSQL superusers always bypass RLS — this is a PG design decision,
not a TigerFS limitation. The RLS integration test creates a non-superuser
role for realistic testing. If role creation fails (insufficient privileges),
the test skips gracefully via `t.Skip`.

## Compatibility Matrix

| Deployment | Session-scoped (`set_config(..., false)`) | **SET LOCAL (implemented)** |
|---|---|---|
| Direct PostgreSQL | Works | Works |
| PgBouncer session mode | Works | Works |
| PgBouncer transaction mode | Broken (state lost) | **Works** |
| RDS Proxy | Pinned (defeats pooling) | **Works (no pinning)** |

## Future Work

- **Daemon mode**: a future `tigerfs serve` process could own a single
  pool and use `db.WithSessionVars(ctx, vars)` to scope queries per mount.
  The context API introduced here is daemon-ready.
- **Benchmarks**: measure per-query overhead of `BEGIN; SET LOCAL; query; COMMIT`
  vs. direct query. Expected to be small relative to network RTT.
