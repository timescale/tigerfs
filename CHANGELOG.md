# Changelog

All notable changes to TigerFS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-06-02

**Operation logs, savepoints, and atomic undo -- safe exploration on a transactional filesystem.**

### Operation log, savepoints, and atomic undo

- **Operation log** (`.log/`) -- every write produces a log entry capturing user, timestamp, before/after content, and operation type; browse via `.log/.last/N/.export/json` or index queries `.log/.by/file_id/`, `.log/.by/user_id/`, `.log/.by/type/`
- **Diff symlinks** -- each log entry exposes `before`, `current`, and `after` virtual files so `diff -u .log/<id>/before .log/<id>/current` works directly
- **Savepoints** (`.savepoint/`) -- named bookmarks created by writing JSON to `.savepoint/<name>.json`; survive across sessions and users
- **Auto-savepoints** -- configurable per-mount inactivity gap (`--auto-savepoint-interval`, default 30 minutes) records bookmarks automatically at session boundaries
- **Undo** (`.undo/`) -- three modes: `.undo/id/<log_id>/.apply` (single op), `.undo/to-id/<log_id>/.apply` (multi-file rollback to a log entry), `.undo/to-savepoint/<name>/.apply` (multi-file rollback to a savepoint); preview affected files via `.undo/<mode>/<target>/.info/summary` before applying
- **Per-user undo** -- filter any undo by user via `.by/user_id/<user>/.apply`; agents only roll back their own changes in collaborative workspaces
- **Undo of undo** -- undo operations are themselves logged, so you can reverse an undo by undoing its log entry; directory renames round-trip correctly even when batched with child file renames

### Schema model and migration

- **Composite primary key support** -- tables with multi-column PKs (e.g., `PRIMARY KEY (region, user_id)`) now mount cleanly; rows appear as comma-encoded directories like `us,1`, with `,` and `/` URL-escaped in PK values
- **Relational directory structure** -- file/directory rows now reference their parent by `id` (UUID) rather than path string, so a directory rename is a single-row UPDATE that all children inherit (ADR-017)
- **`tigerfs migrate` updates** -- new migrations move history triggers and add parent-pointer columns; idempotent and dry-run-safe

### Filesystem behavior and correctness

- **POSIX rename-as-replace** -- the atomic-rename pattern used by Vim, Claude Write/Edit, and most editors works correctly: `rename(tmp, target)` replaces `target` atomically instead of erroring
- **Directory mtimes via trigger + `noac` mount option** -- directory mtimes reflect actual filesystem changes via a database-side trigger; the new `noac` mount option keeps the kernel from caching stale attributes across mounts of the same database
- **Real dotfiles in file-first workspaces** -- user-created dotfiles (`.gitignore`, `.editorconfig`) coexist with the control-surface dotnames (`.log/`, `.savepoint/`, `.undo/`, etc.) without collision in `markdown,history`-built workspaces
- **Directory-listing recursion safety** -- recursive scanners (`rm -rf`, `find`, agents probing structure) no longer infinite-loop on capability dirs; pipeline depth is capped and self-referential capability dirs are suppressed past a configurable limit
- **FUSE_INTERRUPT decoupling** -- kernel interrupt signals (from `SIGURG`-driven Go preemption) no longer propagate as `context.Canceled` into the DB layer, eliminating spurious `EIO`/`ENOENT` under load on Linux FUSE mounts

### Workspace identity and connectivity

- **Mount-level user identity via `.info/user`** -- the mount's identity (used for per-user filtering on `.log/` and `.undo/.by/user_id/`) is exposed as a virtual file; configure via `--user`, `TIGERFS_USER`, or the process owner's username
- **SELECT-only roles supported** -- primary-key and unique-constraint discovery now reads `pg_constraint` instead of relations a read-only role can't access, so workspaces mounted with a SELECT-only DB user work end-to-end

### Tools

- **`tigerfs-stress` harness** -- new companion binary for soak-testing a workspace under deterministic, PRNG-seeded filesystem + undo workloads; ships with diagnostic dumps, end-of-run monotonicity reporting, and a Docker FUSE mode (`./scripts/test-docker.sh`)

### Breaking Changes

- **0.6 -> 0.7 history format migration.** Run `tigerfs migrate` on workspaces upgraded from 0.6 -- it adds `parent_id`/`filetype`/`filename` columns to backing tables and rebuilds history triggers. The migration is idempotent. Undo of log entries created before the migration is blocked (`EPERM`) because pre-migration history has lossy `parent_id` information; `.log/` and `.history/` remain readable for those entries. Fresh 0.7 installs are unaffected.

## [0.6.0] - 2026-03-26

**Dedicated tigerfs schema, security hardening, and unified demo.**

- **Dedicated `tigerfs` schema** -- synth backing tables, triggers, functions, and history now live in a `tigerfs` schema instead of underscore-prefixed tables in the user's schema (`tigerfs.blog` + `public.blog` view, replacing `public._blog` + `public.blog` view)
- **`tigerfs migrate` command** -- migration framework with `--describe` and `--dry-run` flags; first migration moves legacy `_name` tables to the `tigerfs` schema
- **`.tables/` directory** -- browse backing tables in the `tigerfs` schema with full pipeline support
- **`.build/` schema restriction** -- `.build/` only operates in the default schema
- **TLS enforcement** -- non-localhost connections default to `sslmode=require`; localhost defaults to `sslmode=disable`; override with `--insecure-no-ssl`
- **SQL injection hardening** -- all identifier interpolation uses properly escaped helpers (`db.QuoteIdent`, `db.QuoteTable`)
- **Credential sanitization** -- connection strings in debug logs are scrubbed to prevent credential exposure
- **Agent skill auto-install** -- TigerFS skills are automatically installed when a coding agent (Claude Code, Gemini CLI, Codex) is detected at mount time
- **Unified demo** -- single `scripts/demo/` directory with `demo.sh start [--docker|--mac]` (auto-detects platform); file-first apps seeded via mount instead of hand-written SQL
- **Bug fixes** -- plain text history triggers no longer reference markdown-only columns; export/info files use full path as stat cache key

### Breaking Changes

- Backing tables are created in the `tigerfs` schema instead of the user's schema. Run `tigerfs migrate` to update existing databases.

## [0.5.0] - 2026-02-28

**Performance and observability — dramatically fewer SQL queries, flexible logging, and column projection.**

- **Column projection** — `.columns/col1,col2/` pipeline stage selects only the columns you need, reducing data transfer for wide tables
- **Stat caching** — multi-tier stat cache with row-level priming eliminates redundant queries; `ls -l` on a table directory now issues one query instead of 1+N
- **Schema cache unification** — default schema and per-schema metadata share a single cache, cutting duplicate catalog lookups
- **Synth query reduction** — synth file operations skip unnecessary table/view list queries when the table context is already resolved
- **Configurable log levels** — `--log-level debug|info|warn|error` replaces the binary `--debug` flag for fine-grained control
- **SQL parameter logging** — `--log-sql-params` flag optionally includes bind parameter values in SQL query traces
- **Grouped `config show`** — `tigerfs config show` now displays all configuration fields organized into logical sections (connection, filesystem, nfs, logging, etc.)
- **Mountpoint cleanup** — auto-created mountpoint directories are removed on unmount when empty
- **NFS pagination fix** — readdir cache handles READDIRPLUS pagination correctly for non-deterministic query results
- **Permission query fix** — schema parameter now uses explicit text cast, fixing errors on some PostgreSQL configurations

## [0.4.0] - 2026-02-25

**Cloud backends, versioned history, and CDN-based installs — manage cloud databases, travel through time, and install in seconds.**

- **Cloud backend prefixes** — `tiger:ID` and `ghost:ID` prefix scheme for connecting to Tiger Cloud and Ghost databases without raw connection strings
- **`tigerfs create` command** — provision new databases via `tigerfs create tiger:mydb` or `tigerfs create ghost:mydb`
- **`tigerfs fork` command** — fork existing databases with `tigerfs fork tiger:source`, supports point-in-time recovery
- **`tigerfs info` command** — display service details for cloud-managed databases
- **Optional mountpoint** — `tigerfs mount tiger:ID` auto-derives mountpoint from `default_mount_dir`, no explicit path needed
- **Versioned history** — `.history/` virtual directory shows previous versions of synth files using TimescaleDB continuous aggregates
- **Per-directory history** — `.history/` works at every level of hierarchical synth views, not just the root
- **Extension-aware table listing** — tables and views owned by extensions (like TimescaleDB internals) are excluded from directory listings
- **Query reduction caching** — three-tier cache strategy reduces `ls -l` on synth views from ~37 SQL queries to 1, with targeted WHERE queries for single-file lookups
- **SQL query tracing** — `--debug` mode now logs every SQL query with timing, statement text, and PostgreSQL backend PID
- **CDN-based binary distribution** — `curl -fsSL https://install.tigerfs.io | sh` with S3/CloudFront CDN, split checksums, and fast version discovery via `latest.txt`

## [0.3.0] - 2026-02-13

**Synthesized apps — work with domain-native files instead of raw rows.**

- **Markdown and plain text views** — map database rows to `.md` and `.txt` files with frontmatter, title extraction, and round-trip parsing via `.build/` scaffolding
- **Directory hierarchies** — organize synth files into nested folders (`blog/tutorials/intro.md`) backed by a path column; mkdir, rmdir, and mv all work
- **Custom frontmatter** — extra `headers` JSONB column for user-defined metadata that round-trips through file reads and writes
- **Rename (mv) on Linux** — FUSE now supports `mv` for primary key updates and synth file renames (previously macOS-only via NFS)
- **Cross-directory moves** — `mv blog/post.md blog/archive/post.md` updates the path column in the database
- **Claude Code skills** — built-in skills for discovering, reading, writing, and searching TigerFS-mounted data, plus persistent agent memory via TigerFS

## [0.2.0] - 2026-02-11

**Full platform parity — macOS and Linux now have identical capabilities.**

- **Full write support on macOS** — create, update, and delete rows and columns via NFS (previously read-only)
- **DDL on both platforms** — create, modify, and delete tables, indexes, schemas, and views via the filesystem
- **Editor-friendly DDL** — vim/emacs temp file handling, `.test`/`.commit`/`.abort` trigger via touch
- **DDL grace period** — completed sessions stay visible for post-close editor operations
- **Runtime schema resolution** — schema detected at mount time, no longer hardcoded to "public"
- **Persistent file cache** — streaming reads/writes with memory limits and graceful shutdown
- **Shared `fs/` backend** — unified FUSE/NFS logic eliminates feature drift between platforms
- **FUSE migration** — Linux FUSE now uses the shared backend (`--legacy-fuse` for original)
- **106 integration tests** — declarative test framework covering NFS (macOS) and FUSE (Linux/Docker)

## [0.1.0] - 2026-02-01

**Mount PostgreSQL as a filesystem — for humans and agents.**

- **Full CRUD via filesystem** — ls, cat, echo, rm for rows and columns
- **Row-as-file** — .json, .csv, .tsv, .yaml formats with PATCH semantics on write
- **Row-as-directory** — column files with type-based extensions (.txt, .json, .bin)
- **Index navigation** — `.by/column/value/` with pagination
- **Pipeline queries** — chainable `.by/`, `.filter/`, `.order/`, `.export/` with database pushdown
- **Large table handling** — `.first/N/`, `.last/N/`, `.sample/N/` pagination
- **Bulk export/import** — `.export/` and `.import/` for batch data transfer
- **DDL via filesystem** — `.create/`, `.modify/`, `.delete/` for tables, indexes, schemas, and views
- **Table metadata** — `.info/` with schema, columns, count, ddl, indexes
- **All primary key types** — serial, UUID, text, composite
- **Database views** — read-only and updatable
- **macOS** — native NFS backend (no dependencies)
- **Linux** — FUSE backend
- **Tiger Cloud integration** — `--tiger-service-id` for managed databases
- **CLI** — mount, unmount, status, list, config commands
- **Install script** — `curl -fsSL https://install.tigerfs.io | sh`

[0.6.0]: https://github.com/timescale/tigerfs/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/timescale/tigerfs/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/timescale/tigerfs/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/timescale/tigerfs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/timescale/tigerfs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/timescale/tigerfs/releases/tag/v0.1.0
