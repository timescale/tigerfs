# Changelog

All notable changes to TigerFS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

**Dedicated tigerfs schema for backing tables -- cleaner namespacing, migration support, and .tables/ directory.**

- **Backing table schema** -- synth backing tables, triggers, functions, and history tables now live in a dedicated `tigerfs` schema instead of using underscore-prefixed names in the user's schema. Before: `public._blog` + `public.blog` (view). After: `tigerfs.blog` + `public.blog` (view).
- **`.tables/` directory** -- new `/.tables/` directory lists all backing tables in the `tigerfs` schema with full pipeline support (filters, export, etc.)
- **`tigerfs migrate` command** -- general migration framework with `--describe` and `--dry-run` flags. First migration moves legacy `_name` tables to the `tigerfs` schema.
- **Legacy convention warning** -- logs a warning when old-style `_name` backing tables are detected, directing users to run `tigerfs migrate`

### Breaking Changes

- Backing tables are no longer created with underscore prefix in the user's schema. Existing databases need `tigerfs migrate` to update.

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

[0.5.0]: https://github.com/timescale/tigerfs/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/timescale/tigerfs/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/timescale/tigerfs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/timescale/tigerfs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/timescale/tigerfs/releases/tag/v0.1.0
