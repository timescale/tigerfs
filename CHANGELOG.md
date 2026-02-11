# Changelog

All notable changes to TigerFS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-11

Full platform parity release: macOS NFS now supports all write and DDL operations,
matching Linux FUSE capabilities.

### Added

- **NFS write support on macOS** — full CRUD operations (create, update, delete rows
  and columns) via the NFS backend, previously read-only
- **DDL operations via NFS** — create, modify, and delete tables, indexes, schemas,
  and views through the filesystem on both platforms
- **Shared `fs/` package** — unified backend providing feature parity between FUSE
  and NFS without duplicating logic
- **Linux FUSE migration** — FUSE adapter now uses the shared `fs.Operations` backend
  (`--legacy-fuse` flag available for the original implementation)
- **Persistent NFS file cache** — streaming reads/writes with reference counting,
  configurable memory limits, automatic reaper, and graceful shutdown (ADR-010)
- **Editor support for DDL** — vim/emacs temp file handling and DDL trigger feedback
  via touch on `.test`, `.commit`, `.abort` control files
- **DDL grace period** — completed DDL sessions remain visible for post-close
  operations (prevents ENOENT from editors reading after close)
- **Runtime schema resolution** — schema detected at mount time instead of
  hardcoding "public"
- **Declarative integration test framework** — table-driven command tests with
  106 integration tests across NFS and FUSE
- **Test runner scripts** — `test-macos.sh` and `test-docker.sh` for consistent
  CI and local testing

### Fixed

- NFS file handles aligned to 4-byte boundary for macOS compatibility
- Large write test hangs resolved via NFS write optimizations (128KB chunks)
- GC deadlock in integration tests when NFS client/server share a process
- `pgxpool` context bug causing intermittent "conn closed" errors
- Schema flattening works with runtime-resolved schemas
- Pipeline path validation rejects disallowed combinations
- Import `.no-headers` option works on NFS
- `.info/indexes` and `.info/count` return correct sizes on NFS
- DDL trigger double-firing prevented in FUSE/NFS adapters

### Changed

- Integration tests auto-detect mount method (NFS on macOS, FUSE on Linux)
- Test names use generic prefixes (`TestMount_`, `TestWrite_`, `TestDDL_`)
  instead of platform-specific ones
- Single shared testcontainer across all integration tests for faster runs

## [0.1.0] - 2026-02-01

Initial release with full read/write filesystem operations.

### Added

- **FUSE filesystem on Linux** — mount PostgreSQL databases as directories
- **NFS filesystem on macOS** — read-only mount with no dependencies required
- **Full CRUD operations** — read, write, create, and delete rows and columns
- **Multiple data formats** — TSV, CSV, JSON, YAML with PATCH semantics on write
- **Row access patterns** — row-as-file and row-as-directory with column files
- **Type-based file extensions** — `.txt`, `.json`, `.bin` based on column type
- **All primary key types** — serial, UUID, text, and composite keys
- **Database views** — read-only and updatable view support
- **Index navigation** — `.by/column/value/` with pagination (`.first/N/`, `.last/N/`)
- **Pipeline queries** — chainable `.by/`, `.filter/`, `.order/`, `.export/` with
  database query pushdown
- **Large table handling** — `.first/N/`, `.last/N/`, `.sample/N/` pagination
- **Bulk operations** — `.export/` and `.import/` for batch data transfer
- **DDL via filesystem** — `.create/`, `.modify/`, `.delete/` for tables, indexes,
  schemas, and views (FUSE only)
- **Schema management** — automatic flattening with `.schemas/` for explicit access
- **Metadata directory** — `.info/` with schema, columns, count, ddl, indexes
- **CLI commands** — mount, unmount, status, list, config
- **Tiger Cloud integration** — `--tiger-service-id` flag for managed databases
- **PostgreSQL connection pooling** — pgx/v5 with configurable pool size
- **Permission mapping** — PostgreSQL grants mapped to Unix file permissions
- **GoReleaser builds** — linux/darwin x amd64/arm64 binaries
- **Install script** — `curl -fsSL https://tigerfs.tigerdata.com | sh`
- **Docker demo** — self-contained demo with sample data
- **macOS demo** — native demo with PostgreSQL in Docker

[0.2.0]: https://github.com/timescale/tigerfs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/timescale/tigerfs/releases/tag/v0.1.0
