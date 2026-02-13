# Changelog

All notable changes to TigerFS will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- **Install script** — `curl -fsSL https://tigerfs.tigerdata.com | sh`

[0.3.0]: https://github.com/timescale/tigerfs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/timescale/tigerfs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/timescale/tigerfs/releases/tag/v0.1.0
