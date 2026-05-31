# TigerFS

A versioned filesystem backed by PostgreSQL, and a filesystem interface to PostgreSQL. Built for agents.

Every file is a real PostgreSQL row. Directories are tables. File contents are columns. Multiple agents and humans can read and write the same files concurrently with full ACID guarantees. Every change is versioned and fully reversible. No sync protocols. No coordination layer.  **The filesystem is the API.**

You can use TigerFS in two ways:

* **File-first**: Write markdown with frontmatter or other file types, organize into directories. Writes are atomic, changes are versioned, and everything is reversible: optionally take a savepoint, try changes, roll back individual or all changes if they don't work out. Any tool that works with files -- Claude Code, Cursor, grep, vim -- just works.  Build lightweight workspaces via the filesystem: multi-agent task coordination is just `mv`'ing files between todo/doing/done directories.

* **Data-first**: Mount any Postgres database and explore it with `ls`, `cat`, `grep`, and other unix tools. For
large databases, chain filters into paths that push down to SQL (e.g., the last 10 orders by customer 123 can be found at directory path
`.by/customer_id/123/.order/created_at/.last/10/.export/json`). No database client or SQL needed.

Both modes are backed by the same transactional database. You get real transactions, true concurrent access, and a SQL escape hatch when you need it. TigerFS mounts via FUSE on Linux and NFS on macOS, no extra dependencies needed.

### Agent Skills

TigerFS ships with agent skills for Claude Code, Gemini CLI, Codex, and others, automatically installed at mount time. The skills teach your agent to read, write, search, and -- new in 0.7 -- safely roll back any change. You don't need to learn the filesystem interface. Just ask:

- "Create a new workspace for my notes"
- "Set up a kanban board for our sprint with todo/doing/done columns" (file-first)
- "Set a savepoint before refactoring the code, then revert if the tests fail" (file-first)
- "What did agent-7 change in the last hour? Undo just those edits" (file-first, multi-agent)
- "Find customer alice@example.com and bump their tier to premium" (data-first)
- "Show me the last 10 orders by customer 123" (data-first)

The skills teach both modes -- **file-first** (file and markdown workspaces, directory-as-state recipes like kanban, history with savepoints and atomic undo) and **data-first** (index lookups, pipeline filters with SQL pushdown, PATCH updates, DDL via `.create/`/`.delete/`) -- all through the shared dot-directory control surface that turns every capability into a filesystem path. For details on what's underneath, read on.

### Install

```bash
curl -fsSL https://install.tigerfs.io | sh
```

**New project?** Start file-first. **Existing database?** Start data-first.

### The Filesystem is the API

Your data lives in regular files and directories. Metadata, queries, and operations live in dot-directories: invisible by default, always available.

```bash
$ ls /mnt/db/notes/
hello.md  tutorials/

$ ls -a /mnt/db/notes/
.  ..  .history/  .log/  .savepoint/  .undo/  hello.md  tutorials/
```

Dot-directories are the control surface. Navigate them to browse history, filter data, undo changes, and manage schemas, all through the same filesystem interface.

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Unix Tools  │────▶│  Filesystem  │────▶│   TigerFS    │────▶│  PostgreSQL  │
│  ls, cat,    │     │   Backend    │     │   Daemon     │     │   Database   │
│  echo, rm    │◀────│  (FUSE/NFS)  │◀────│              │◀────│              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

| File-first | Data-first |
|------------|------------|
| `.history/` past versions | `.info/` table metadata |
| `.log/` operation log with diffs | `.by/` index lookups |
| `.savepoint/` bookmarks for undo | `.filter/` column filtering |
| `.undo/` preview and apply undo | `.order/`, `.first/`, `.last/` sort and paginate |
| `.build/` create workspaces | `.export/`, `.import/` bulk I/O |
| | `.create/`, `.modify/`, `.delete/` schema management |

## File-First: Transactional Workspace

### Workspaces

Workspaces tell TigerFS how to present a table as a native file format. Write "markdown" to `.build/` and the table becomes a directory of .md files with YAML frontmatter:

```bash
# Mount a database and create a workspace with history
tigerfs mount postgres://localhost/mydb /mnt/db
echo "markdown,history" > /mnt/db/.build/blog

# Write a post. Frontmatter becomes columns, body becomes text
cat > /mnt/db/blog/hello-world.md << 'EOF'
---
title: Hello World
author: alice
tags: [intro]
---

# Hello World

Welcome to my blog...
EOF

# Search, edit, and manage content with standard tools
grep -l "author: alice" /mnt/db/blog/*.md
```

Organize files into directories. `mkdir` creates folders, `mv` moves files between them:

```bash
mkdir /mnt/db/blog/tutorials
mv /mnt/db/blog/hello-world.md /mnt/db/blog/tutorials/
```

See [docs/file-first.md](docs/file-first.md) for column mapping, frontmatter handling, and use cases.

### History, Savepoints, and Undo

Workspaces opt into automatic versioning by declaring the `history` feature at creation:

```bash
echo "markdown,history" > /mnt/db/.build/notes
```

Every write -- create, edit, rename, delete -- is captured in the operation log with timestamp and user identity. From there, four capabilities follow:

**Browse past versions.** `.history/<file>/` lists every snapshot of a file; read one to see what it looked like at that point. Useful for "what did this look like last week?" without invoking undo.

```bash
ls /mnt/db/notes/.history/plan.md/                          # all snapshots
cat /mnt/db/notes/.history/plan.md/2026-04-12T14:23:00Z     # one specific version
```

**Browse what happened.** The operation log records who changed what, when. Filter by recency, user, or operation type, and diff any individual edit:

```bash
cat /mnt/db/notes/.log/.last/10/.export/json                      # 10 most recent operations
cat /mnt/db/notes/.log/.by/user_id/agent-7/.last/5/.export/json   # filtered by user
diff -u /mnt/db/notes/.log/<id>/before /mnt/db/notes/.log/<id>/current  # diff a specific edit
```

**Savepoints for sandboxed exploration.** Create a named bookmark, try changes, preview what an undo would touch, roll back if it doesn't pan out:

```bash
echo '{"description":"Before refactoring"}' > /mnt/db/notes/.savepoint/before-refactor.json
# ... edit multiple files ...
diff -ru /mnt/db/notes/.undo/to-savepoint/before-refactor /mnt/db/notes/ -x '.*'  # review
touch /mnt/db/notes/.undo/to-savepoint/before-refactor/.apply                     # apply
```

Auto-savepoints (`--auto-savepoint-interval`, default 30m) record bookmarks at session-boundary inactivity gaps, so there's always a recent checkpoint available.

**Granular undo.** Reverse one operation by log id, everything since a log id, or everything since a savepoint:

```bash
touch /mnt/db/notes/.undo/id/<log_id>/.apply                                # one operation
touch /mnt/db/notes/.undo/to-id/<log_id>/.apply                             # everything since that log entry
touch /mnt/db/notes/.undo/to-savepoint/before-refactor/.apply               # everything since savepoint
touch /mnt/db/notes/.undo/to-savepoint/before-refactor/.by/user_id/agent-7/.apply  # only agent-7's changes since savepoint
```

Per-user filtering makes agents reversible independently in collaborative workspaces. Undo operations are themselves logged and reversible: changing your mind about an undo is just one more `.apply`.

See [docs/history.md](docs/history.md) for the full guide.

### With an agent

You don't have to run any of the commands above yourself. Once TigerFS is mounted, just talk to your agent:

> **You:** "Undo the recent changes to why-postgres.md"
> 
> **Agent:** Reads the recent log... Each edit via Claude's Write/Edit becomes 3 log entries (create temp, delete target, rename), so the two edits I see produce 6 entries. I'll apply them newest-first to roll back to the saved state.
>
> 1. `019e73d9-33c0-...` (rename)
> 2. `019e73d9-33bf-...` (delete)
> 3. `019e73d9-33b5-...` (create temp)
> 4. `019e73d9-2b1f-...` (rename)
> 5. `019e73d9-2b1e-...` (delete)
> 6. `019e73d9-2b11-...` (create temp)
>
> Apply all 6?

Because the operation log is structured and queryable through the filesystem, the agent reasons about composite operations on its own. The skills teach the patterns; you stay in natural language.

### Use Cases

**Shared agent workspace.**
Multiple agents and humans operate on the same knowledge base concurrently. Changes are visible instantly. Every edit is automatically versioned, so if one agent overwrites another's work, browse the full edit trail in `.history/` and recover it.

```bash
# Agent A writes research findings
cat > /mnt/db/kb/auth-analysis.md << 'EOF'
---
author: agent-a
---
OAuth 2.0 is the recommended approach because...
EOF

# Agent B reads it immediately, no sync, no pull
cat /mnt/db/kb/auth-analysis.md

# Browse the full edit trail
ls /mnt/db/kb/.history/auth-analysis.md/
```

**Multi-agent task queue.** Three directories (`todo/`, `doing/`, `done/`) and `mv` is your only API. Moves are atomic database operations, so two agents can't claim the same task.

```bash
# Set up a task board
echo "markdown,history" > /mnt/db/.build/tasks
mkdir /mnt/db/tasks/todo /mnt/db/tasks/doing /mnt/db/tasks/done

# Agent claims a task by moving it to doing
mv /mnt/db/tasks/todo/fix-auth-bug.md /mnt/db/tasks/doing/fix-auth-bug.md

# Marks it complete
mv /mnt/db/tasks/doing/fix-auth-bug.md /mnt/db/tasks/done/fix-auth-bug.md

# See what everyone is working on
ls /mnt/db/tasks/doing/
grep "author:" /mnt/db/tasks/doing/*.md
```

**Safe exploration.** An agent creates a savepoint, investigates a bug, makes changes across multiple files. If the approach doesn't work, undo atomically to the savepoint. Every file reverts in one step.

```bash
# Agent creates savepoint before investigating
echo '{"description":"Before investigating auth bug"}' > /mnt/db/notes/.savepoint/pre-investigation.json

# Agent explores, edits multiple files...
# User reviews: "that's not right, roll it back"
touch /mnt/db/notes/.undo/to-savepoint/pre-investigation/.apply
# All files restored to pre-investigation state
```

## Data-First: Database as Filesystem

Mount any Postgres database and explore it with `ls`, `cat`, `grep`. Every path resolves to optimized SQL pushed down to the database.

```
  Filesystem                       Database
  ──────────                       ────────
  /mnt/db/                     →   tables (default schema)
  /mnt/db/users/               →   rows (by PK)
  /mnt/db/users/123/           →   columns as files
  /mnt/db/.schemas/            →   all schemas (including default)
```

```bash
$ ls -a /mnt/db/users/
.  ..  .by/  .filter/  .order/  .first/  .last/  .info/  .export/  .import/
1/  2/  3/  4/  5/  ...
```

**Explore an unfamiliar database.** Point an agent at a mounted database and it understands the schema immediately using `ls` and `cat`. No SQL, no database client, no connection strings to pass around.

**Quick data fixes.** Update a customer's email, toggle a feature flag, delete a test record. One shell command instead of opening a SQL client, remembering the table schema, and writing a `WHERE` clause.

**Export and analyze.** Chain filters, ordering, and pagination into a single path, then pipe the result into `jq`, `awk`, or export as CSV for a spreadsheet.

### Explore

```bash
# Mount an existing database
tigerfs mount postgres://localhost/mydb /mnt/db

ls /mnt/db/                                      # List tables
ls /mnt/db/users/                                # List rows (by primary key)
cat /mnt/db/users/123.json                       # Row as JSON
cat /mnt/db/users/123/email.txt                  # Single column
cat /mnt/db/users/.by/email/foo@example.com.json # Index lookup
```

### Modify

```bash
echo 'new@example.com' > /mnt/db/users/123/email.txt            # Update column
echo '{"email":"a@b.com","name":"A"}' > /mnt/db/users/123.json  # Update via JSON (PATCH)
mkdir /mnt/db/users/456                                         # Create row
rm -r /mnt/db/users/456/                                        # Delete row
```

### With an agent

Same model on the data side. You ask, the agent navigates the schema and pipeline paths. Two examples.

**Exploration** -- agent reads the schema as files:

> **You:** "I'm new to this database. What's in the orders table?"
>
> **Agent:** Reads `/mnt/db/orders/.info/columns`, `/mnt/db/orders/.info/count`, and a sample of 5 rows...
>
> **Shape:** 47,283 rows, 8 columns. Indexed on `customer_id`, `status`, `created_at`.
>
> **Columns:** `id` (uuid PK), `customer_id` (uuid), `status` (pending/shipped/delivered/cancelled), `total` (numeric), `created_at`/`shipped_at` (timestamptz, shipped_at nullable), `payment_method`, `currency`.
>
> **Recent activity:** mostly status=shipped, payment_method=card, totals under $200. Want me to drill into a customer, date range, or status?

**Modification with confirmation** -- agent navigates to a row, plans the change, asks first:

> **You:** "Find customer alice@example.com and bump her tier to premium"
>
> **Agent:** Reads `/mnt/db/customers/.by/email/alice@example.com.json`...
>
> **Found:** customer `4172` -- Alice Chen, signed up 2025-11-04, current tier: **standard**.
>
> **Planned update:** write `premium` to `/mnt/db/customers/4172/tier`. Other fields untouched (PATCH-style).
>
> Apply?

### Pipeline Queries

Chain filters, ordering, and pagination in a single path. The database executes it as one query:

```bash
cat /mnt/db/orders/.by/customer_id/123/.order/created_at/.last/10/.export/json

# Select specific columns from a filtered query
cat /mnt/db/orders/.filter/status/shipped/.columns/id,total,created_at/.export/csv
```

Pipeline segments can be chained in any order. Available segments: `.by/` (indexed filter), `.filter/` (any column), `.order/` (sort), `.columns/col1,...`  (projection), `.first/N/`, `.last/N/`, `.sample/N/` (pagination), and `.export/csv|json|tsv` (output format). 
 

### Ingest

Bulk-load data from CSV, JSON, or YAML. The write mode is part of the path: `.append/` adds rows, `.sync/` upserts by primary key, `.overwrite/` replaces the table.

```bash
cat data.csv > /mnt/db/orders/.import/.append/csv
```

### Schema Management

Create, modify, and delete tables through a staging pattern:

```bash
mkdir /mnt/db/.create/orders && echo "CREATE TABLE orders (...)" > /mnt/db/.create/orders/sql
touch /mnt/db/.create/orders/.commit
```

See [docs/data-first.md](docs/data-first.md) for the full reference: row formats, index navigation, pipeline query chaining, schema management workflows, and configuration.

## Why TigerFS

### If you're building on files (file-first)

- **vs. local files:** Instead of a single-writer assumption, TigerFS supports real concurrent access with isolation guarantees, versioning, and undo.

- **vs. git:** No branches, no merges, no conflicts. Every write is immediately visible to everyone, and any change is atomically reversible, including selective per-user undo.

- **vs. object storage (S3):** Instead of blobs, you get structured rows, ACID transactions, and query pushdown.

### If you're querying data (data-first)

- **vs. database clients / psql:** No SQL to learn. Every agent already speaks files.

- **vs. ORMs and APIs:** No schemas to define, no SDK to install. Mount and go.

- **vs. using a database directly:** Instead of clients and schemas, you use files. Every tool and every agent already understands the interface.

The result is simple: you delete coordination code from your application.

## Cloud Backends

TigerFS works with any PostgreSQL database. Just pass a connection string. It also integrates with [Tiger Cloud](https://www.timescale.com/cloud) and [Ghost](https://ghost.build) through their CLIs for credential-free mounting. Use a prefix to specify the backend:

```bash
# Mount any Postgres database
tigerfs mount postgres://user:pass@host/mydb /mnt/db

# Or mount cloud services by ID
tigerfs mount tiger:abcde12345 /mnt/db
tigerfs mount ghost:fghij67890 /mnt/db
```

TigerFS calls the backend CLI to retrieve credentials, so there are no passwords in your config. Authenticate once with `tiger auth login` or `ghost login`.

Set a default backend to skip the prefix:

```bash
# In ~/.config/tigerfs/config.yaml: default_backend: tiger
tigerfs mount abcde12345 /mnt/db    # uses tiger: implicitly
```

### Create and Fork

```bash
# Create a new cloud database (auto-mounts)
tigerfs create tiger:my-db
tigerfs create tiger:my-db /mnt/data   # custom mount path
tigerfs create ghost:my-db --no-mount  # create without mounting

# Fork (clone) for safe experimentation
tigerfs fork /mnt/db my-experiment
tigerfs fork tiger:abcde12345 my-experiment

# Inspect a mount
tigerfs info /mnt/db
tigerfs info --json /mnt/db           # JSON output for scripting
```

## Design Principles

- **Keep the interface familiar.** If you can `ls`, you can explore a database.
- **Make concurrency safe.** Multiple writers without corruption or conflicts.
- **Push logic down.** Every path resolves to optimized SQL.
- **Make changes reversible.** Savepoints, undo, and version history mean you can always go back.
- **Remove coordination code.** The database handles it.

## Try the Demo

The **demo** is a ready-to-run TigerFS environment seeded with sample data so you can poke around with `ls`, `cat`, and friends -- see [scripts/demo/README.md](scripts/demo/README.md).

```bash
cd scripts/demo
./demo.sh start     # auto-detects platform (--docker or --mac)
./demo.sh shell     # explore: ls, cat users/1.json, etc.
./demo.sh stop
```

The **stress tester** (`tigerfs-stress`) is a self-contained correctness exerciser that runs long, randomized sequences of file operations and undo rollbacks against a mounted workspace, validating filesystem state against an in-memory model after every operation -- see [test/stress/README.md](test/stress/README.md).

```bash
go build -o bin/tigerfs-stress ./test/stress
bin/tigerfs-stress start    # randomized op + undo loop with default iterations/seed
bin/tigerfs-stress stop
```

## Configuration

Config file: `~/.config/tigerfs/config.yaml`. Run `tigerfs config show` to see all options and their current values. All options support environment variables with `TIGERFS_` prefix. See [docs/spec.md](docs/spec.md) for the full reference.

## Documentation

| Guide | Description |
|-------|-------------|
| [docs/file-first.md](docs/file-first.md) | File-first mode: workspaces, column mapping, frontmatter, directories |
| [docs/history.md](docs/history.md) | History, savepoints, and undo: versioned snapshots, safe exploration, atomic rollback |
| [docs/data-first.md](docs/data-first.md) | Data-first mode: row formats, indexes, pipeline queries, schema management |
| [docs/quickstart.md](docs/quickstart.md) | Guided scenarios with sample data |

## Development

```bash
git clone https://github.com/timescale/tigerfs.git
cd tigerfs
go build -o bin/tigerfs ./cmd/tigerfs

# Local unit tests (no cache)
go test -count=1 ./internal/tigerfs/...

# Local integration tests (no cache; spins up its own Postgres via testcontainers)
go test -count=1 -timeout 600s ./test/integration/...

# Trigger unit tests on CI for the current branch
gh workflow run test.yml --ref "$(git branch --show-current)"

# Trigger unit + integration tests on CI for the current branch
gh workflow run test.yml --ref "$(git branch --show-current)" -f run_integration=true
```

For development guidelines, architecture details, and the full specification, see [CLAUDE.md](CLAUDE.md) and [docs/spec.md](docs/spec.md).

## Project Status

TigerFS is early, but the core idea is stable: transactional, concurrent files as the foundation for human-agent collaboration.

**v0.7.0.** Undo and recovery: savepoints, operation log, and atomic undo for safe exploration.

**Highlights:**
- **Markdown and plaintext workspaces** with YAML frontmatter, directory hierarchies, and per-file version browsing via `.history/<file>/`
- **Savepoints, undo, and operation log** with `before`/`current`/`after` diff symlinks on every log entry: bookmark a state, preview changes, roll back atomically, and undo the undo if you change your mind
- **ACID concurrent writes**: multiple agents and humans operate on the same workspace without merge conflicts or sync coordination
- **Per-user undo**: multiple agents with separate identities, selectively undo one agent's work
- **Auto-savepoints:** detect session boundaries on inactivity gaps
- **Agent skills** for Claude Code, Gemini CLI, and Codex (auto-installed at mount time) that teach savepoint/undo workflows, dot-directory navigation, and data-first exploration.
- **Cloud backends**: mount, create, and fork Tiger Cloud and Ghost databases by service ID
- **Pipeline queries:** Navigate complex queries with `ls` and `cat` -- chain filters, sort, pagination, and column projection into one path that pushes down to a single optimized SQL query, so huge tables stay fast without writing SQL
- **Multi-tier stat caching and query reduction**:  Snappy `ls`, `cat`, and `find` over remote databases -- TigerFS coalesces what would be many catalog queries into one so filesystem operations feel local even across high latency
- **Schema management:** Manage schemas by writing files -- `mkdir` to create a table, edit a `.modify/` draft to alter it, review the diff, and `touch .commit` to apply
- **Format agnostic**: Read and write rows in whatever format fits the task. JSON for `jq`, YAML for hand editing, CSV for spreadsheets, single columns for shell scripts; look up by any indexed column (not just primary key); updates touch only the fields you specify
- **Security conscious**: Safe to point at production databases: TLS required for non-localhost connections, SQL injection blocked in synthesized paths, credentials scrubbed from logs and error messages
- **`tigerfs migrate`** framework for in-place updates across releases: applies schema, trigger, and index changes incrementally to bring existing workspaces forward without rebuilding (idempotent, `--dry-run` and `--describe` flags)
- **Binary distribution** via GoReleaser with install script


**Planned:**
- Tables without primary keys (read-only via ctid)
- Windows support

## Contributing

Contributions are welcome! Please see the development guidelines in [CLAUDE.md](CLAUDE.md).

## Support

- **Issues**: https://github.com/timescale/tigerfs/issues
- **Discussions**: https://github.com/timescale/tigerfs/discussions
