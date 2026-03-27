# TigerFS Demo

A ready-to-run demo environment for TigerFS with PostgreSQL.

## Quick Start

```bash
# Start the demo (auto-detects platform)
./demo.sh start

# Explore
ls /tmp/tigerfs-demo/           # macOS
# or: ./demo.sh shell           # Docker

# Stop
./demo.sh stop
```

The launcher auto-detects your platform (macOS uses native NFS, Linux uses Docker FUSE). Override with `--docker` or `--mac`:

```bash
./demo.sh start --docker    # Force Docker mode
./demo.sh start --mac       # Force macOS mode
```

## Commands

| Command | Description |
|---------|-------------|
| `./demo.sh start` | Start PostgreSQL, mount TigerFS, seed demo apps |
| `./demo.sh stop` | Unmount TigerFS and stop containers |
| `./demo.sh status` | Show current status |
| `./demo.sh restart` | Stop and start again |
| `./demo.sh shell` | Enter the TigerFS environment |

## What's Included

### Data-first tables (SQL)

Created by `init.sql` at PostgreSQL startup:

- **users** (1,000 rows) -- SERIAL primary key
- **categories** (10 rows) -- TEXT primary key (slugs)
- **products** (200 rows) -- SERIAL primary key
- **orders** (8,000 rows) -- UUIDv7 primary key
- **active_users** -- updatable view
- **order_summary** -- JOIN view
- Indexes for `.by/` navigation (single-column and composite)

### File-first apps (via mount)

Created by `seed.sh` after TigerFS mounts, using `.build/` and file writes:

| App | Format | Files | History |
|-----|--------|-------|---------|
| `blog/` | Markdown | 5 posts in 2 subdirectories | Yes |
| `docs/` | Markdown | 4 pages in 2 subdirectories | Yes |
| `snippets/` | Plain text | 3 files in 1 subdirectory | Yes |

## Example Commands

```bash
# Data-first tables
cat users/1.json                              # Read a user
cat products/.by/category/electronics/.export/json  # Products by category
ls orders/.sample/10/                         # Random orders

# File-first apps
ls blog/                                      # List blog posts
cat blog/hello-world.md                       # Read a post
cat docs/getting-started/installation.md      # Read docs
cat snippets/todo.txt                         # Read a snippet

# History (docs and blog have versioned history)
ls docs/.history/getting-started/installation.md/   # Past versions
```

## Architecture

### Docker mode (Linux)

```
Docker containers:
  postgres  -- PostgreSQL with init.sql
  tigerfs   -- TigerFS binary (FUSE mount at /mnt/db)
```

### macOS mode

```
macOS host:
  TigerFS (native NFS) --> Docker: PostgreSQL (localhost:5432)
  Mount at /tmp/tigerfs-demo
```

## Prerequisites

- **Docker mode:** Docker with Compose v2, privileged container support (for FUSE)
- **macOS mode:** macOS (Apple Silicon or Intel), Docker Desktop, Go 1.21+

## Using Claude Code

The Docker image includes Claude Code. Set `ANTHROPIC_API_KEY` before starting:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
./demo.sh start --docker
./demo.sh shell
claude
```

## Troubleshooting

### Port 5432 already in use (macOS)

```bash
brew services stop postgresql    # If using Homebrew PostgreSQL
lsof -i :5432                    # Check what's using the port
```

### Mount stuck (macOS)

```bash
./demo.sh status --mac
diskutil unmount force /tmp/tigerfs-demo
```

## File Structure

```
scripts/demo/
  README.md             # This file
  init.sql              # Data-first tables only
  seed.sh               # File-first seeding via mount
  demo.sh               # Unified launcher
  docker/
    docker-compose.yml  # PostgreSQL + TigerFS containers
    Dockerfile          # TigerFS container image
  mac/
    docker-compose.yml  # PostgreSQL container only
```
