# TigerFS macOS Demo

Run TigerFS natively on macOS with PostgreSQL in Docker.

## Prerequisites

- macOS (Apple Silicon or Intel)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Go 1.21+ (for building TigerFS)

## Quick Start

```bash
# Start the demo
./demo.sh start

# Explore the mounted filesystem
ls /tmp/tigerfs-demo
ls /tmp/tigerfs-demo/users
cat /tmp/tigerfs-demo/users/1.json
cat /tmp/tigerfs-demo/products/1.json

# Stop when done
./demo.sh stop
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  macOS Host                                             │
│  ┌─────────────────┐     ┌─────────────────────────┐    │
│  │  TigerFS        │────▶│  Docker: PostgreSQL     │    │
│  │  (native NFS)   │     │  localhost:5432         │    │
│  └────────┬────────┘     └─────────────────────────┘    │
│           │                                             │
│           ▼                                             │
│     /tmp/tigerfs-demo                                   │
└─────────────────────────────────────────────────────────┘
```

TigerFS runs as a native macOS process using the NFS backend (no FUSE required). PostgreSQL runs in a Docker container, accessible via localhost:5432.

## Commands

| Command | Description |
|---------|-------------|
| `./demo.sh start` | Start PostgreSQL and mount TigerFS |
| `./demo.sh stop` | Unmount TigerFS and stop PostgreSQL |
| `./demo.sh status` | Show current status |
| `./demo.sh restart` | Stop and start again |

## Demo Data

The demo uses the same database schema as `docker-demo/`:

- **users** (1,000 rows) - SERIAL primary key
- **categories** (10 rows) - TEXT primary key (slugs)
- **products** (200 rows) - SERIAL primary key
- **orders** (8,000 rows) - UUIDv7 primary key

## Troubleshooting

### Port 5432 already in use

Stop any existing PostgreSQL:
```bash
# If using Homebrew PostgreSQL
brew services stop postgresql

# Or check what's using the port
lsof -i :5432
```

### Mount fails

Check if a previous mount is stuck:
```bash
./demo.sh status
diskutil unmount force /tmp/tigerfs-demo
```

### Permission denied

Make sure Docker Desktop is running and has necessary permissions.
