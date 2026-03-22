# CLI Reference

TigerFS CLI commands for mounting, creating, forking, and managing databases.

## Mounting

```bash
tigerfs mount CONNECTION MOUNTPOINT
```

Connection formats:
- `tiger:SERVICE_ID` -- Tiger Cloud service (requires tiger CLI)
- `ghost:DATABASE_ID` -- Ghost database (requires ghost CLI)
- `postgres://user:pass@host:5432/dbname` -- direct connection string
- Omit connection to use environment variables (`PGHOST`, `PGUSER`, etc.)

Mountpoint is auto-derived from the connection if omitted (e.g., `/tmp/mydb`).

Key flags:
- `--read-only` -- mount as read-only
- `--schema` -- default schema for queries
- `--query-timeout` -- global query timeout (e.g., `30s`, `1m`)
- `--foreground` -- run in foreground (don't daemonize)

## Cloud Backends

TigerFS integrates with cloud database providers for creating, forking, and credential-free mounting. TigerFS auto-detects which CLIs are installed; specify a backend with a prefix (`tiger:` or `ghost:`) or set `default_backend` in config.

| | Tiger Cloud | Ghost |
|---|------------|-------|
| **Best for** | Production databases, data-first exploration | Ephemeral/dev databases, file-first workspaces |
| **Sign up** | https://www.tigerdata.com/cloud | https://ghost.build/ |
| **Install CLI** | `curl -fsSL https://cli.tigerdata.com \| sh` | `curl -fsSL https://install.ghost.build \| sh` |
| **Authenticate** | `tiger auth login` | `ghost login` |

## Creating a Database (Cloud backends only)

```bash
tigerfs create [BACKEND:]NAME [MOUNTPOINT]
```

Creates a new cloud database and mounts it. Requires a cloud backend CLI to be installed and authenticated.

```bash
tigerfs create tiger:my-project           # Create on Tiger Cloud, mount at /tmp/my-project
tigerfs create ghost:my-project           # Create on Ghost, mount at /tmp/my-project
tigerfs create my-project                 # Uses default_backend config
tigerfs create tiger:                     # Auto-generate name
tigerfs create tiger:my-project --no-mount  # Create without mounting
```

## Forking a Database (Cloud backends only)

```bash
tigerfs fork SOURCE [DEST]
```

Forks (copies) a cloud database and mounts the fork. Requires a cloud backend CLI to be installed and authenticated. Use fork to safely explore or experiment with existing data without affecting the original.

Source can be:
- A mountpoint path -- `tigerfs fork /mnt/prod` (looks up backend from mount registry)
- A backend reference -- `tigerfs fork tiger:SERVICE_ID`

```bash
tigerfs fork /mnt/prod                    # Fork mounted database, auto-mount the fork
tigerfs fork /mnt/prod /mnt/staging       # Fork and mount at specific path
tigerfs fork tiger:abc123 --name my-fork  # Fork by service ID with custom name
tigerfs fork /mnt/prod --no-mount         # Fork without mounting
```

### When to Create vs Fork

| Scenario | Use |
|----------|-----|
| Starting a new project from scratch | `tigerfs create` |
| Experimenting with existing data safely | `tigerfs fork` |
| Creating a dev/staging copy of production | `tigerfs fork` |
| Branching a shared workspace for isolated work | `tigerfs fork` |

## Status and Info

```bash
tigerfs status                  # List all active mounts
tigerfs info /mountpoint        # Detailed info about a mount and its backing service
tigerfs list                    # Simple list output (for scripting)
```

## Unmounting

```bash
tigerfs unmount /mountpoint
```

## Configuration

Config file: `~/.config/tigerfs/config.yaml`

```bash
tigerfs config show             # Show current configuration
tigerfs config path             # Show config file location
```

Key settings:
- `default_backend` -- `tiger` or `ghost` (used when no prefix specified)
- `default_mount_dir` -- base directory for auto-derived mountpoints (default: `/tmp`)
