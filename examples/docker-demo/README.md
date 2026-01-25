# TigerFS Docker Demo

A self-contained demo environment to try TigerFS with PostgreSQL.

## Quick Start

```bash
# From this directory
docker-compose up -d --build

# Connect to the TigerFS container
docker-compose exec tigerfs bash

# Mount the database
tigerfs mount postgres://demo:demo@postgres:5432/demo /mnt/db

# Explore!
ls /mnt/db
cat /mnt/db/users/1.tsv
cat /mnt/db/users/1.json
```

## Demo Data

The demo includes ~9,200 rows across three tables:

**users** (1,000 rows)
- Generated users with varied ages, bios, and activity status
- Fields: id, name, email, age, active, bio, created_at

**products** (200 rows)
- Widgets, Gadgets, Gizmos, Devices, Tools across 4 categories
- Fields: id, name, price, in_stock, description, category

**orders** (8,000 rows)
- Realistic order distribution with power users and varied statuses
- Fields: id, user_id, product_id, quantity, total, status, created_at

## Example Commands

```bash
# List all tables
ls /mnt/db

# Check row counts
cat /mnt/db/users/.count      # 1000
cat /mnt/db/products/.count   # 200
cat /mnt/db/orders/.count     # 8000

# List users by ID (shows first 10000 by default)
ls /mnt/db/users

# Read a user row (TSV format)
cat /mnt/db/users/1.tsv

# Read as JSON
cat /mnt/db/users/500.json

# Read single column
cat /mnt/db/users/1/email

# View products by category
cat /mnt/db/products/50.json

# Check orders for a user
ls /mnt/db/orders
cat /mnt/db/orders/100.tsv
```

## Cleanup

```bash
docker-compose down
```

## Requirements

- Docker with Compose v2
- Privileged container support (for FUSE)
