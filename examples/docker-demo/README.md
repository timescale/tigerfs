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
cat /mnt/db/users/1
cat /mnt/db/users/1.json
```

## Demo Data

The demo includes three tables:

**users** (5 rows)
- Alice, Bob, Charlie, Diana, Eve
- Fields: id, name, email, age, active, bio, created_at

**products** (4 rows)
- Widget, Gadget, Doohickey, Thingamajig
- Fields: id, name, price, in_stock, description

**orders** (4 rows)
- Sample orders linking users to products
- Fields: id, user_id, product_id, quantity, total, status, created_at

## Example Commands

```bash
# List all tables
ls /mnt/db

# List users by ID
ls /mnt/db/users

# Read a user row (TSV format)
cat /mnt/db/users/1

# Read as JSON
cat /mnt/db/users/1.json

# Read single column
cat /mnt/db/users/1/email

# View products
cat /mnt/db/products/2.json

# Check orders
ls /mnt/db/orders
cat /mnt/db/orders/1
```

## Cleanup

```bash
docker-compose down
```

## Requirements

- Docker with Compose v2
- Privileged container support (for FUSE)
