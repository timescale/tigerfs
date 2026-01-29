# TigerFS Docker Demo

A containerized environment to try TigerFS with PostgreSQL.

## Quick Start

Choose your connection method:

- **Option 1: Local PostgreSQL** (default) - Self-contained demo with sample data
- **Option 2: Tiger Cloud** - Connect to your Tiger Cloud database

### Option 1: Local PostgreSQL (Recommended for Testing)

```bash
# From this directory
cd examples/docker-demo

# Optional: set API key if you want to use Claude Code inside the container
export ANTHROPIC_API_KEY=sk-ant-...

# Build and start containers
docker-compose up -d --build

# Connect to the TigerFS container
docker-compose exec tigerfs bash

# Mount the local demo database
mnt  # Alias for: tigerfs mount postgres://demo:demo@postgres:5432/demo /mnt/db &

# Explore!
ls /mnt/db
ls /mnt/db/users/.first/5/           # First 5 users
cat /mnt/db/users/.first/1/          # First user as TSV
```

### Option 2: Tiger Cloud

Connect to your Tiger Cloud database using headless authentication.

**Prerequisites:**
1. Create client credentials in Tiger Cloud Console → Settings → Create credentials
2. Note your Project ID and Service ID

```bash
# From this directory
cd examples/docker-demo

# Build and start (only the tigerfs container needed)
docker-compose up -d --build tigerfs

# Connect with Tiger Cloud credentials
docker-compose exec tigerfs bash

# Inside container: Install Tiger CLI
curl -fsSL https://cli.tigerdata.com | sh

# Set credentials (replace with your values)
export TIGER_PUBLIC_KEY=<your-public-key>
export TIGER_SECRET_KEY=<your-secret-key>
export TIGER_PROJECT_ID=<your-project-id>

# Authenticate (uses env vars, no browser needed)
tiger auth login

# Mount your Tiger Cloud database
export TIGER_SERVICE_ID=<your-service-id>
tigerfs mount /mnt/db &

# Explore your data
ls /mnt/db
```

## Demo Data (Option 1 only)

The local PostgreSQL demo includes ~9,200 rows across four tables demonstrating mixed primary key types.

**users** (1,000 rows) - SERIAL primary key
- Generated users with realistic name distributions
- Fields: id (integer), name, first_name, last_name, email, age, active, bio, created_at

**categories** (10 rows) - TEXT primary key (slug-based)
- Product categories with human-readable slugs as primary keys
- Fields: slug (text), name, description, icon, display_order, active, created_at

**products** (200 rows) - SERIAL primary key
- Widgets, Gadgets, Gizmos, Devices, Tools across 10 categories
- Fields: id (integer), name, price, in_stock, description, category (FK)

**orders** (8,000 rows) - UUIDv7 primary key (time-sortable, PostgreSQL 18 native)
- Realistic order distribution with power users and varied statuses
- Fields: id (uuid), user_id (integer), product_id (integer), quantity, total, status, created_at

**Indexes** (for index-based navigation via `.by/`)
- Single-column: `email`, `category`, `user_id`, `created_at`
- Composite: `last_name.first_name`, `status.created_at`

## Example Commands

```bash
# List all tables
ls /mnt/db

# Check row counts
cat /mnt/db/users/.info/count      # 1000
cat /mnt/db/products/.info/count   # 200
cat /mnt/db/orders/.info/count     # 8000

# View table schema
cat /mnt/db/users/.info/schema
cat /mnt/db/users/.info/ddl        # Full DDL with indexes

# List first 10 users (integer IDs as filenames)
ls /mnt/db/users/.first/10/

# Read a user row by ID
cat /mnt/db/users/1.json

# Read single column
cat /mnt/db/users/1/email

# Order by column
ls /mnt/db/users/.order/created_at/first/10/   # 10 oldest users
ls /mnt/db/users/.order/age/last/5/            # 5 oldest by age

# Categories use TEXT primary keys (slug filenames)
ls /mnt/db/categories/                          # electronics, home, office, outdoor
cat /mnt/db/categories/electronics.json         # Read by slug directly

# Index-based navigation (via .by/)
ls /mnt/db/users/.by/                           # List indexed columns
ls /mnt/db/users/.by/email/                     # List distinct emails
ls /mnt/db/products/.by/category/               # List all 10 category slugs
ls /mnt/db/products/.by/category/electronics/   # Products in electronics category

# Composite index navigation
ls /mnt/db/users/.by/last_name.first_name/              # List distinct last names
ls /mnt/db/users/.by/last_name.first_name/Smith/        # First names for last_name='Smith'
ls /mnt/db/users/.by/last_name.first_name/Smith/Alice/  # Users named Alice Smith

# Random sample (orders use UUIDv7 filenames)
ls /mnt/db/orders/.sample/20/                   # 20 random orders

# Process with jq (users use integer IDs)
for id in $(ls /mnt/db/users/.first/20/); do
  cat "/mnt/db/users/$id.json"
done | jq -s 'sort_by(.age)'
```

## Using Claude Code

The demo image includes Claude Code. If you set `ANTHROPIC_API_KEY` before starting, you can use it to explore the mounted filesystem:

```bash
docker-compose exec tigerfs bash
claude
```

## Cleanup

```bash
docker-compose down
```

## Requirements

- Docker with Compose v2
- Privileged container support (for FUSE)
