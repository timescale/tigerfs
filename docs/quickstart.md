# TigerFS Quickstart

This guide demonstrates common TigerFS workflows using real-world scenarios.

## Prerequisites

- TigerFS installed (`go install github.com/timescale/tigerfs/cmd/tigerfs@latest`)
- PostgreSQL database with data
- FUSE support (macFUSE on macOS, native on Linux)

---

## Example 1: Using the Docker Demo (Any Platform)

**Scenario:** You want to try TigerFS without setting up a local database.

The Docker demo runs both TigerFS and PostgreSQL in containers with sample data (1,000 users, 200 products, 8,000 orders).

### Commands

```bash
# From the TigerFS repository root
cd examples/docker-demo

# Start the demo (builds containers, starts PostgreSQL, mounts TigerFS)
./demo.sh start

# Enter the container (starts in /mnt/db)
./demo.sh shell

# Explore!
ls
cat users/1.json
cat products/1.json
ls orders/.first/10/
```

### Expected Output

```
$ ls
categories/  orders/  products/  users/

$ cat users/1.json
{"id":1,"name":"Alice Smith","first_name":"Alice","last_name":"Smith",...}
```

### Explanation

The `demo.sh` script handles everything: building containers, waiting for PostgreSQL to be ready, and mounting TigerFS automatically. When you run `./demo.sh shell`, you're placed directly in `/mnt/db` where the database is mounted.

### Other Commands

```bash
./demo.sh status   # Check if running
./demo.sh restart  # Stop and start again
./demo.sh stop     # Unmount and stop containers
```

---

## Example 1b: Using the macOS Demo (Native)

**Scenario:** You're on macOS and want TigerFS to run natively (not in Docker) for better performance and integration.

The macOS demo runs TigerFS natively using the NFS backend, with only PostgreSQL in Docker.

### Prerequisites

- macOS (Apple Silicon or Intel)
- Docker Desktop
- Go 1.21+ (for building TigerFS)

### Commands

```bash
# From the TigerFS repository root
cd examples/mac-demo

# Start the demo (starts PostgreSQL, builds TigerFS, mounts at /tmp/tigerfs-demo)
./demo.sh start

# Explore directly from your Mac terminal
ls /tmp/tigerfs-demo
cat /tmp/tigerfs-demo/users/1.json
cat /tmp/tigerfs-demo/products/1.json
```

### Expected Output

```
$ ls /tmp/tigerfs-demo
categories/  orders/  products/  users/

$ cat /tmp/tigerfs-demo/users/1.json
{"id":1,"name":"Alice Smith","first_name":"Alice","last_name":"Smith",...}
```

### Explanation

Unlike the Docker demo, TigerFS runs as a native macOS process. This means:
- The mount appears at `/tmp/tigerfs-demo` on your Mac (not inside a container)
- You can use any Mac tools directly (Finder, VS Code, your favorite editor)

### Cleanup

```bash
./demo.sh stop
```

---

## Example 2: Connecting to a Local PostgreSQL Database

**Scenario:** You have PostgreSQL running directly on your machine (not in Docker) and want to explore it.

### Commands

```bash
# Create a mount point
mkdir -p /mnt/db

# Mount with a connection string
tigerfs mount postgres://user:password@localhost:5432/mydb /mnt/db &

# Or mount using environment variables
export PGHOST=localhost
export PGPORT=5432
export PGUSER=user
export PGPASSWORD=password
export PGDATABASE=mydb
tigerfs mount /mnt/db &

# Verify the mount
ls /mnt/db
```

### Expected Output

```
your_table1/
your_table2/
...
```

### Explanation

TigerFS connects to the PostgreSQL database and presents each table in the default schema (typically `public`) as a directory. The mount runs in the background (`&`) so you can continue using the terminal.

The mount point `/mnt/db` now acts as a window into your database - each subdirectory is a table, and you can navigate into them to access rows and columns.

### Unmounting

```bash
# When finished
tigerfs unmount /mnt/db
```

---

## Example 3: Exploring Database Schema with ls and cat

**Scenario:** You've connected to an unfamiliar database and want to understand its structure without writing SQL.

### Commands

```bash
# List all tables
ls /mnt/db

# See hidden metadata files
ls -la /mnt/db/users

# View table schema (CREATE TABLE statement)
cat /mnt/db/users/.schema

# List columns
cat /mnt/db/users/.columns

# Check row count
cat /mnt/db/users/.count

# View indexes
cat /mnt/db/users/.indexes
```

### Expected Output

```bash
$ ls /mnt/db
orders/  products/  users/

$ ls -la /mnt/db/users
total 0
drwxr-xr-x  1 root root    0 Jan 26 12:00 .
drwxr-xr-x  1 root root    0 Jan 26 12:00 ..
-r--r--r--  1 root root  512 Jan 26 12:00 .columns
-r--r--r--  1 root root    4 Jan 26 12:00 .count
drwxr-xr-x  1 root root    0 Jan 26 12:00 .email
drwxr-xr-x  1 root root    0 Jan 26 12:00 .first
-r--r--r--  1 root root 1024 Jan 26 12:00 .indexes
drwxr-xr-x  1 root root    0 Jan 26 12:00 .sample
-r--r--r--  1 root root  256 Jan 26 12:00 .schema
-rw-r--r--  1 root root   64 Jan 26 12:00 1
-rw-r--r--  1 root root   64 Jan 26 12:00 2
...

$ cat /mnt/db/users/.schema
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INTEGER,
    active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

$ cat /mnt/db/users/.columns
id
name
email
age
active
bio
created_at

$ cat /mnt/db/users/.count
1000

$ cat /mnt/db/users/.indexes
PRIMARY KEY: id
UNIQUE: email
```

### Explanation

TigerFS exposes database metadata as dotfiles (hidden files starting with `.`):

- **`.schema`** - The complete CREATE TABLE statement
- **`.columns`** - Simple list of column names in schema order
- **`.count`** - Total number of rows (runs `SELECT COUNT(*)`)
- **`.indexes`** - Available indexes for efficient lookups

Dotfiles are hidden by default with `ls` but visible with `ls -a` or `ls -la`. This keeps the view clean while making metadata accessible.

---

## Example 4: Reading Data in Different Formats

**Scenario:** You need to read row data and want to choose the most convenient format for your use case.

### Commands

```bash
# Read a row as TSV (default, no extension)
cat /mnt/db/users/1

# Read a row as TSV (explicit extension)
cat /mnt/db/users/1.tsv

# Read a row as JSON
cat /mnt/db/users/1.json

# Read a row as CSV
cat /mnt/db/users/1.csv

# Read individual columns (row-as-directory)
cat /mnt/db/users/1/email
cat /mnt/db/users/1/name
cat /mnt/db/users/1/age
```

### Expected Output

```bash
$ cat /mnt/db/users/1
1	Alice Johnson	alice@example.com	28	true	Software engineer who loves hiking.	2024-01-15T10:30:00Z

$ cat /mnt/db/users/1.json
{"id":1,"name":"Alice Johnson","email":"alice@example.com","age":28,"active":true,"bio":"Software engineer who loves hiking.","created_at":"2024-01-15T10:30:00Z"}

$ cat /mnt/db/users/1.csv
1,Alice Johnson,alice@example.com,28,true,Software engineer who loves hiking.,2024-01-15T10:30:00Z

$ cat /mnt/db/users/1/email
alice@example.com

$ cat /mnt/db/users/1/name
Alice Johnson

$ cat /mnt/db/users/1/age
28
```

### Explanation

TigerFS supports three data formats, selected by file extension:

| Extension | Format | Use Case |
|-----------|--------|----------|
| (none) or `.tsv` | Tab-separated values | Shell pipelines, `cut`, `awk` |
| `.json` | JSON object | APIs, `jq`, programming languages |
| `.csv` | Comma-separated values | Spreadsheets, data tools |

**Row-as-file** (`/users/1.json`) returns the entire row in one read operation - efficient for complete row access.

**Row-as-directory** (`/users/1/email`) treats the row as a directory where each column is a separate file - useful for reading or updating individual fields.

Both representations exist simultaneously for every row.

---

## Example 5: Basic Data Modification

**Scenario:** You need to update a user's email address and insert a new product.

### Commands

```bash
# Update a single column
echo "newemail@example.com" > /mnt/db/users/1/email

# Verify the update
cat /mnt/db/users/1/email

# Update specific columns via JSON (PATCH semantics - only specified keys updated)
echo '{"name":"Alice Smith","email":"alice.smith@example.com"}' > /mnt/db/users/1.json

# Update specific columns via TSV (PATCH semantics - header specifies columns)
echo -e 'name\temail\nAlice Smith\talice.smith@example.com' > /mnt/db/users/1.tsv

# Insert a new row (use next available ID or specify one)
echo '{"name":"New Product","price":29.99,"in_stock":true,"category":"gadgets"}' > /mnt/db/products/new.json

# Delete a row
rm /mnt/db/users/999
```

### Expected Output

```bash
$ echo "newemail@example.com" > /mnt/db/users/1/email
$ cat /mnt/db/users/1/email
newemail@example.com

$ echo '{"name":"Alice Smith","email":"alice.smith@example.com"}' > /mnt/db/users/1.json
$ cat /mnt/db/users/1.json
{"id":1,"name":"Alice Smith","email":"alice.smith@example.com","age":28,"active":true,"bio":"Software engineer who loves hiking.","created_at":"2024-01-15T10:30:00Z"}
```

### Explanation

TigerFS translates filesystem operations to SQL:

| Operation | SQL Generated |
|-----------|---------------|
| `echo "x" > /users/1/email` | `UPDATE users SET email = 'x' WHERE id = 1` |
| `echo '{...}' > /users/1.json` | `UPDATE users SET name=..., email=... WHERE id = 1` |
| `echo -e 'col\nval' > /users/1.tsv` | `UPDATE users SET col = 'val' WHERE id = 1` |
| `echo '{...}' > /products/new.json` | `INSERT INTO products (...) VALUES (...)` |
| `rm /users/999` | `DELETE FROM users WHERE id = 999` |

**PATCH Semantics:** All format extensions (`.json`, `.yaml`, `.csv`, `.tsv`) use PATCH semantics - only the columns you specify are updated. Columns not included retain their existing values.

**Important notes:**
- Updates respect database constraints (NOT NULL, UNIQUE, foreign keys)
- Writes fail with appropriate errors if constraints are violated
- TSV/CSV writes require a header row specifying column names
- Delete operations are permanent (no trash/undo)

---

## Example 6: Using grep and awk to Query Data

**Scenario:** You want to find specific records and analyze data using familiar Unix tools.

### Commands

```bash
# Find users with "alice" in their email
grep -r "alice" /mnt/db/users/*/email

# List all active users (TSV format, 5th column is 'active')
for id in $(ls /mnt/db/users | head -100); do
  row=$(cat /mnt/db/users/$id)
  active=$(echo "$row" | cut -f5)
  if [ "$active" = "true" ]; then
    echo "$row"
  fi
done

# Calculate average age using awk
for id in $(ls /mnt/db/users | head -100); do
  cat /mnt/db/users/$id/age
done | awk '{sum+=$1; count++} END {print "Average age:", sum/count}'

# Find products over $50 using jq
for id in $(ls /mnt/db/products); do
  cat /mnt/db/products/$id.json
done | jq -s '[.[] | select(.price > 50)] | length'

# Sort users by age using jq
for i in $(ls /mnt/db/users | head -20); do
  cat /mnt/db/users/$i.json
done | jq -s 'sort_by(.age)'

# Find orders with specific status
for id in $(ls /mnt/db/orders | head -1000); do
  status=$(cat /mnt/db/orders/$id/status)
  if [ "$status" = "pending" ]; then
    echo "Order $id is pending"
  fi
done
```

### Expected Output

```bash
$ grep -r "alice" /mnt/db/users/*/email
/mnt/db/users/1/email:alice@example.com
/mnt/db/users/47/email:alice.jones@company.com

$ for id in $(ls /mnt/db/users | head -100); do cat /mnt/db/users/$id/age; done | awk '{sum+=$1; count++} END {print "Average age:", sum/count}'
Average age: 34.7

$ for id in $(ls /mnt/db/products); do cat /mnt/db/products/$id.json; done | jq -s '[.[] | select(.price > 50)] | length'
47
```

### Explanation

TigerFS enables database queries using standard Unix tools:

- **grep** - Search for patterns across rows
- **awk** - Process columnar data, calculate aggregates
- **cut** - Extract specific columns from TSV/CSV
- **jq** - Filter, transform, and analyze JSON data
- **sort** - Order results
- **uniq** - Find distinct values
- **wc** - Count matching rows

**Performance tips:**
- Use `.first/N/` or `.sample/N/` paths to limit rows for exploration
- Use index paths (`.email/value`) for indexed lookups instead of scanning
- Combine with `head` to limit iterations

---

## Example 7: Mounting a Tiger Cloud Service

**Scenario:** You have a database running on Tiger Cloud and want to mount it without managing connection strings.

### Prerequisites

1. Tiger CLI installed: `curl -fsSL https://cli.tigerdata.com | sh`
2. Tiger Cloud credentials from Console -> Settings -> Create credentials

### Commands

**Desktop (Browser-based authentication):**
```bash
# Authenticate via browser
tiger auth login

# Mount using your service ID
tigerfs mount --tiger-service-id=<your-service-id> /mnt/cloud &

# Explore your Tiger Cloud database
ls /mnt/cloud
```

**Headless/Docker (Client credentials):**
```bash
# Set credentials (from Tiger Cloud Console)
export TIGER_PUBLIC_KEY=<your-public-key>
export TIGER_SECRET_KEY=<your-secret-key>
export TIGER_PROJECT_ID=<your-project-id>

# Authenticate using environment variables
tiger auth login

# Set service ID and mount
export TIGER_SERVICE_ID=<your-service-id>
tigerfs mount /mnt/cloud &

# Explore your data
ls /mnt/cloud
cat /mnt/cloud/users/1.json
```

### Expected Output

```bash
$ tiger auth login
Authenticated successfully

$ tigerfs mount --tiger-service-id=e6ue9697jf /mnt/cloud &
Filesystem mounted at /mnt/cloud

$ ls /mnt/cloud
analytics/  customers/  events/  orders/
```

### Explanation

TigerFS integrates with Tiger Cloud through the Tiger CLI:

1. **Authentication** - `tiger auth login` handles OAuth (desktop) or client credentials (headless)
2. **Connection retrieval** - TigerFS calls `tiger db connection-string --with-password --service-id=<id>`
3. **Mounting** - Uses the retrieved connection string to mount the database

**Configuration options:**
- `--tiger-service-id=<id>` flag on command line
- `TIGER_SERVICE_ID` environment variable
- `tiger_service_id` in config file

This eliminates the need to manage PostgreSQL connection strings manually - Tiger CLI handles secure credential storage and retrieval.

---

## Quick Reference

### Mount Commands

```bash
# Basic mount
tigerfs mount postgres://user:pass@host:5432/db /mnt/db &

# Using environment variables
tigerfs mount /mnt/db &

# Tiger Cloud
tigerfs mount --tiger-service-id=<id> /mnt/db &

# With debug logging
tigerfs mount --debug postgres://... /mnt/db &

# Unmount
tigerfs unmount /mnt/db
```

### Path Patterns

| Path | Description |
|------|-------------|
| `/mnt/db/` | List all tables |
| `/mnt/db/users/` | List rows in table |
| `/mnt/db/users/123` | Row as TSV |
| `/mnt/db/users/123.json` | Row as JSON |
| `/mnt/db/users/123/email` | Single column |
| `/mnt/db/users/.schema` | Table DDL |
| `/mnt/db/users/.count` | Row count |
| `/mnt/db/users/.first/100/` | First 100 rows |
| `/mnt/db/users/.sample/50/` | Random 50 rows |
| `/mnt/db/users/.email/foo@x.com/` | Index lookup |

### Format Selection

| Extension | Format | Best For |
|-----------|--------|----------|
| (none) | TSV | Shell scripts, `awk`, `cut` |
| `.tsv` | TSV | Explicit TSV |
| `.csv` | CSV | Spreadsheets, pandas |
| `.json` | JSON | APIs, `jq`, web apps |
