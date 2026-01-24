# Database Filesystem Interface Design

**Date:** 2026-01-23
**Status:** Design Phase

## Overview

A FUSE-based filesystem interface that exposes PostgreSQL database contents as a mountable directory structure. Users interact with database tables, rows, and columns using standard filesystem operations (`ls`, `cat`, `cd`, `grep`, etc.) instead of SQL queries.

## Motivation

**Primary Use Case:** Enable Claude Code and other tools to interact with database-backed data using familiar filesystem tools (Read, Glob, Grep) rather than specialized database interfaces.

**Key Benefits:**
- Natural exploration of database contents using standard Unix tools
- Shareable, centralized data (database) accessible via filesystem paradigm
- Performance hints through filesystem structure (indexed vs non-indexed paths)
- Familiar interface reduces context switching between filesystem and database operations

## Target Platforms

- **Primary:** Developer machines (Linux, macOS, Windows via FUSE/macFUSE/WinFsp)
- **Secondary:** Docker containers with FUSE support, VMs, Firecracker, GitHub Actions Linux runners
- **Out of scope (initially):** Managed container platforms (AWS Fargate, Cloud Run) that restrict FUSE

## Core Architecture

### Technology Stack
- **Implementation Language:** Go
  - Excellent PostgreSQL support (pgx library)
  - Good FUSE libraries (bazil.org/fuse or jacobsa/fuse)
  - Single binary distribution
  - Strong concurrency model for filesystem operations
  - Easy cross-platform compilation

- **Filesystem Layer:** FUSE (Filesystem in Userspace)
  - True filesystem mount accessible to all tools
  - Platform-specific: FUSE (Linux), macFUSE (macOS), WinFsp (Windows)

- **Database:** PostgreSQL
  - Rich schema introspection
  - Index metadata for navigation optimization
  - Full SQL query capabilities

## Filesystem Structure

### Schema-Based Hierarchy

```
/mount/
├── schema_name/
│   ├── table_name/
│   │   ├── .email/                    # Index-based navigation (dotfile)
│   │   │   └── foo@example.com/       # Lookup by indexed column
│   │   ├── .last_name.first_name/     # Composite index
│   │   │   └── Smith/
│   │   │       └── Johnson/
│   │   ├── 123                        # Row-as-file (default: TSV)
│   │   ├── 123.json                   # Row-as-file (JSON format)
│   │   ├── 123.csv                    # Row-as-file (CSV format)
│   │   ├── 123.tsv                    # Row-as-file (TSV format)
│   │   └── 123/                       # Row-as-directory
│   │       ├── email                  # Individual column file
│   │       ├── name                   # Individual column file
│   │       └── age                    # Individual column file
```

### Namespace Conventions

- **Schemas:** Top-level directories under mount point
- **Tables:** Directories under schema
- **Indexes:** Dot-prefixed directories (hidden from default `ls`, visible with `ls -a`)
  - Single column: `.column_name/`
  - Composite: `.column1.column2.column3/`
- **Rows:** Accessed by primary key, multiple representations

## Data Representation

### Dual Representation Model

Every row is accessible in two ways simultaneously:

#### 1. Row-as-File (Full Row Operations)
Complete row data in a single file. Multiple format options:

**TSV (default, headerless):**
```
foo@example.com	Foo Bar	25
```

**CSV (headerless):**
```
foo@example.com,Foo Bar,25
```

**JSON:**
```json
{"email":"foo@example.com","name":"Foo Bar","age":25}
```

**Format Selection:** File extension determines format (`.tsv`, `.csv`, `.json`), default extension uses TSV.

#### 2. Row-as-Directory (Column-Level Operations)
Row as a directory containing individual column files:
```
/schema/users/123/
├── email    (contains: foo@example.com)
├── name     (contains: Foo Bar)
└── age      (contains: 25)
```

### NULL Handling

**TSV/CSV:** Empty fields represent NULL
```
foo@example.com		25    # name is NULL (empty field between tabs)
```

**JSON:** Explicit `null` or omit key
```json
{"email":"foo@example.com","age":25}           # name omitted = NULL
{"email":"foo@example.com","name":null,"age":25}  # explicit null
```

## Read/Write Operations

### Read Operations

**Full row read:**
```bash
cat /schema/users/123.json
cat /schema/users/123          # Default format (TSV)
```

**Column read:**
```bash
cat /schema/users/123/email
```

**Indexed lookup:**
```bash
cat /schema/users/.email/foo@example.com/name
```

### Write Operations

All three formats support partial data with consistent semantics:

**Write Model:**
- TSV/CSV must follow schema column order
- Empty fields = NULL
- Omitted trailing columns = NULL (or column default)
- JSON can omit keys or use explicit `null`
- Columns not provided use NULL or column defaults

**Full row write (INSERT or UPDATE):**
```bash
# TSV with NULLs (empty fields)
echo -e "foo@example.com\t\t25" > /schema/users/123

# JSON
echo '{"email":"foo@example.com","age":25}' > /schema/users/123.json
```

**Column write (UPDATE existing or INSERT new):**
```bash
# Update single column
echo 'newemail@example.com' > /schema/users/123/email

# INSERT via directory creation
mkdir /schema/users/124
echo 'bar@example.com' > /schema/users/124/email
# Other columns use defaults or NULL
```

### Constraint Enforcement

**INSERT semantics:**
- NOT NULL columns without defaults must be provided
- Nullable columns default to NULL
- Columns with defaults use those defaults
- Constraint violations return filesystem errors (EACCES or EIO)

**Example:**
```bash
# Schema: email NOT NULL, name nullable, age default 0

mkdir /schema/users/124
echo 'foo@example.com' > /schema/users/124/email
# ✓ Success: email provided, name=NULL, age=0

mkdir /schema/users/125
echo 'Bar' > /schema/users/125/name
# ✗ FAIL: email is NOT NULL without default
```

## Index-Based Navigation

### Philosophy

Index paths are **syntactic sugar for WHERE clauses**. The filesystem structure provides:
1. **Discovery:** `ls -a` reveals what queries will be fast (indexed)
2. **Convenience:** Navigate via indexed columns
3. **Performance hints:** Presence of index path signals efficient query

PostgreSQL's query planner handles index selection - the filesystem just generates appropriate WHERE clauses.

### Index Path Generation

**Expose only indexed columns as dot-prefixed directories:**
- Single-column index on `email` → `.email/` directory
- Composite index on `(last_name, first_name)` → `.last_name.first_name/` directory

**Composite index prefix matching:**
```bash
# Composite index: (last_name, first_name)
ls /schema/users/.last_name.first_name/Smith/
# WHERE last_name = 'Smith' (prefix query, uses composite index)

ls /schema/users/.last_name.first_name/Smith/Johnson/
# WHERE last_name = 'Smith' AND first_name = 'Johnson' (full composite)
```

**Automatic prefix path exposure:**
If composite index `(last_name, first_name)` exists but no single-column index on `last_name`:
- Both `.last_name/Smith/` and `.last_name.first_name/Smith/` work
- Query planner chooses index
- Filesystem provides multiple intuitive paths to same data

### Non-Indexed Search

**Option 1: Glob patterns with column access**
```bash
grep "25" /schema/users/*/age
```
Filesystem can optimize: detect pattern and translate to `WHERE age = '25'` or `WHERE age::text LIKE '%25%'`

**Option 2: Full table listing**
```bash
ls /schema/users/              # List all primary keys
cat /schema/users/*.json       # Read all rows
```
Use standard tools for client-side filtering.

**Strategy:** Support both. Optimize common patterns (Option 1) when detectable, fallback to full scan otherwise.

## Performance Considerations

### Caching Strategy (To Be Determined)

**Key questions:**
- Is real-time consistency required, or is eventual consistency acceptable?
- What are typical Claude Code usage patterns?
- How large are the tables?

**Options under consideration:**
- No caching (always live)
- TTL-based caching (5-10 second TTL)
- Smart caching with PostgreSQL NOTIFY/LISTEN
- Read-through cache with background refresh

**Deferred decision:** Start simple (likely TTL-based for directory listings), measure, and optimize as needed.

### Query Optimization

**Intelligent WHERE clause generation:**
- Detect `grep` patterns and translate to SQL WHERE clauses
- Leverage PostgreSQL query planner for index selection
- Only indexed columns exposed as navigable paths (performance signal to users)

## Design Principles

### YAGNI (You Aren't Gonna Need It)
- Start with read operations, add write support incrementally
- Simple caching initially, optimize based on real usage
- Support common formats (TSV/CSV/JSON), not every possible serialization
- Index-based navigation only, no complex query DSL

### Unix Philosophy
- Filesystem as universal interface
- Standard tools work without modification
- Dot-prefix convention for metadata/indexes (hidden by default)
- Simple, composable operations

### Database-First
- Leverage PostgreSQL's query planner, don't reinvent optimization
- Respect database constraints (NOT NULL, defaults, etc.)
- Index structure drives filesystem structure
- WHERE clause generation, not custom query language

## Future Considerations

**Write support phases:**
1. Read-only (v1)
2. UPDATE via column writes
3. INSERT via directory/file creation
4. DELETE via `rm`

**Additional features (if needed):**
- Custom view hierarchies (user-defined navigation patterns)
- Full-text search via `.search/` directories
- Transaction support (multi-file atomic writes?)
- Connection pooling and multi-database support
- Alternative interfaces for FUSE-restricted environments (future, if needed)

## Open Questions

1. **Caching:** What level of consistency is required? Real-time vs eventual?
2. **Error handling:** How should SQL errors map to filesystem errors?
3. **Large tables:** How to handle tables with millions of rows in `ls` operations?
4. **Transactions:** Should multi-file operations be atomic?
5. **Authentication:** How to handle database credentials and connection management?
6. **Multi-database:** Should one mount support multiple PostgreSQL databases?

## Next Steps

1. Finalize caching strategy (discuss consistency requirements)
2. Design error handling approach
3. Prototype basic FUSE + PostgreSQL integration in Go
4. Test with Claude Code's Read/Glob/Grep tools
5. Iterate on performance and usability
