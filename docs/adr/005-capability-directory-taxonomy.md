# ADR-005: Capability-based Directory Taxonomy

**Date:** 2026-01-28
**Status:** Accepted
**Deciders:** Mike Freedman

## Context

TigerFS uses dotfiles (hidden files/directories) for special operations. As features have grown, the table-level listing has become confusing:

```
/mnt/db/products/
├── .all              # capability: all rows iterator
├── .category         # index navigation (by column name)
├── .columns          # metadata: column info
├── .count            # metadata: row count
├── .ddl              # metadata: table DDL
├── .delete           # capability: bulk delete staging
├── .first            # capability: first N rows
├── .indexes          # DDL: index management
├── .last             # capability: last N rows
├── .modify           # capability: bulk modify staging
├── .name             # index navigation (by column name)
├── .sample           # capability: random sample
├── .schema           # metadata: schema info
└── 1/                # row by PK
```

The problem: `.category` and `.name` are dynamically named based on indexed columns, while `.first`, `.indexes`, `.count` are fixed names. Users cannot distinguish:
- Metadata files from navigation directories
- Reserved names from dynamic column-based names
- What operations are available vs what data exists

Additionally, AI agents (like Claude Code) need to discover available operations programmatically. With all dotfiles at the same level using the same prefix, agents must know the full list of reserved names or read documentation.

### Design Goals

1. **Clear taxonomy**: Users should immediately understand what each dotfile does
2. **No collisions**: Dynamic names (columns) should not conflict with reserved names
3. **Discoverable**: `ls -la` should reveal available capabilities; agents should enumerate via glob patterns
4. **Composable**: Capabilities should work together (e.g., pagination + index lookup)

## Options Considered

### Option 1: Nest index navigation under `.indexes/`

Move all index-related navigation inside `.indexes/`:

```
.indexes/
├── .create/           # DDL staging
├── category/          # Index navigation
└── name/
```

**Pros:**
- Clean separation
- `.indexes/` becomes one-stop for index operations

**Cons:**
- Longer paths: `.indexes/category/Electronics/` vs `.category/Electronics/`
- Mixes DDL (create) with navigation (lookup)

### Option 2: Different prefix for index navigation

Use `@column/` or `~column/` for index navigation:

```
@category/Electronics/
@name/Widget/
.count
.ddl
```

**Pros:**
- Short paths
- Visually distinct

**Cons:**
- `@` requires escaping in some shells
- Non-standard convention

### Option 3: Capability-based organization with `.info/` for metadata (Chosen)

Organize by **what you can do** (capabilities), with metadata grouped under `.info/`:

```
.info/                # metadata: all table information
├── count
├── ddl
├── schema
└── columns
.by/                  # capability: lookup by indexed column
├── category/
└── name/
.first/               # capability: first N rows
.last/                # capability: last N rows
.indexes/             # capability: index management (also lists indexes)
```

**Pros:**
- Clear taxonomy: metadata (`.info/` directory) vs capabilities (other `.name/` directories)
- Agent-discoverable: `ls .info/` enumerates all metadata
- No collisions: `.by/category/` cannot conflict with reserved names
- Extensible: easy to add `.search/`, `.filter/`, `.bulk/`

**Cons:**
- Longer paths for both metadata (`cat .info/count`) and index lookup (`.by/category/`)
- Requires migration from current structure

### Option 4: Different prefix for metadata

Use `._count`, `._ddl` for metadata:

**Rejected:** Underscore prefix collides with macOS AppleDouble files (`._*`), and is visually awkward.

### Option 5: `.meta.` prefix for metadata

Use `.meta.count`, `.meta.ddl` prefix pattern:

**Rejected:** More awkward than subdirectory, and `.info/` reads more naturally.

## Decision

Use **Option 3: Capability-based organization with `.info/` subdirectory**.

### Taxonomy

| Type | Location | Naming | Examples |
|------|----------|--------|----------|
| **Metadata** | `.info/` subdirectory | Nouns (what it IS) | `.info/count`, `.info/ddl`, `.info/schema`, `.info/columns` |
| **Capabilities** | Top-level dotfiles | Verbs/prepositions (what you DO) | `.by/`, `.columns/`, `.first/`, `.last/`, `.sample/`, `.order/`, `.export/`, `.import/`, `.indexes/`, `.all/`, `.delete/`, `.modify/` |

Key distinctions:
- **Metadata** (`.info/`): Read-only files describing the table
- **Capabilities**: Directories enabling actions (may also list options when browsed)

Note: Capability directories can be self-describing. For example, `ls .by/` shows available indexed columns, and `ls .indexes/` shows existing indexes. The distinction is that `.info/` contains pure data files, while capabilities enable operations.

**Name reuse:** The metadata file `.info/columns` (column definitions) and the capability directory `.columns/` (column projection) share the word "columns" but serve different purposes. `.info/columns` describes the table schema; `.columns/col1,col2/` selects which columns appear in query output. The disambiguation is structural: metadata lives under `.info/`, capabilities live at the top level.

### Why `.info/`?

Considered alternatives:

| Name | Assessment |
|------|------------|
| `.meta/` | Technical jargon |
| `.info/` | Simple, clear, universally understood |
| `.about/` | Natural but slightly longer |
| `.desc/` | Ambiguous with "descending" |
| `.stats/` | Not all content is statistics |

`.info/` was chosen for clarity and simplicity: "What's the info about this table?" → `ls .info/`

### New Structure

```
/mnt/db/products/
├── .info/            # metadata: table information
│   ├── count         # row count
│   ├── ddl           # table DDL
│   ├── schema        # schema info
│   └── columns       # column definitions
├── .by/              # capability: lookup by indexed column
│   ├── category/
│   │   └── Electronics/
│   └── name/
│       └── Widget/
├── .columns/         # capability: column projection (pipeline query)
│   └── id,name,price/
│       └── .export/
├── .first/           # capability: first N rows
│   └── 100/
├── .last/            # capability: last N rows
│   └── 100/
├── .sample/          # capability: random sample
│   └── 100/
├── .order/           # capability: specify row ordering (see ADR-006)
│   └── price/        # (dynamic: any column name)
├── .export/          # capability: bulk read (see ADR-006)
│   ├── csv
│   ├── tsv
│   ├── json
│   └── yaml
├── .import/          # capability: bulk write (see ADR-006)
│   ├── .overwrite/
│   │   └── csv, tsv, json, yaml
│   ├── .sync/
│   │   └── csv, tsv, json, yaml
│   └── .append/
│       └── csv, tsv, json, yaml
├── .all/             # capability: all rows iterator
├── .indexes/         # capability: index DDL management
│   └── .create/
├── .delete/          # capability: table delete staging
├── .modify/          # capability: table modify staging
└── 1/                # row by PK
```

### Agent Discovery

With this structure, AI agents can discover capabilities programmatically:

```bash
# Enumerate all metadata
ls .info/

# Enumerate capabilities (all top-level dot-directories except .info)
ls -d .*/ | grep -v '^\.info/$'

# Check specific metadata
cat .info/count
cat .info/columns

# Discover what you can look up by
ls .by/
```

### Migration

| Before | After |
|--------|-------|
| `.category/Electronics/` | `.by/category/Electronics/` |
| `.name/Widget/` | `.by/name/Widget/` |
| `.count` | `.info/count` |
| `.ddl` | `.info/ddl` |
| `.schema` | `.info/schema` |
| `.columns` | `.info/columns` |
| `.first/`, `.last/`, `.sample/` | Unchanged |
| `.indexes/`, `.delete/`, `.modify/` | Unchanged |

### Why `.by/`?

Considered alternatives:

| Name | Example | Assessment |
|------|---------|------------|
| `.lookup/` | `.lookup/category/` | Explicit but longer |
| `.by/` | `.by/category/` | Short, natural ("by category") |
| `.find/` | `.find/category/` | Action-oriented |
| `.via/` | `.via/category/` | Slightly formal |
| `.where/` | `.where/category/` | SQL-like, may over-promise |

`.by/` reads naturally: "products by category" → `products/.by/category/`

## Consequences

### Positive

- **Clear taxonomy**: Metadata (`.info/`) vs capabilities (other dot-directories)
- **Agent-discoverable**: `ls .info/` enumerates all metadata programmatically
- **No name collisions**: Column names like `count` or `first` won't conflict
- **Discoverable**: `ls -la` shows available capabilities clearly
- **Extensible**: Easy to add `.search/`, `.filter/`, `.bulk/` etc.
- **Composable**: `.by/category/Electronics/.first/10/` works naturally

### Negative

- **Longer paths**: Extra directory level for both metadata and index lookups
- **Breaking change**: Existing scripts using `.column/` or `.count` paths need updates
- **Learning curve**: Users must learn new paths

### Neutral

- Consistent with existing `.first/N/`, `.last/N/` pattern
- Capability directories can still provide information when listed (self-describing)
