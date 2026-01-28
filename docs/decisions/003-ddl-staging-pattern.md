# ADR-003: DDL Operations via Staging Pattern

**Date:** 2026-01-27
**Status:** Accepted
**Deciders:** Mike Freedman

## Context

TigerFS enables database manipulation through filesystem operations. While CRUD on rows maps naturally to file operations (read/write/delete), schema changes (DDL) present unique challenges:

| Challenge | Details |
|-----------|---------|
| Destructive potential | `DROP TABLE` can destroy data irreversibly |
| Complex syntax | DDL requires precise SQL syntax |
| Dependencies | Tables have foreign keys, indexes, views |
| No "undo" | Unlike data changes, DDL is not easily reversible |
| Multi-statement | Some changes require multiple coordinated statements |

### Design Goals

1. **Maintain filesystem metaphor**: No special CLI commands; DDL should work through the mounted filesystem like everything else
2. **Unified interface**: Same pattern for all database objects (tables, indexes, schemas, views) and operations (create, modify, delete)
3. **Support both humans and automation**: Interactive workflows with templates and review, plus scriptable single-command execution

The question: How should filesystem operations map to DDL commands while satisfying these goals?

## Options Considered

### Option 1: Direct File Writes

Write DDL directly to a special file to execute immediately.

```bash
echo "CREATE TABLE orders (id serial PRIMARY KEY)" > /mnt/db/.execute
```

**Pros:**
- Minimal ceremony - single command
- Familiar pattern (like `/dev/null`, `/proc` files)

**Cons:**
- No review before execution
- No validation/test capability
- Accidental writes cause immediate damage
- No template guidance for users
- **Partially violates goal #3**: No support for human review workflow

### Option 2: Interactive CLI Commands

Require explicit CLI commands for DDL operations.

```bash
tigerfs create-table orders --columns "id serial, name text"
```

**Pros:**
- Clear separation from filesystem operations
- Can prompt for confirmation

**Cons:**
- **Violates design goal #1**: Breaks the "everything is a file" paradigm
- **Violates design goal #2**: Different interface for DDL vs DML operations
- Harder for scripts and tools that expect filesystem operations

### Option 3: Staging Pattern with Control Files (Chosen)

Stage DDL in memory, validate optionally, then commit explicitly.

```bash
mkdir /mnt/db/.create/orders              # Create staging entry
cat /mnt/db/.create/orders/.sql           # See template
vi /mnt/db/.create/orders/.sql            # Edit DDL
touch /mnt/db/.create/orders/.test        # Validate (optional)
touch /mnt/db/.create/orders/.commit      # Execute
```

**Pros:**
- **Satisfies goal #1**: Pure filesystem operations (`mkdir`, `cat`, `echo >`, `touch`)
- **Satisfies goal #2**: Same `.sql`/`.test`/`.commit`/`.abort` pattern for all objects
- **Satisfies goal #3**: Human workflow (read template → edit → test → commit) and script workflow (write → commit)
- Review before execution (read staged content)
- Test without side effects (BEGIN/ROLLBACK)
- Templates guide correct syntax
- Abort capability (`.abort`)

**Cons:**
- More steps than direct execution
- Requires understanding the staging workflow
- Staged content is per-mount (in-memory, not persisted)

## Decision

Use **Option 3: Staging Pattern with Control Files**.

### Filesystem Structure

| Operation | Path Pattern |
|-----------|--------------|
| Create table | `.create/<name>/.sql`, `.commit`, `.test`, `.abort` |
| Modify table | `<table>/.modify/.sql`, `.commit`, `.test`, `.abort` |
| Delete table | `<table>/.delete/.sql`, `.commit`, `.test`, `.abort` |
| Create index | `<table>/.indexes/.create/<idx>/.sql`, `.test`, `.commit`, `.abort` |
| Delete index | `<table>/.indexes/<idx>/.delete/.sql`, `.test`, `.commit`, `.abort` |
| View index | `<table>/.indexes/<idx>/.schema` (read-only, shows CREATE INDEX DDL) |
| Create schema | `.schemas/.create/<name>/.sql`, `.test`, `.commit`, `.abort` |
| Delete schema | `.schemas/<name>/.delete/.sql`, `.test`, `.commit`, `.abort` |

### Control Files

| File | Read | Write/Touch |
|------|------|-------------|
| `.sql` | Staged DDL, or template if empty | Stage new DDL |
| `.test` | Last test result | Validate via BEGIN/ROLLBACK |
| `.commit` | (empty) | Execute staged DDL |
| `.abort` | (empty) | Clear staged content |

### Template Generation

Templates provide context-aware starting points:

**Create table:**
```sql
CREATE TABLE orders (
    -- add columns here
    -- id SERIAL PRIMARY KEY,
    -- name TEXT NOT NULL,
);
```

**Delete table (with dependencies):**
```sql
-- Table: users
-- Columns: id, name, email
-- Rows: ~42

-- Foreign keys referencing this table:
--   orders.user_id -> users.id (3,847 rows)

-- Uncomment to delete:
-- DROP TABLE users RESTRICT;
-- DROP TABLE users CASCADE;
```

### Workflow Examples

**Human workflow (interactive):**
```bash
mkdir /mnt/db/.create/orders
vi /mnt/db/.create/orders/.sql       # Edit template
touch /mnt/db/.create/orders/.test   # Optional: validate
touch /mnt/db/.create/orders/.commit # Execute
```

**Script workflow (programmatic):**
```bash
mkdir /mnt/db/.create/orders && echo "CREATE TABLE orders (id serial PRIMARY KEY)" > /mnt/db/.create/orders/.sql
touch /mnt/db/.create/orders/.commit
```

## Consequences

### Positive

- **Safety**: Explicit commit required; accidental writes don't execute DDL
- **Validation**: `.test` allows dry-run before committing
- **Discoverability**: Templates show correct syntax and dependencies
- **Abort capability**: Mistakes can be corrected before commit
- **Unified pattern**: Same workflow for all DDL operations
- **Tool-friendly**: Works with editors, scripts, and AI assistants

### Negative

- **More steps**: Creating a table requires 2-3 commands vs 1
- **Learning curve**: Users must understand staging workflow
- **Memory-only**: Staged content lost on unmount (by design - no stale state)

### Neutral

- Aligns with git's staging model (stage → commit)
- Similar to plan/apply patterns in infrastructure tools (Terraform)

