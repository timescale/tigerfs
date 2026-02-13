# ADR-011: Directory Hierarchies in Synthesized Apps

**Status:** Accepted
**Date:** 2026-02-12
**Author:** Mike Freedman

## Context

Users may want to organize synthesized files into subdirectories—for example, grouping blog posts by category (`posts/tutorials/getting-started.md`) or nesting tasks under project folders. The current synth layer presents all files flat within a single view directory.

This ADR evaluates approaches for introducing directory hierarchies into TigerFS.

## Decision

**Approach B (filename-based hierarchy) with a `filetype` column.** Slashes in the filename column create directory structure within a single view. Every directory has an explicit row.

### Database Model

Generated tables include a `filetype` column:

```sql
filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory'))
UNIQUE(filename, filetype)
```

The compound UNIQUE constraint allows a file and directory with the same base name to coexist.

**Example data:**
```
_memory:
filename           | filetype  | body
projects           | directory | NULL
projects/web       | directory | NULL
projects/web/todo  | file      | "Fix CSS..."
projects/web/notes | file      | "Meeting..."
readme             | file      | "Welcome..."
```

**Filesystem view:**
```
mount/memory/
├── projects/
│   └── web/
│       ├── todo.md
│       └── notes.md
└── readme.md
```

### Key Design Points

- **Format-agnostic** — works for markdown, plaintext, and future formats. `SupportsHierarchy` is keyed on `filetype` column presence, not format type.
- **Auto-create parents on file write** — writing `projects/web/todo` auto-INSERTs directory rows for `projects` and `projects/web` via `ON CONFLICT DO NOTHING`.
- **Atomic directory rename** — single SQL UPDATE with prefix swap renames a directory and all descendants.
- **No migration** — new apps via `.build/` get the `filetype` column; existing apps stay flat and work unchanged (`SupportsHierarchy=false`).
- **Both NFS and FUSE** — all hierarchy logic lives in the shared `fs.Operations` layer. No backend-specific code needed.
- **`resolveSynthHierarchy`** — after `parsePath`, converts multi-segment `PathColumn` to `PathRow` with the full subpath as `PrimaryKey`. All existing synth hooks at `PathRow` then handle the rest.
- **Cross-table moves blocked** — enforced at the write layer (unchanged).

## Approaches Evaluated

### Approach A: Slash-in-Table-Names

Use `/` in PostgreSQL table names (which PostgreSQL allows when quoted), with longest-prefix matching to resolve paths.

**Example:**
```
/mnt/db/public/projects/          → table "projects"
/mnt/db/public/projects/web/      → table "projects/web"
```

**Problems:**
- Fundamental ambiguity between rows and subdirectories for native tables—is `/projects/web/` a row named `web` or a subtable?
- `mkdir` semantic collision—does it create a table or something else?
- Requires quoting everywhere in SQL tools

### Approach B: Filename-Based Hierarchy (Synth Only) — CHOSEN

Use `/` in the synth filename column to create virtual directory structure within a single view.

**Example:**
```sql
-- Single view, filenames contain paths
INSERT INTO notes (filename, body) VALUES ('tutorials/getting-started', '...');
INSERT INTO notes (filename, body) VALUES ('tutorials/advanced', '...');
INSERT INTO notes (filename, body) VALUES ('general/hello', '...');
```

```
/mnt/db/notes/
├── tutorials/
│   ├── getting-started.md
│   └── advanced.md
└── general/
    └── hello.md
```

**Benefits:**
- Cleanest design—hierarchy is just a filename convention
- Confined to synth layer, no changes to native tables
- Explicit directory rows enable `mkdir`/`rmdir` semantics
- Auto-created parent directories simplify file creation
- Atomic prefix rename for directory moves

### Approach C: Slash-in-View-Names (Synth Only)

Each synth directory at any nesting level is a real PostgreSQL view with `/` in its name.

**Problems:**
- `.build/` path parsing becomes complex
- Virtual intermediate directories need synthetic inodes
- Cascade renames/deletes across multiple views
- PostgreSQL 63-character identifier limit constrains nesting depth
- Significant code complexity for corner cases

### Approach D: Virtual Directory Registry / Schema-Based Nesting

**Not pursued:** Metadata tables contradict ADR-008's "no metadata tables" principle. Schema-based nesting overloads PostgreSQL schema semantics.

## Consequences

### Benefits

1. **Natural organization** — users can organize files into subdirectories using standard Unix tools (`mkdir`, `mv`, `rm -rf`)
2. **Backward compatible** — existing apps without the `filetype` column continue to work flat
3. **Minimal core changes** — all hierarchy logic confined to `synth_ops.go`; core files get only pure dispatch hooks
4. **Format independence** — any synth format gets hierarchy automatically if its `.build/` generates the `filetype` column

### Limitations

1. **No cross-table moves** — moving files between views requires data migration (blocked by design)
2. **No depth limit enforcement** — deeply nested paths allowed; practical depth limited by filesystem path length

## References

- [ADR-008: Synthesized Apps](008-synthesized-apps.md) — Synth view architecture
- PostgreSQL identifier length limit: 63 characters (`NAMEDATALEN - 1`)
- Implementation: Phase 6.3 in `docs/implementation/implementation-tasks.md`
