# ADR-011: Directory Hierarchies in Synthesized Apps

**Status:** Deferred
**Date:** 2026-02-12
**Author:** Mike Freedman

## Context

Users may want to organize synthesized files into subdirectories—for example, grouping blog posts by category (`posts/tutorials/getting-started.md`) or nesting tasks under project folders. The current synth layer presents all files flat within a single view directory.

This ADR evaluates approaches for introducing directory hierarchies into TigerFS and documents the decision to defer implementation.

## Decision

**Not implementing now.** The complexity budget is too high for the value delivered, particularly around `rm -r` cascading across views, multi-view renames, and native table ambiguity.

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

### Approach B: Filename-Based Hierarchy (Synth Only)

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
- Virtual intermediate directories derived from filename prefixes

**Problems:**
- Cross-directory moves (`mv tutorials/getting-started.md general/`) are just filename updates—fine
- But cross-table moves (`mv general/hello.md ../other-notes/`) require data migration, not just a rename
- Scope too narrow for the general case

### Approach C: Slash-in-View-Names (Synth Only)

Each synth directory at any nesting level is a real PostgreSQL view with `/` in its name. Cross-boundary moves become `ALTER VIEW RENAME`.

**Example:**
```sql
CREATE VIEW "notes" ...;                -- /mnt/db/notes/
CREATE VIEW "notes/tutorials" ...;      -- /mnt/db/notes/tutorials/
CREATE VIEW "notes/tutorials/advanced" ...;  -- /mnt/db/notes/tutorials/advanced/
```

**Benefits:**
- Most powerful—each directory can be its own view with independent queries
- Cross-boundary moves are `ALTER VIEW RENAME`
- Best overall design for the general case

**Problems:**
- `.build/` path parsing becomes complex—need to handle nested paths
- Virtual intermediate directories need synthetic inodes (a view `notes/tutorials/advanced` implies `notes/tutorials/` exists even if there's no view for it)
- Cascade renames/deletes across multiple views (renaming `notes/` must rename all `notes/*` views)
- PostgreSQL 63-character identifier limit constrains nesting depth
- Significant code complexity for corner cases

### Approach D: Virtual Directory Registry / Schema-Based Nesting

Use a metadata table or PostgreSQL schemas to track directory structure.

**Not pursued:** Less promising than A-C. Metadata tables contradict ADR-008's "no metadata tables" principle. Schema-based nesting overloads PostgreSQL schema semantics.

## If Revisited

- **Simplest start:** Approach B (filename hierarchy within one synth view). Confined to synth layer, no cross-table moves needed, minimal code changes. Good for the common case where files within one view need folder organization.
- **Full solution:** Approach C (slash-in-view-names for synth only). More powerful but requires careful handling of cascade operations, virtual intermediate directories, and identifier length limits.

## Consequences

### Benefits of Deferring

1. **Reduced complexity:** The synth layer is already substantial; adding hierarchy multiplies edge cases
2. **Focus:** Effort goes to stabilizing the existing synth features (markdown, tasks, format detection)
3. **Learning:** Real usage will clarify whether hierarchy is needed and which approach fits best

### Costs of Deferring

1. **Flat namespace:** Users must use flat directory structures or create separate views per category
2. **Workaround:** Users can approximate hierarchy with filename prefixes (e.g., `tutorials-getting-started.md`) but lose `ls` navigability

## References

- [ADR-008: Synthesized Apps](008-synthesized-apps.md) — Synth view architecture
- PostgreSQL identifier length limit: 63 characters (`NAMEDATALEN - 1`)
