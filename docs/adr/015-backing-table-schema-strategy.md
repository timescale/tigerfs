# ADR-015: Backing Table Schema Strategy for File-First Apps

**Status:** Accepted
**Date:** 2026-03-26
**Author:** Mike Freedman

## Context

File-first (synth) apps require backing tables, triggers, functions, and history tables. Previously these lived in the user's schema with an underscore-prefixed name (e.g., `public._blog` for the `public.blog` view). This was fragile -- user tables starting with `_` got misidentified as backing tables.

We moved all backing infrastructure to a dedicated `tigerfs` schema (`tigerfs.blog` table + `public.blog` view). This works well for the default/public schema, but raises a question: what happens when a user creates a file-first app in a non-public schema via `/.schemas/myschema/.build/blog`?

The backing table would be `tigerfs.blog`, which collides with any default-schema app of the same name. We need a strategy for namespacing backing tables when multiple user schemas have file-first apps.

## Options Considered

### Option 1: Only support file-first in the default schema

Restrict `.build/` to the root level (default schema only). Remove `.build` from `/.schemas/<schema>/` paths. The `tigerfs` schema holds all backing tables, and since only one user schema can create apps, there are no collisions.

**Pros:**
- Simplest implementation -- no namespacing needed
- Covers the primary use case (most users work in one schema)
- `.tables/` directory is flat and straightforward

**Cons:**
- Users with multi-schema databases cannot use file-first mode in non-default schemas

### Option 2: Per-user-schema backing schemas

Create a separate backing schema for each user schema that has file-first apps. The default schema uses `tigerfs`, non-default schemas use `tigerfs_{schemaname}` (e.g., `tigerfs_myschema`).

- `public.blog` (view) backed by `tigerfs.blog` (table)
- `myschema.blog` (view) backed by `tigerfs_myschema.blog` (table)

Access paths:
- `/.tables/blog` for default schema
- `/.schemas/myschema/.tables/blog` for non-default schemas

**Pros:**
- Clean table names, no collision possible
- Schemas are PostgreSQL's native namespacing mechanism
- Each backing schema is isolated -- `DROP SCHEMA tigerfs_myschema CASCADE` cleanly removes one schema's file-first infrastructure
- `/.schemas/myschema/.tables/` queries a single schema -- no extra filtering

**Cons:**
- Schema proliferation (one backing schema per user schema with file-first apps)
- More `CREATE SCHEMA` statements
- Migration logic must handle per-schema backing schemas

### Option 3: Namespaced table names in one schema

Keep a single `tigerfs` schema but encode the user schema in the table name using a separator (e.g., `tigerfs.myschema_blog` or `tigerfs.myschema__blog`).

**Pros:**
- Single backing schema, simpler to manage
- All backing tables visible in one place

**Cons:**
- No safe separator exists -- PostgreSQL identifiers can contain any character when quoted, so `_`, `__`, or any other delimiter could appear in legitimate table names, causing collisions
- `.tables/` directory needs filtering/parsing to show the right tables for each schema
- Table names become less readable

## Decision

**Option 1 for now.** File-first apps are restricted to the default schema. `.build/` is only available at the root level and `/.schemas/<schema>/.build/` is removed. This is correct, simple, and covers the current use case.

**Option 2 is the recommended path if multi-schema file-first support is needed in the future.** It uses PostgreSQL's native namespacing, avoids collision risks, and integrates cleanly with the existing `/.schemas/<schema>/.tables/` path structure.
