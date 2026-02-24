# Versioned History

## Overview

Captures every UPDATE and DELETE as a timestamped snapshot. History is read-only — you cannot write to `.history/`. Requires the "history" feature on the app.

## Enabling History

At creation:

```bash
Bash "echo 'markdown,history' > mount/.build/notes"
```

Add to an existing app:

```bash
Bash "echo 'history' > mount/.build/notes"
```

How it works: a BEFORE UPDATE/DELETE trigger copies the old row into a `_notes_history` hypertable, keyed by the row's UUID and a version timestamp.

## Browsing by Filename

List files that have history:

```
Glob "mount/notes/.history/*"
```

List past versions of a specific file:

```
Glob "mount/notes/.history/hello.md/*"
```

Returns `.id` (the row UUID) followed by version timestamps, newest first.

Read a past version:

```
Read "mount/notes/.history/hello.md/2026-02-12T013000Z"
```

Returns the full file content (frontmatter + body) as it existed at that point.

## Version IDs

Timestamps are extracted from the row's UUIDv7, formatted as `2006-01-02T150405Z` (filesystem-safe, no colons). Listed newest-first.

## Row UUID (.id file)

```
Read "mount/notes/.history/hello.md/.id"
```

Returns the UUID that tracks this file across renames. If you rename `hello.md` to `intro.md`, the UUID stays the same and all history follows it.

## Browsing by UUID (Cross-Rename Tracking)

List all row UUIDs with history:

```
Glob "mount/notes/.history/.by/*"
```

List all versions for a specific row UUID:

```
Glob "mount/notes/.history/.by/<uuid>/*"
```

Read a past version by UUID:

```
Read "mount/notes/.history/.by/<uuid>/2026-02-12T013000Z"
```

`.by/` is only available at the root `.history/` level, not in subdirectory `.history/` directories.

## Subdirectory History

Each directory has its own `.history/` scoped to files in that directory:

```
Glob "mount/notes/tutorials/.history/*"         # only tutorial files
Glob "mount/notes/tutorials/.history/intro.md/*" # versions of intro.md
```

Subdirectory `.history/` does not include `.by/` (UUID browsing).

## Common Tasks

### "What changed in this file recently?"

1. List versions:
   ```
   Glob "mount/notes/.history/hello.md/*"
   ```
2. Read the most recent version (first timestamp after `.id`):
   ```
   Read "mount/notes/.history/hello.md/2026-02-24T150000Z"
   ```
3. Read the current file:
   ```
   Read "mount/notes/hello.md"
   ```
4. Compare the two — report what was added, removed, or changed.

### "What was different at time X?"

1. List versions and find the one closest to time X:
   ```
   Glob "mount/notes/.history/hello.md/*"
   ```
2. Read that version.
3. Read the current version.
4. Report the differences.

### "Show me the edit history of this file"

1. List all version timestamps:
   ```
   Glob "mount/notes/.history/hello.md/*"
   ```
2. Read each version (newest to oldest).
3. Summarize what changed at each point.

### Recovering a Past Version

Read the old version from `.history/`, then Write it back to the current file to restore it:

```
Read "mount/notes/.history/hello.md/2026-02-12T013000Z"
Write "mount/notes/hello.md" with the content from above
```

## Quick Reference

| Goal | Path |
|------|------|
| List files with history | `Glob "mount/app/.history/*"` |
| List versions of a file | `Glob "mount/app/.history/file.md/*"` |
| Read a past version | `Read "mount/app/.history/file.md/<timestamp>"` |
| Get row UUID | `Read "mount/app/.history/file.md/.id"` |
| List all row UUIDs | `Glob "mount/app/.history/.by/*"` |
| Versions by UUID | `Glob "mount/app/.history/.by/<uuid>/*"` |
| Read version by UUID | `Read "mount/app/.history/.by/<uuid>/<timestamp>"` |
| Subdirectory history | `Glob "mount/app/subdir/.history/*"` |
| Restore old version | Read from `.history/`, Write to current path |
