---
name: tigerfs-memory
description: Use TigerFS as persistent agent memory — store facts, decisions, session context, and coordination state across sessions.
---

# TigerFS as Agent Memory

Use a TigerFS mount as persistent storage that survives across sessions. Store knowledge, resume context, and coordinate with other agents — all through file operations.

## When to Use

- You need to **persist knowledge** that should survive session restarts
- You need to **resume context** from a previous session
- Multiple agents need to **coordinate** through shared state
- You want **searchable, structured storage** beyond flat files

**Prerequisite:** A TigerFS mount must be available. Detect it by checking for `.info/` directories inside table subdirectories (see the `using-tigerfs` skill).

## Choose Your Storage

| Storage Type | Best For | Access Pattern |
|--------------|----------|----------------|
| **Markdown app** | Notes, facts, decisions, documents | `Read`/`Write` `.md` files with frontmatter metadata |
| **Native table** | Structured records, typed data, coordination state | `.json` format, column-level access, `.by/` index lookups |

**Default to markdown** for most memory use cases — it's human-readable, searchable with grep, and supports rich metadata via frontmatter.

**Use native tables** when you need typed columns, index lookups, or atomic status updates on individual fields.

## Quick Start

### Set Up Memory (One-Time)

```bash
# Create a markdown app for knowledge/notes
echo "markdown" > mount/.build/memory

# Create a markdown app for session summaries
echo "markdown" > mount/.build/sessions
```

### At Session Start

```
# Check what's available
Glob "mount/memory/*.md"
Glob "mount/sessions/*.md"

# Read recent session summaries
Read "mount/sessions/2024-01-15-auth-refactor.md"

# Search for relevant context
Grep pattern="authentication" path="mount/memory/"
```

### During a Session

```
# Store a learned fact
Write "mount/memory/api-rate-limits.md" with content:
---
title: API Rate Limits
category: infrastructure
confidence: high
source: production logs
---

The external payment API has a 100 req/min rate limit.
Batch requests where possible. See /src/payments/client.ts.
```

### At Session End

```
# Write session summary
Write "mount/sessions/2024-01-15-auth-refactor.md" with content:
---
title: Auth Refactor Session
scope: authentication
status: in-progress
---

## Completed
- Migrated from session tokens to JWT
- Updated /src/auth/middleware.ts

## Open Questions
- Should refresh tokens be stored in httpOnly cookies or localStorage?
- Need to benchmark token validation overhead

## Next Steps
- Implement refresh token rotation
- Add rate limiting to /auth/token endpoint
```

## Patterns Overview

See [patterns.md](patterns.md) for detailed examples of each pattern:

1. **Knowledge Base** — Store learned facts with category, confidence, and source metadata
2. **Session Summaries** — Compressed context from previous sessions for fast resume
3. **Structured Data** — Native table rows for typed, queryable records
4. **Cross-Agent Coordination** — Locks, queues, and shared state via native tables
