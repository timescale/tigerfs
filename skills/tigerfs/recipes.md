# Recipes

Practical patterns for file-first workflows. Workflow patterns come first (how to work safely), then application patterns (what to build).

---

# Workflow Patterns

## Recipe 1: Safe Agent Exploration

Create a savepoint before investigating, exploring, or making uncertain changes. Auto-savepoints detect session boundaries automatically.

### Pattern: Savepoint, Explore, Review, Keep or Revert

```bash
# 1. Create savepoint before starting
Bash "echo '{\"description\":\"Before investigating bug #42\"}' > mount/notes/.savepoint/before-investigation.json"

# 2. Explore and make changes (edits, creates, deletes)
# ... agent works ...

# 3. Review what changed
Read "mount/notes/.undo/to-savepoint/before-investigation/.info/summary"
Bash "cd mount/notes && diff -ru .undo/to-savepoint/before-investigation . -x '.*'"

# 4a. Keep changes (do nothing -- changes are already saved)

# 4b. Revert all changes (after user confirmation)
Bash "touch mount/notes/.undo/to-savepoint/before-investigation/.apply"
```

### Auto-Savepoints

TigerFS creates savepoints automatically after 30 minutes of inactivity. If an agent starts working after a gap, an auto-savepoint captures the state before the new session. Named `auto-<user>-<timestamp>`.

```
Glob "mount/notes/.savepoint/auto-*"    # List auto-savepoints
```

## Recipe 2: Compare Approaches with Savepoints

Try two implementations and keep the better one.

```bash
# 1. Savepoint the baseline
Bash "echo '{\"description\":\"Clean baseline\"}' > mount/app/.savepoint/baseline.json"

# 2. Implement approach A
# ... agent implements DFS approach ...

# 3. Savepoint after A
Bash "echo '{\"description\":\"DFS implementation complete\"}' > mount/app/.savepoint/after-dfs.json"

# 4. Undo to baseline (clean slate for approach B)
Bash "touch mount/app/.undo/to-savepoint/baseline/.apply"

# 5. Implement approach B
# ... agent implements BFS approach ...

# 6. Compare: B is current, preview A via undo
Read "mount/app/.undo/to-savepoint/after-dfs/.info/summary"

# 7a. Keep B (already current -- do nothing)

# 7b. Keep A instead
Bash "touch mount/app/.undo/to-savepoint/after-dfs/.apply"
# This works because undo-to-savepoint restores the state at that savepoint,
# regardless of what happened after (including B's implementation).
```

## Recipe 3: Multi-Agent Selective Undo

Multiple agents work on the same data with separate identities. An orchestrator can selectively undo one agent's changes while preserving another's.

### Setup: Separate User IDs

Each agent mounts with its own identity:

```bash
# Agent 1: research
tigerfs mount --user-id agent-research postgres://... /mnt/research

# Agent 2: implementation
tigerfs mount --user-id agent-implement postgres://... /mnt/implement
```

Both see the same data. Operations are tagged with the agent's user_id in the log.

### Selective Undo

```bash
# Create a shared savepoint
Bash "echo '{\"description\":\"Sprint start\"}' > mount/notes/.savepoint/sprint-start.json"

# Both agents work...
# agent-research explores and edits files
# agent-implement makes changes too

# View changes by user
Read "mount/notes/.log/.by/user_id/agent-research/.last/10/.export/json"

# Undo only agent-research's changes (preserves agent-implement's work)
Bash "touch mount/notes/.undo/to-savepoint/sprint-start/.by/user_id/agent-research/.apply"
```

**Caveat:** If two agents edit the same file, per-user undo reverts the file to its state before the specified agent's first edit -- which also reverts the other agent's interleaved edits on that same file.

---

# Application Patterns

## Recipe 4: Task Board

Works as: todo list, kanban board, project tracker, shared queue, work coordination. The core pattern: directories = states, files = items, `mv` = transitions, `author` = ownership.

### Setup

```bash
Bash "echo 'markdown,history' > mount/.build/tasks"
Bash "mkdir mount/tasks/todo mount/tasks/doing mount/tasks/done"
```

### Add a Task

```
Write "mount/tasks/todo/fix-auth-bug.md" with content:
---
title: Fix Auth Bug
priority: high
---

The login endpoint returns 500 when session cookie is expired.
```

### Claim a Task

```bash
Bash "mv mount/tasks/todo/fix-auth-bug.md mount/tasks/doing/fix-auth-bug.md"
```

Optionally update the file to set `author: your-name` for ownership tracking.

### Complete a Task

```bash
Bash "mv mount/tasks/doing/fix-auth-bug.md mount/tasks/done/fix-auth-bug.md"
```

### View Board State

```
Glob "mount/tasks/todo/*.md"      # pending
Glob "mount/tasks/doing/*.md"     # in-progress
Glob "mount/tasks/done/*.md"      # completed
Glob "mount/tasks/**/*.md"        # everything
```

### Find Tasks by Author

```
Grep pattern="author: your-name" path="mount/tasks/" glob="*.md"
```

### Multi-Agent Coordination

Multiple agents can read/write concurrently. Each agent sets their name as `author` when claiming. Use Grep to find what others are working on:

```
Grep pattern="author:" path="mount/tasks/doing/" glob="*.md"
```

### Review Task History

```
Glob "mount/tasks/.history/doing/fix-auth-bug.md/*"
```

Shows when the task was moved, who edited it, previous content.

### Custom States

Use any directory names: `backlog/`, `sprint/`, `review/`, `shipped/`. The directory IS the state. `mv` IS the transition. No status columns needed.

## Recipe 5: Knowledge Base with History

### Setup

```bash
Bash "echo 'markdown,history' > mount/.build/kb"
Bash "mkdir mount/kb/architecture mount/kb/debugging mount/kb/conventions"
```

### Store a Fact

```
Write "mount/kb/architecture/chose-jwt.md" with content:
---
title: Chose JWT Over Server Sessions
author: alice
confidence: high
---

## Decision
Use JWT tokens instead of server-side sessions.

## Reasoning
- Stateless -- no session store
- Works across multiple server instances
```

### Organize by Topic

Directories = categories. Move to recategorize:

```bash
Bash "mv mount/kb/debugging/null-bytes.md mount/kb/conventions/null-bytes.md"
```

### Search All Knowledge

```
Grep pattern="authentication" path="mount/kb/"
Grep pattern="confidence: high" path="mount/kb/" glob="*.md"
```

### Track Changes

```
Glob "mount/kb/.history/architecture/chose-jwt.md/*"
```

Read old version vs current to see what evolved.

### Suggested Frontmatter

| Key | Values | Purpose |
|-----|--------|---------|
| `confidence` | `high`, `medium`, `low` | How certain |
| `source` | free text | Where you learned this |
| `supersedes` | filename | If this replaces an older fact |

## Recipe 6: Session Context (Resuming Work)

### Setup

```bash
Bash "echo 'markdown' > mount/.build/sessions"
```

### Save at End of Session

```
Write "mount/sessions/2026-02-24-auth-refactor.md" with content:
---
title: Auth Refactor
status: in-progress
---

## Completed
- Migrated to JWT
- Updated /src/auth/middleware.ts

## Next Steps
- Implement refresh token rotation
```

### Resume at Start of Next Session

```
Glob "mount/sessions/*.md"
Grep pattern="status: in-progress" path="mount/sessions/" glob="*.md"
Read "mount/sessions/2026-02-24-auth-refactor.md"
```

### Naming Convention

Date + topic: `2026-02-24-auth-refactor.md`. Use `status` frontmatter for filtering.

## Recipe 7: Activity Log

Append-only log of what agents and users have done. One file per activity, immutable once written. Multiple agents can write simultaneously without conflicts.

### Setup

```bash
Bash "echo 'markdown' > mount/.build/activity"
```

### Log an Activity

```
Write "mount/activity/2026-03-21T150000.000Z-fixed-auth-bug.md" with content:
---
author: agent-a
type: fix
---

Fixed the auth bug in login endpoint. Changed session cookie handling to check expiry before validating.
```

Use timestamp + description as filename: `YYYY-MM-DDTHHMMSS.mmmZ-short-description.md`. Timestamps ensure chronological ordering.

### Review Recent Activity

```
Glob "mount/activity/*.md"
Glob "mount/activity/2026-03-21*"                              # Today's activity
Grep pattern="author: agent-a" path="mount/activity/" glob="*.md"  # By agent
Grep pattern="type: fix" path="mount/activity/" glob="*.md"        # By type
```

