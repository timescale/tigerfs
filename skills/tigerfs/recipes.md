# Recipes

Practical patterns for real workflows. All recipes use markdown apps.

## Recipe 1: Task Board

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

Move it to `doing/` and set yourself as author:

```bash
Bash "mv mount/tasks/todo/fix-auth-bug.md mount/tasks/doing/fix-auth-bug.md"
```

Then update to set ownership:

```
Read "mount/tasks/doing/fix-auth-bug.md"
```

Write back with `author: your-name` added to frontmatter.

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

---

## Recipe 2: Knowledge Base with History

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
- Stateless — no session store
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

---

## Recipe 3: Session Context (Resuming Work)

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

---

## Recipe 4: Plain Text Snippets

### Setup

```bash
Bash "echo 'plaintext' > mount/.build/snippets"
```

### Store a Snippet

```
Write "mount/snippets/bash-loop.txt" with body content only (no frontmatter).
```

### Organize

```bash
Bash "mkdir mount/snippets/bash mount/snippets/sql"
Bash "mv mount/snippets/bash-loop.txt mount/snippets/bash/bash-loop.txt"
```

### Search

```
Grep pattern="for.*in" path="mount/snippets/"
Glob "mount/snippets/sql/*.txt"
```
