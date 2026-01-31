# Tasks App

Manage ordered task lists with status tracking in PostgreSQL.

## What It Does

The Tasks App presents database rows as task files with structured filenames. Tasks have hierarchical numbering, status indicators, and automatic reordering.

**Filename format:** `{number}-{name}-{status}.md`

```
/work/
├── 1-setup-project-x.md      # done
├── 1.1-create-repo-x.md      # done
├── 1.2-add-readme-~.md       # doing (in progress)
├── 2-implement-feature-o.md  # todo (open)
└── 2.1-write-tests-o.md      # todo
```

**Status symbols:**

| Symbol | Meaning | Stored As |
|--------|---------|-----------|
| `o` | Open/todo | `todo` |
| `~` | In progress | `doing` |
| `x` | Complete | `done` |

## Why Use It

- **Hierarchical organization** - Break tasks into subtasks (1.1, 1.2, 1.1.1)
- **Visual status** - See task state at a glance in `ls` output
- **Auto-timestamps** - Track when tasks started/completed
- **Reorderable** - Move tasks by renaming, others shift automatically
- **Agent-friendly** - AI agents manage tasks via simple file operations
- **Persistent** - Tasks survive restarts, stored in PostgreSQL

## Quick Start

```bash
# Create a tasks app
echo "tasks" > /mnt/db/.build/work

# Add tasks
echo "Set up the development environment" > /mnt/db/work/1-setup-o.md
echo "Implement user authentication" > /mnt/db/work/2-auth-o.md
echo "Write API documentation" > /mnt/db/work/3-docs-o.md

# List tasks
ls /mnt/db/work/
# 1-setup-o.md  2-auth-o.md  3-docs-o.md
```

## Usage

### Adding Tasks

```bash
# Simple task
echo "Description here" > /mnt/db/work/1-task-name-o.md

# Subtask
echo "Subtask description" > /mnt/db/work/1.1-subtask-o.md

# With full frontmatter
cat > /mnt/db/work/1-setup-o.md << 'EOF'
---
number: "1"
name: setup
status: todo
assignee: alice
---

Set up the development environment:
- Install dependencies
- Configure database
- Run initial migrations
EOF
```

### Changing Status

**Option 1: Rename the file** (quick)

```bash
# Start working on a task (todo → doing)
mv /mnt/db/work/1-setup-o.md /mnt/db/work/1-setup-~.md

# Complete a task (doing → done)
mv /mnt/db/work/1-setup-~.md /mnt/db/work/1-setup-x.md
```

**Option 2: Edit the frontmatter** (when already editing the file)

```bash
# Open the file and change status: todo → doing
vim /mnt/db/work/1-setup-o.md
```

```yaml
---
number: "1"
name: setup
status: doing    # Changed from "todo"
---
```

The filename updates automatically to match: `1-setup-~.md`

Status timestamps are set automatically:
- `todo_at` - when status becomes `todo`
- `doing_at` - when status becomes `doing`
- `done_at` - when status becomes `done`

### Reordering Tasks

Moving a task to an occupied number shifts others automatically:

```bash
# Before: 1-setup, 2-auth, 3-docs
# Move docs to position 2
mv /mnt/db/work/3-docs-o.md /mnt/db/work/2-docs-o.md

# After: 1-setup, 2-docs, 3-auth (auth shifted to 3)
ls /mnt/db/work/
```

### Closing Gaps

Gaps are preserved until you explicitly compact:

```bash
# After deleting task 2, you have: 1, 3, 4
rm /mnt/db/work/2-some-task-o.md
ls /mnt/db/work/
# 1-setup-x.md  3-auth-o.md  4-docs-o.md

# Compact to close gaps
touch /mnt/db/work/.renumber
ls /mnt/db/work/
# 1-setup-x.md  2-auth-o.md  3-docs-o.md

# Compact only a subtree
echo "2" > /mnt/db/work/.renumber  # Only renumber 2.*
```

### Reading Task Details

```bash
cat /mnt/db/work/1-setup-x.md
```

Output:
```markdown
---
number: "1"
name: setup
status: done
assignee: alice
created_at: 2024-01-15T09:00:00Z
modified_at: 2024-01-15T14:30:00Z
todo_at: 2024-01-15T09:00:00Z
doing_at: 2024-01-15T10:00:00Z
done_at: 2024-01-15T14:30:00Z
---

Set up the development environment:
- Install dependencies
- Configure database
- Run initial migrations
```

### Filtering Tasks

Use TigerFS capabilities to filter:

```bash
# All todo tasks
ls /mnt/db/work/.by/status/todo/

# All tasks assigned to alice
ls /mnt/db/work/.by/assignee/alice/

# Completed tasks as JSON
cat /mnt/db/work/.by/status/done/.export/json
```

## Hierarchical Numbering

Tasks support unlimited nesting with integers at each level:

```
1           # Top-level task
1.1         # First subtask
1.2         # Second subtask
1.2.1       # Sub-subtask
2           # Another top-level
```

**Rules:**
- Numbers start at 1 (no zero)
- Integers only (no letters)
- Any depth allowed

### Creating Subtasks

```bash
# Create a parent task
echo "Build user authentication" > /mnt/db/work/1-auth-o.md

# Add subtasks
echo "Design login flow" > /mnt/db/work/1.1-login-design-o.md
echo "Implement login API" > /mnt/db/work/1.2-login-api-o.md
echo "Build login UI" > /mnt/db/work/1.3-login-ui-o.md

# Add sub-subtasks
echo "Create JWT tokens" > /mnt/db/work/1.2.1-jwt-o.md
echo "Add session storage" > /mnt/db/work/1.2.2-sessions-o.md

# View the hierarchy
ls /mnt/db/work/
# 1-auth-o.md
# 1.1-login-design-o.md
# 1.2-login-api-o.md
# 1.2.1-jwt-o.md
# 1.2.2-sessions-o.md
# 1.3-login-ui-o.md
```

### Moving Tasks Between Parents

```bash
# Move task 1.3 to become 2.1 (under a different parent)
mv /mnt/db/work/1.3-login-ui-o.md /mnt/db/work/2.1-login-ui-o.md

# Promote a subtask to top-level
mv /mnt/db/work/1.2.1-jwt-o.md /mnt/db/work/3-jwt-o.md

# Demote a task to become a subtask
mv /mnt/db/work/3-jwt-o.md /mnt/db/work/1.2.1-jwt-o.md
```

### Shifting is Scoped to Siblings

When you insert at an occupied number, only siblings at the same level shift:

```bash
# Current: 1.1, 1.2, 1.3, 2.1, 2.2
# Insert new task at 1.2
echo "New task" > /mnt/db/work/1.2-new-task-o.md

# Result: 1.1, 1.2(new), 1.3(was 1.2), 1.4(was 1.3), 2.1, 2.2
# Note: 2.x tasks are unaffected
```

### Dynamic Padding

Filenames are zero-padded per-level for correct `ls` sorting:

```bash
# With 15 subtasks under task 1
ls /mnt/db/work/
# 1-auth-o.md
# 1.01-first-o.md
# 1.02-second-o.md
# ...
# 1.15-last-o.md

# Frontmatter shows unpadded numbers
cat /mnt/db/work/1.01-first-o.md
# number: "1.1"  (not "1.01")
```

Padding adjusts automatically based on the maximum number at each level.

## Use Cases

### Project Management

```bash
# Create project tasks
echo "tasks" > /mnt/db/.build/project

# Define milestones and subtasks
echo "Complete MVP" > /mnt/db/project/1-mvp-o.md
echo "User authentication" > /mnt/db/project/1.1-auth-o.md
echo "Login page" > /mnt/db/project/1.1.1-login-o.md
echo "Signup page" > /mnt/db/project/1.1.2-signup-o.md
echo "Dashboard" > /mnt/db/project/1.2-dashboard-o.md

# Track progress
mv /mnt/db/project/1.1.1-login-o.md /mnt/db/project/1.1.1-login-x.md

# See what's left
ls /mnt/db/project/.by/status/todo/
```

### Sprint Planning

```bash
echo "tasks" > /mnt/db/.build/sprint

# Add sprint items with assignees
cat > /mnt/db/sprint/1-api-endpoints-o.md << 'EOF'
---
assignee: alice
---

Implement REST API endpoints for users resource
EOF

cat > /mnt/db/sprint/2-frontend-forms-o.md << 'EOF'
---
assignee: bob
---

Build React forms for user registration
EOF

# Check alice's tasks
ls /mnt/db/sprint/.by/assignee/alice/
```

### Agent Task Management

AI agents can find and claim the next task using sequential numbering:

```bash
# List todo tasks - already sorted by number
ls /mnt/db/work/*-o.md
# 1-research-o.md  2-summarize-o.md  3-draft-o.md

# Get the first (lowest numbered) todo task
NEXT=$(ls /mnt/db/work/*-o.md | head -1)

# Claim it: change status and set assignee via native table
mv "$NEXT" "${NEXT/-o.md/-~.md}"
echo "agent-1" > /mnt/db/_work/.by/number/1/assignee

# When done, mark complete
mv "${NEXT/-o.md/-~.md}" "${NEXT/-o.md/-x.md}"
```

**Search by content:**

```bash
# Find tasks mentioning "database"
grep -l "database" /mnt/db/work/*-o.md
```

### Personal Todo List

```bash
echo "tasks" > /mnt/db/.build/todo

# Quick task entry
echo "Buy groceries" > /mnt/db/todo/1-groceries-o.md
echo "Call dentist" > /mnt/db/todo/2-dentist-o.md
echo "Review PR #123" > /mnt/db/todo/3-review-o.md

# Mark done
mv /mnt/db/todo/2-dentist-o.md /mnt/db/todo/2-dentist-x.md

# Clean up completed
for f in /mnt/db/todo/*-x.md; do rm "$f"; done
touch /mnt/db/todo/.renumber
```

### Multi-Agent Coordination

Multiple agents can coordinate via shared task list:

```bash
# Create shared work queue
echo "tasks" > /mnt/db/.build/queue

# Agent A adds work
echo "Process batch 1" > /mnt/db/queue/1-batch1-o.md
echo "Process batch 2" > /mnt/db/queue/2-batch2-o.md

# Agent B claims a task (atomic via rename)
mv /mnt/db/queue/1-batch1-o.md /mnt/db/queue/1-batch1-~.md

# Agent B completes
mv /mnt/db/queue/1-batch1-~.md /mnt/db/queue/1-batch1-x.md

# Agent C claims next available
```

## Native Table Access

Access the underlying table for SQL operations:

```bash
# Synthesized view (task files)
ls /mnt/db/work/

# Native table (row directories)
ls /mnt/db/_work/
ls /mnt/db/_work/1/status
```

## Tips

1. **Status shortcuts** - Use `o`, `~`, `x` in filenames for quick status changes
2. **Subtasks inherit nothing** - Each task is independent; hierarchy is for organization
3. **Gaps are OK** - Don't feel obligated to renumber; gaps don't affect functionality
4. **Timestamps are automatic** - Just change status; timestamps update automatically
5. **Use assignee** - Great for filtering who should work on what
6. **Combine with grep** - Search task descriptions: `grep -r "urgent" /mnt/db/work/`
