# TigerFS Use Cases for Claude Code

## Overview

Exploring how Claude Code can use TigerFS as a persistent data layer - not just for mounting existing databases, but as a place to read/write data that enhances Claude's capabilities.

**Key Insight:** TigerFS + Claude's existing tools (Read/Write/Glob/Grep) = no code needed. We can implement most use cases as **Skills** (behavior instructions) and **Hooks** (automatic triggers).

---

## Use Cases

### 1. External Memory (Skill)
**Problem:** Claude's context is ephemeral. Every session starts fresh.

**Solution:** Skill that teaches Claude to persist and retrieve knowledge.

```
/memory/
├── facts/                    # Things learned about the project
│   └── <id>.md
├── decisions/                # Why we chose X over Y
│   └── <id>/reasoning.md
├── summaries/                # Compressed context from past sessions
│   └── <session_id>/summary.md
└── preferences/              # Learned user style/preferences
    └── coding_style.md
```

**Skill behavior:**
- Write when learning something important
- Read at session start for context
- Search (grep) when context feels missing

---

### 2. Inter-Session Coordination (Skill + Conventions)
**Problem:** Multiple Claude sessions/agents need to coordinate.

**Solution:** Skill that defines protocols for locks, queues, messages.

```
/coordination/
├── messages/
│   └── <id>/
│       ├── from.txt
│       ├── to.txt
│       ├── body.md
│       └── read.txt          # "false" → "true" when consumed
├── locks/
│   └── <resource_name>/      # Existence = locked
│       └── owner.txt
├── queue/
│   └── <task_id>/
│       ├── payload.json
│       └── status.txt        # pending|processing|done
└── state/
    └── <key>/value.txt       # Simple KV store
```

**TigerFS advantage:** File/row existence as lock primitive.

---

### 3. Local → Cloud Pipeline (Config Only)
**Problem:** Want work to flow from local dev to cloud services.

**Solution:** Just TigerFS configuration - mount remote Tiger Cloud DB instead of local PostgreSQL.

```
Local Claude Code                    Cloud
      │                                │
      ▼                                │
   TigerFS ──────[PostgreSQL]─────────► Tiger Cloud DB
   (mount)         (wire)              │
                                       ▼
                              Dashboards, other tools,
                              remote agents, APIs
```

**What flows through:** Analysis results, artifacts, status updates, metrics.

---

### 4. Audit Trail (Skill + Hook)
**Problem:** Need both *what* happened and *why* for accountability and learning.

**Solution:** Combined skill with two components:

#### Part A: Decision Log (Skill)
Conscious logging of significant moments with reasoning.

```
/audit/decisions/
├── <id>/
│   ├── timestamp.txt
│   ├── type.txt              # decision|milestone|problem|tradeoff
│   ├── summary.txt           # One-line description
│   ├── reasoning.md          # Why this decision/action
│   ├── context.md            # What led to this
│   └── outcome.md            # Result (filled in later)
```

**What gets logged:**
- Architectural decisions
- Major implementation milestones
- Problems encountered and solutions
- Trade-offs made

#### Part B: Activity Trace (Hook)
Automatic logging of every tool use.

```
/audit/trace/
├── <timestamp>_<tool>/
│   ├── tool.txt              # "Edit"
│   ├── args.json             # {"file": "/src/auth.ts", ...}
│   ├── result.txt            # "success" or error
│   └── duration_ms.txt
```

**Hook implementation:** Fires on every tool call, writes to trace.

**How they complement:**
- Activity Trace = **what** happened (forensics, debugging)
- Decision Log = **why** it happened (understanding, learning)

---

### 5. Task Management (Skill)
**Problem:** Current task tools are ephemeral - don't survive session restarts.

**Solution:** Skill that persists tasks to TigerFS.

```
/tasks/
├── <id>/
│   ├── subject.txt
│   ├── description.md
│   ├── status.txt            # pending|in_progress|completed
│   ├── owner.txt             # Which agent/session
│   ├── created_at.txt
│   ├── updated_at.txt
│   └── blocks/               # Dependencies
│       └── <other_task_id>
```

**Benefits:** Survives restarts, visible to all agents, queryable history.

---

## Implementation Approach

| Use Case | Type | Implementation |
|----------|------|----------------|
| External Memory | Skill | Read/write patterns for `/memory/` |
| Coordination | Skill | Protocols for locks/queues/messages |
| Local→Cloud | Config | Mount remote DB via TigerFS |
| Audit Trail | Skill + Hook | Decision Log (skill) + Activity Trace (hook) |
| Task Management | Skill | Persist tasks to `/tasks/` |

---

## Directory Structure

```
examples/skills/
├── README.md                 # This file
├── external-memory/
│   ├── SKILL.md              # The skill instructions
│   ├── schema.sql            # CREATE TABLE statements
│   └── README.md             # Setup and usage docs
├── coordination/
│   ├── SKILL.md
│   ├── schema.sql
│   └── README.md
├── audit-trail/
│   ├── SKILL.md              # Decision Log instructions
│   ├── hook.yaml             # Activity Trace hook config
│   ├── schema.sql
│   └── README.md
└── task-management/
    ├── SKILL.md
    ├── schema.sql
    └── README.md
```

**Note:** Local→Cloud Pipeline is config-only, no skill needed.

---

## Prerequisites

Currently, TigerFS cannot create tables via the filesystem. Users must:
1. Connect directly to PostgreSQL
2. Run the `schema.sql` for each use case
3. Mount with TigerFS

---

## Getting Started

Each skill directory contains:
- **SKILL.md** - Instructions for Claude Code to follow
- **schema.sql** - Database tables to create before mounting
- **README.md** - Human-readable setup and usage documentation

To use a skill:
1. Create the required database tables: `psql -f examples/skills/<skill>/schema.sql`
2. Mount the database with TigerFS: `tigerfs mount <connection> /mnt/tigerfs`
3. Install the skill in your Claude Code configuration
4. Claude will automatically use the skill when relevant
