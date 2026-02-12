# Memory Patterns

Detailed patterns with concrete examples for using TigerFS as persistent agent memory.

## Pattern 1: Knowledge Base (Markdown App)

Store learned facts, decisions, and discoveries as markdown files with structured metadata.

### Setup

```bash
echo "markdown" > mount/.build/memory
```

### Store a Fact

```
Write "mount/memory/postgres-json-nullbytes.md" with content:
---
title: PostgreSQL TEXT Cannot Store Null Bytes
category: databases
confidence: high
source: debugging session 2024-01-10
---

PostgreSQL TEXT columns silently reject or corrupt data containing null bytes (0x00).
When writing binary or arbitrary data, use BYTEA columns instead.
For test data, always use printable ASCII characters.
```

### Store a Decision

```
Write "mount/memory/chose-jwt-over-sessions.md" with content:
---
title: Chose JWT Over Server Sessions
category: architecture
confidence: high
source: team discussion 2024-01-12
---

## Decision
Use JWT tokens for API authentication instead of server-side sessions.

## Reasoning
- Stateless — no session store needed
- Works across multiple server instances
- Client can decode token for user info without API call

## Trade-offs
- Tokens can't be revoked instantly (use short expiry + refresh tokens)
- Larger request payload than session cookies
```

### Read at Session Start

```
# List all stored knowledge
Glob "mount/memory/*.md"

# Read specific entries
Read "mount/memory/chose-jwt-over-sessions.md"

# Search by category
Grep pattern="category: architecture" path="mount/memory/" glob="*.md"

# Search by content
Grep pattern="authentication" path="mount/memory/"
```

### Update When Knowledge Changes

```
# Overwrite with corrected/updated content
Write "mount/memory/api-rate-limits.md" with updated content including new information
```

### Suggested Frontmatter Keys

| Key | Values | Purpose |
|-----|--------|---------|
| `category` | `architecture`, `debugging`, `infrastructure`, `conventions`, `preferences` | Organize by topic |
| `confidence` | `high`, `medium`, `low` | How certain this fact is |
| `source` | Free text | Where you learned this |
| `supersedes` | Filename | If this replaces an older fact |

## Pattern 2: Session Summaries (Markdown App)

Compressed context from previous sessions, enabling fast resume without re-reading all code.

### Setup

```bash
echo "markdown" > mount/.build/sessions
```

### Write a Summary at Session End

```
Write "mount/sessions/2024-01-15-payment-integration.md" with content:
---
title: Payment Integration
scope: payments, api
status: completed
---

## What Was Done
- Integrated Stripe API for payment processing
- Created /src/payments/client.ts with retry logic
- Added webhook handler at /api/webhooks/stripe

## Key Decisions
- Used Stripe Checkout (hosted) instead of Elements (embedded) for PCI simplicity
- Webhook signature verification in middleware, not per-handler
- Idempotency keys derived from order ID

## Files Changed
- src/payments/client.ts (new)
- src/api/webhooks/stripe.ts (new)
- src/config/env.ts (added STRIPE_* vars)

## Open Issues
- Need to handle subscription cancellation flow
- Webhook retry behavior untested under load
```

### Resume at Session Start

```
# Find recent sessions
Glob "mount/sessions/*.md"

# Read the most relevant ones
Read "mount/sessions/2024-01-15-payment-integration.md"

# Search for sessions related to current work
Grep pattern="scope: payments" path="mount/sessions/" glob="*.md"
Grep pattern="status: in-progress" path="mount/sessions/" glob="*.md"
```

### Suggested Frontmatter Keys

| Key | Values | Purpose |
|-----|--------|---------|
| `scope` | Comma-separated areas | What parts of the project this covers |
| `status` | `in-progress`, `completed`, `abandoned` | Whether work is done |

## Pattern 3: Structured Data (Native Table)

Use native table access for typed, queryable records — useful for logs, metrics, or any data where column types and index lookups matter.

### Setup

Create a table directly in PostgreSQL (TigerFS doesn't create tables via native access):

```sql
CREATE TABLE agent_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    event_type VARCHAR(50) NOT NULL,
    agent_id VARCHAR(100),
    details JSONB DEFAULT '{}'
);
CREATE INDEX ON agent_events (event_type);
CREATE INDEX ON agent_events (agent_id);
```

### Write an Event

```
Write "mount/agent_events/new.json" with content:
{"event_type":"task_completed","agent_id":"claude-session-42","details":{"task":"fix auth bug","duration_seconds":180}}
```

### Query Events

```
# Check available indexes
Read "mount/agent_events/.info/indexes"

# Find events by type
Glob "mount/agent_events/.by/event_type/task_completed/.last/10/*"

# Find events by agent
Glob "mount/agent_events/.by/agent_id/claude-session-42/*"

# Read a specific event
Read "mount/agent_events/1.json"

# Read just the details
Read "mount/agent_events/1/details"
```

### Configuration Key-Value Store

For simple key-value storage, a single-column approach works well:

```sql
CREATE TABLE agent_config (
    key VARCHAR(255) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

```
# Write a config value
Write "mount/agent_config/new.json" with content '{"key":"preferred_model","value":"claude-opus"}'

# Read a config value (use the key as PK)
Read "mount/agent_config/preferred_model/value"

# Update a config value
Write "mount/agent_config/preferred_model/value" with content "claude-sonnet"
```

## Pattern 4: Cross-Agent Coordination (Native Table)

When multiple agents or sessions need to share state, use native tables with status columns and row-level operations.

### Locks (Row Existence = Lock Held)

```sql
CREATE TABLE agent_locks (
    resource VARCHAR(255) PRIMARY KEY,
    owner VARCHAR(100) NOT NULL,
    acquired_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Acquire a lock:**
```
Write "mount/agent_locks/new.json" with content '{"resource":"deploy-staging","owner":"agent-session-42"}'
```

If the resource already exists, the insert fails (UNIQUE constraint) — lock not acquired.

**Check lock status:**
```
Read "mount/agent_locks/deploy-staging/owner"    # Returns owner, or file-not-found if unlocked
```

**Release a lock:**
```bash
rm mount/agent_locks/deploy-staging
```

### Task Queue (Status Column)

```sql
CREATE TABLE agent_tasks (
    id SERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    owner VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON agent_tasks (status);
```

**Enqueue a task:**
```
Write "mount/agent_tasks/new.json" with content '{"payload":{"action":"run_tests","target":"auth"},"status":"pending"}'
```

**Claim a task:**
```
# Find pending tasks
Glob "mount/agent_tasks/.by/status/pending/.first/1/*"

# Claim it by updating status and owner
Write "mount/agent_tasks/42.json" with content '{"status":"processing","owner":"agent-session-7"}'
```

**Complete a task:**
```
Write "mount/agent_tasks/42.json" with content '{"status":"done"}'
```

### Shared State

Use a simple key-value table for shared configuration or state:

```
# Write shared state
Write "mount/agent_config/new.json" with content '{"key":"deploy_status","value":"in-progress"}'

# Read shared state
Read "mount/agent_config/deploy_status/value"

# Update shared state
Write "mount/agent_config/deploy_status/value" with content "completed"
```

## Choosing Between Patterns

| Need | Pattern | Why |
|------|---------|-----|
| Store knowledge/facts | Knowledge base (markdown) | Human-readable, searchable, rich metadata |
| Resume from previous session | Session summaries (markdown) | Narrative format, easy to scan |
| Log events/actions | Structured data (native) | Typed columns, indexed queries |
| Simple key-value storage | Structured data (native) | Direct column access by key |
| Multi-agent locking | Coordination (native) | Row existence as lock, UNIQUE constraints |
| Task distribution | Coordination (native) | Status column, index lookups |
| Human-editable notes | Knowledge base (markdown) | Standard markdown, any editor works |
