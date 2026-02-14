-- Demo data for TigerFS (Scale Test)
-- ~1,000 users, 10 categories, ~200 products, ~8,000 orders
-- Demonstrates mixed PK types: SERIAL (users, products), TEXT (categories), UUIDv7 (orders)

-- Enable TimescaleDB extension (required for history hypertable support)
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    age INTEGER,
    active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Categories use TEXT primary key (slug-based)
CREATE TABLE categories (
    slug TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    icon TEXT,
    display_order INTEGER,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    in_stock BOOLEAN DEFAULT true,
    description TEXT,
    category TEXT REFERENCES categories(slug)
);

-- Orders use UUIDv7 (time-sortable, native in PostgreSQL 18+)
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id INTEGER REFERENCES users(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Generate 1,000 users with varied data
-- Uses realistic name distributions for composite index testing
INSERT INTO users (name, first_name, last_name, email, age, active, bio, created_at)
SELECT
    first_names[(i % 20) + 1] || ' ' || last_names[(i % 25) + 1] AS name,
    first_names[(i % 20) + 1] AS first_name,
    last_names[(i % 25) + 1] AS last_name,
    'user' || i || '@example.com' AS email,
    18 + (i % 50) AS age,
    (i % 10 != 0) AS active,  -- 10% inactive
    CASE
        WHEN i % 5 = 0 THEN NULL
        WHEN i % 4 = 0 THEN 'Software engineer'
        WHEN i % 4 = 1 THEN 'Data scientist'
        WHEN i % 4 = 2 THEN 'Product manager'
        ELSE 'Designer'
    END AS bio,
    NOW() - (i || ' days')::INTERVAL AS created_at
FROM generate_series(1, 1000) AS i,
     (SELECT ARRAY['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry',
                   'Iris', 'Jack', 'Kate', 'Leo', 'Mia', 'Noah', 'Olivia', 'Paul',
                   'Quinn', 'Rose', 'Sam', 'Tina'] AS first_names,
             ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
                   'Davis', 'Rodriguez', 'Martinez', 'Anderson', 'Taylor', 'Thomas',
                   'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White', 'Harris',
                   'Clark', 'Lewis', 'Walker', 'Hall', 'Young'] AS last_names) AS names;

-- Insert categories
INSERT INTO categories (slug, name, description, icon, display_order) VALUES
    ('electronics', 'Electronics', 'Gadgets, devices, and electronic accessories', '🔌', 1),
    ('home', 'Home & Garden', 'Products for home improvement and gardening', '🏠', 2),
    ('office', 'Office Supplies', 'Everything for your workspace', '📎', 3),
    ('outdoor', 'Outdoor & Sports', 'Gear for outdoor activities and sports', '⛺', 4),
    ('clothing', 'Clothing & Apparel', 'Fashion and everyday wear', '👕', 5),
    ('books', 'Books & Media', 'Books, music, movies, and digital content', '📚', 6),
    ('health', 'Health & Beauty', 'Personal care and wellness products', '💊', 7),
    ('toys', 'Toys & Games', 'Fun for all ages', '🎮', 8),
    ('automotive', 'Automotive', 'Car parts and accessories', '🚗', 9),
    ('food', 'Food & Grocery', 'Snacks, beverages, and pantry items', '🍎', 10);

-- Generate 200 products across categories
INSERT INTO products (name, price, in_stock, description, category)
SELECT
    CASE (i % 5)
        WHEN 0 THEN 'Widget'
        WHEN 1 THEN 'Gadget'
        WHEN 2 THEN 'Gizmo'
        WHEN 3 THEN 'Device'
        ELSE 'Tool'
    END || ' ' || ((i / 5) + 1) AS name,
    ROUND((10 + (i * 1.5))::NUMERIC, 2) AS price,
    (i % 7 != 0) AS in_stock,  -- ~14% out of stock
    CASE
        WHEN i % 3 = 0 THEN NULL
        ELSE 'High-quality ' || LOWER(
            CASE (i % 5)
                WHEN 0 THEN 'Widget'
                WHEN 1 THEN 'Gadget'
                WHEN 2 THEN 'Gizmo'
                WHEN 3 THEN 'Device'
                ELSE 'Tool'
            END
        ) || ' for everyday use'
    END AS description,
    CASE (i % 10)
        WHEN 0 THEN 'electronics'
        WHEN 1 THEN 'home'
        WHEN 2 THEN 'office'
        WHEN 3 THEN 'outdoor'
        WHEN 4 THEN 'clothing'
        WHEN 5 THEN 'books'
        WHEN 6 THEN 'health'
        WHEN 7 THEN 'toys'
        WHEN 8 THEN 'automotive'
        ELSE 'food'
    END AS category
FROM generate_series(1, 200) AS i;

-- Generate 8,000 orders with realistic distribution
-- user_id/product_id are integers (SERIAL), order id is UUIDv7
INSERT INTO orders (user_id, product_id, quantity, total, status, created_at)
SELECT
    CASE
        WHEN i % 5 = 0 THEN (i % 100) + 1  -- Power users (first 100)
        ELSE (i % 1000) + 1                 -- All users
    END AS user_id,
    (i % 200) + 1 AS product_id,
    1 + (i % 5) AS quantity,
    ROUND((10 + (i % 200) * 1.5) * (1 + (i % 5))::NUMERIC, 2) AS total,
    CASE (i % 10)
        WHEN 0 THEN 'cancelled'
        WHEN 1 THEN 'pending'
        WHEN 2 THEN 'pending'
        WHEN 3 THEN 'processing'
        WHEN 4 THEN 'processing'
        WHEN 5 THEN 'shipped'
        WHEN 6 THEN 'shipped'
        ELSE 'completed'
    END AS status,
    NOW() - ((i % 365) || ' days')::INTERVAL - ((i % 24) || ' hours')::INTERVAL AS created_at
FROM generate_series(1, 8000) AS i;

-- Single-column indexes
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_product_id ON orders(product_id);
CREATE INDEX idx_orders_created_at ON orders(created_at);

-- Composite indexes for multi-column navigation
-- Example: ls /mnt/db/users/.by/last_name.first_name/Smith/Alice/
CREATE INDEX idx_users_name ON users(last_name, first_name);
CREATE INDEX idx_orders_status_date ON orders(status, created_at);

-- Views demonstrating different view types
-- Simple view (updatable) - single table with WHERE clause
CREATE VIEW active_users AS
SELECT id, name, first_name, last_name, email, age, bio, created_at
FROM users
WHERE active = true;

-- JOIN view (non-updatable) - combines data from multiple tables
CREATE VIEW order_summary AS
SELECT
    o.id AS order_id,
    o.created_at AS order_date,
    o.status,
    o.quantity,
    o.total,
    u.id AS user_id,
    u.name AS user_name,
    u.email AS user_email,
    p.id AS product_id,
    p.name AS product_name,
    p.price AS product_price,
    c.name AS category_name
FROM orders o
JOIN users u ON o.user_id = u.id
JOIN products p ON o.product_id = p.id
JOIN categories c ON p.category = c.slug;

-- ============================================================================
-- Synthesized Apps: Markdown & Plain Text
-- These demonstrate TigerFS's ability to present database views as directories
-- of .md and .txt files with YAML frontmatter.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- App 1: blog (markdown, 5 posts in 2 subdirectories)
-- Appears as: /mountpoint/blog/hello-world.md,
--             /mountpoint/blog/tutorials/getting-started-with-sql.md, etc.
-- ---------------------------------------------------------------------------

CREATE TABLE "_blog" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    title TEXT,
    author TEXT,
    headers JSONB DEFAULT '{}'::jsonb,
    body TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
);

CREATE VIEW "blog" AS SELECT * FROM "_blog";
COMMENT ON VIEW "blog" IS 'tigerfs:md';

CREATE OR REPLACE FUNCTION "set__blog_modified_at"()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "trg__blog_modified_at"
    BEFORE UPDATE ON "_blog"
    FOR EACH ROW EXECUTE FUNCTION "set__blog_modified_at"();

-- Directory rows (no title/body — just structural containers)
INSERT INTO "_blog" (filename, filetype) VALUES
('tutorials', 'directory'),
('deep-dives', 'directory');

-- File rows
INSERT INTO "_blog" (filename, title, author, headers, body, created_at, modified_at) VALUES
(
    'hello-world.md',
    'Hello, World!',
    'Alice',
    '{"tags": ["intro", "welcome"], "draft": false}'::jsonb,
    E'Welcome to our blog! This is the first post on our new platform.\n\nWe''re excited to share ideas about databases, filesystems, and the\nintersection of Unix tooling with modern data infrastructure.\n\n## What to Expect\n\n- Tutorials on SQL and PostgreSQL\n- Tips for working with markdown\n- Deep dives into filesystem design\n\nStay tuned for more posts!',
    '2025-01-15 09:00:00+00',
    '2025-01-15 09:00:00+00'
),
(
    'tutorials/getting-started-with-sql.md',
    'Getting Started with SQL',
    'Bob',
    '{"tags": ["sql", "beginner"], "draft": false}'::jsonb,
    E'SQL is the lingua franca of data. Whether you''re building an app or\nanalyzing a dataset, knowing SQL is essential.\n\n## Your First Query\n\n```sql\nSELECT * FROM users WHERE active = true;\n```\n\nThis returns all active users from the `users` table.\n\n## Filtering Results\n\nUse `WHERE` clauses to narrow your results:\n\n```sql\nSELECT name, email\nFROM users\nWHERE age > 25\nORDER BY name;\n```\n\n## Next Steps\n\nTry joining tables together to combine related data.',
    '2025-01-20 14:30:00+00',
    '2025-01-20 14:30:00+00'
),
(
    'tutorials/markdown-tips.md',
    'Markdown Tips and Tricks',
    'Alice',
    '{}'::jsonb,
    E'Markdown is a lightweight markup language that''s easy to read and write.\nHere are some tips to level up your formatting.\n\n## Headers\n\nUse `#` for headers. More `#` signs mean smaller headers:\n\n- `#` H1\n- `##` H2\n- `###` H3\n\n## Lists\n\nUnordered lists use `-`, `*`, or `+`:\n\n- Item one\n- Item two\n  - Nested item\n\n## Code Blocks\n\nWrap code in triple backticks for syntax highlighting:\n\n```python\ndef hello():\n    print("Hello, world!")\n```\n\n## Links and Images\n\n- Links: `[text](url)`\n- Images: `![alt](url)`',
    '2025-02-01 10:00:00+00',
    '2025-02-01 10:00:00+00'
),
(
    'deep-dives/why-postgres.md',
    'Why PostgreSQL?',
    'Charlie',
    '{"tags": ["postgresql", "database"], "category": "deep-dive"}'::jsonb,
    E'PostgreSQL is the world''s most advanced open source relational database.\nHere''s why we chose it as the foundation for TigerFS.\n\n## Reliability\n\nPostgreSQL has over 35 years of active development. It''s trusted by\norganizations ranging from startups to Fortune 500 companies.\n\n## Extensibility\n\nThe extension ecosystem is unmatched:\n\n- **PostGIS** for geospatial data\n- **pgvector** for AI embeddings\n- **TimescaleDB** for time-series\n\n## Standards Compliance\n\nPostgreSQL follows the SQL standard closely, making your skills\ntransferable and your queries portable.\n\n## The Ecosystem\n\nTools like `psql`, `pg_dump`, and `pg_restore` make operations\nstraightforward. And with FUSE, you can now browse your data\nas files on disk.',
    '2025-02-10 16:45:00+00',
    '2025-02-10 16:45:00+00'
),
(
    'deep-dives/working-with-views.md',
    'Working with Views',
    'Bob',
    '{"tags": ["postgresql", "views"], "draft": false}'::jsonb,
    E'Views are virtual tables defined by a query. They''re one of PostgreSQL''s\nmost powerful features for organizing and securing data.\n\n## Creating a View\n\n```sql\nCREATE VIEW active_users AS\nSELECT id, name, email\nFROM users\nWHERE active = true;\n```\n\n## Updatable Views\n\nSimple views (single table, no aggregation) are automatically updatable:\n\n```sql\nUPDATE active_users SET name = ''Alice B.'' WHERE id = 1;\n```\n\n## Materialized Views\n\nFor expensive queries, materialized views cache the results:\n\n```sql\nCREATE MATERIALIZED VIEW user_stats AS\nSELECT count(*) AS total, avg(age) AS avg_age\nFROM users;\n```\n\nRefresh them with `REFRESH MATERIALIZED VIEW user_stats;`',
    '2025-02-15 11:20:00+00',
    '2025-02-15 11:20:00+00'
);

-- ---------------------------------------------------------------------------
-- App 2: docs (markdown, 4 pages in 2 subdirectories)
-- Appears as: /mountpoint/docs/getting-started/installation.md,
--             /mountpoint/docs/reference/configuration.md, etc.
-- ---------------------------------------------------------------------------

CREATE TABLE "_docs" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    title TEXT,
    author TEXT,
    headers JSONB DEFAULT '{}'::jsonb,
    body TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
);

CREATE VIEW "docs" AS SELECT * FROM "_docs";
COMMENT ON VIEW "docs" IS 'tigerfs:md';

CREATE OR REPLACE FUNCTION "set__docs_modified_at"()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "trg__docs_modified_at"
    BEFORE UPDATE ON "_docs"
    FOR EACH ROW EXECUTE FUNCTION "set__docs_modified_at"();

-- Directory rows
INSERT INTO "_docs" (filename, filetype) VALUES
('getting-started', 'directory'),
('reference', 'directory');

-- File rows
INSERT INTO "_docs" (filename, title, author, headers, body, created_at, modified_at) VALUES
(
    'getting-started/installation.md',
    'Installation Guide',
    'TigerFS Team',
    '{"section": "getting-started"}'::jsonb,
    E'# Installation\n\nTigerFS runs on macOS and Linux. Choose your platform below.\n\n## macOS (Homebrew)\n\n```bash\nbrew install tigerfs\n```\n\nTigerFS uses NFS on macOS — no kernel extensions required.\n\n## Linux\n\nDownload the latest release from GitHub:\n\n```bash\ncurl -L https://github.com/timescale/tigerfs/releases/latest/download/tigerfs_linux_amd64.tar.gz | tar xz\nsudo mv tigerfs /usr/local/bin/\n```\n\nEnsure FUSE is available:\n\n```bash\nsudo apt install fuse3   # Debian/Ubuntu\nsudo dnf install fuse3   # Fedora\n```\n\n## Verify Installation\n\n```bash\ntigerfs version\n```',
    '2025-01-10 08:00:00+00',
    '2025-01-10 08:00:00+00'
),
(
    'reference/configuration.md',
    'Configuration Reference',
    'TigerFS Team',
    '{"section": "reference"}'::jsonb,
    E'# Configuration\n\nTigerFS reads configuration from multiple sources with the following\nprecedence (highest wins):\n\n1. Command-line flags\n2. Environment variables (`TIGERFS_*`)\n3. Config file (`~/.config/tigerfs/config.yaml`)\n4. Built-in defaults\n\n## Config File\n\n```yaml\nformat: json          # Default output format: json, tsv, csv\nread_only: false      # Mount as read-only\nmax_ls_rows: 10000    # Threshold for large table warning\n```\n\n## Environment Variables\n\nAll config keys can be set via environment:\n\n```bash\nexport TIGERFS_FORMAT=tsv\nexport TIGERFS_READ_ONLY=true\nexport TIGERFS_MAX_LS_ROWS=5000\n```\n\n## Connection String\n\nPass via argument or environment:\n\n```bash\ntigerfs mount postgres://user:pass@host/db /mnt/db\n# or\nexport TIGERFS_CONNECTION=postgres://user:pass@host/db\ntigerfs mount /mnt/db\n```',
    '2025-01-10 08:30:00+00',
    '2025-01-10 08:30:00+00'
),
(
    'getting-started/quick-start.md',
    'Quick Start',
    'TigerFS Team',
    '{"section": "getting-started"}'::jsonb,
    E'# Quick Start\n\nGet up and running with TigerFS in under 5 minutes.\n\n## 1. Start a Database\n\nIf you don''t have a PostgreSQL instance, start one with Docker:\n\n```bash\ndocker run -d --name pg -p 5432:5432 \\\n  -e POSTGRES_PASSWORD=secret postgres:17\n```\n\n## 2. Mount the Database\n\n```bash\nmkdir -p /tmp/mydb\ntigerfs mount postgres://postgres:secret@localhost/postgres /tmp/mydb\n```\n\n## 3. Browse Your Data\n\n```bash\nls /tmp/mydb/              # List tables\nls /tmp/mydb/users/        # List rows\ncat /tmp/mydb/users/1.json # Read a row\n```\n\n## 4. Make Changes\n\n```bash\n# Edit a row (opens in $EDITOR)\nvim /tmp/mydb/users/1.json\n\n# Delete a row\nrm /tmp/mydb/users/42.json\n```\n\n## Next Steps\n\nSee the [Configuration](configuration.md) guide for customization options.',
    '2025-01-10 09:00:00+00',
    '2025-01-10 09:00:00+00'
),
(
    'reference/api-reference.md',
    'API Reference',
    'TigerFS Team',
    '{"section": "reference"}'::jsonb,
    E'# API Reference\n\nTigerFS exposes a filesystem API — every operation maps to SQL.\n\n## Reading Data\n\n| Operation | SQL Equivalent |\n|-----------|---------------|\n| `ls tables/` | `SELECT * FROM pg_tables` |\n| `ls users/` | `SELECT pk FROM users` |\n| `cat users/1.json` | `SELECT * FROM users WHERE id = 1` |\n| `cat users/.export/csv` | `SELECT * FROM users` (CSV format) |\n\n## Writing Data\n\n| Operation | SQL Equivalent |\n|-----------|---------------|\n| `echo ''...'' > users/1.json` | `UPDATE users SET ... WHERE id = 1` |\n| `rm users/1.json` | `DELETE FROM users WHERE id = 1` |\n| `cp template.json users/new.json` | `INSERT INTO users ...` |\n\n## Special Directories\n\n- `.export/` — Bulk export in TSV, CSV, or JSON format\n- `.by/` — Index-based navigation\n- `.schema` — Table schema information\n\n## Error Mapping\n\n| Errno | Meaning |\n|-------|---------|\n| ENOENT | Row not found |\n| EACCES | Read-only mount |\n| EINVAL | Invalid data format |\n| EIO | Database connection error |',
    '2025-01-10 09:30:00+00',
    '2025-01-10 09:30:00+00'
);

-- ---------------------------------------------------------------------------
-- App 3: snippets (plain text, 3 files with 1 subdirectory)
-- Appears as: /mountpoint/snippets/todo.txt,
--             /mountpoint/snippets/meetings/meeting-notes.txt, etc.
-- ---------------------------------------------------------------------------

CREATE TABLE "_snippets" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    body TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
);

CREATE VIEW "snippets" AS SELECT * FROM "_snippets";
COMMENT ON VIEW "snippets" IS 'tigerfs:txt';

CREATE OR REPLACE FUNCTION "set__snippets_modified_at"()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "trg__snippets_modified_at"
    BEFORE UPDATE ON "_snippets"
    FOR EACH ROW EXECUTE FUNCTION "set__snippets_modified_at"();

-- Directory rows
INSERT INTO "_snippets" (filename, filetype) VALUES
('meetings', 'directory');

-- File rows
INSERT INTO "_snippets" (filename, body, created_at, modified_at) VALUES
(
    'todo.txt',
    E'TODO List\n=========\n\n[ ] Set up CI/CD pipeline\n[ ] Write integration tests for FUSE adapter\n[ ] Add CSV export support\n[x] Implement JSON row format\n[x] Create demo data script\n[ ] Update README with examples\n[ ] Benchmark large table performance\n[x] Add index-based navigation',
    '2025-02-01 08:00:00+00',
    '2025-02-01 08:00:00+00'
),
(
    'meetings/meeting-notes.txt',
    E'Team Sync — 2025-02-10\n======================\n\nAttendees: Alice, Bob, Charlie\n\nAgenda:\n  1. Sprint review\n  2. Demo prep\n  3. Release timeline\n\nNotes:\n  - Sprint went well, all planned features complete\n  - Demo data needs markdown app examples\n  - Targeting v0.3.0 release by end of month\n  - Bob will handle the docker-demo updates\n  - Charlie to review documentation\n\nAction Items:\n  - Alice: finalize synthesized app feature\n  - Bob: update demo scripts\n  - Charlie: review and edit docs/',
    '2025-02-10 15:00:00+00',
    '2025-02-10 15:00:00+00'
),
(
    'scratch.txt',
    E'Random notes and scratch pad\n----------------------------\n\nUseful psql commands:\n  \\dt          list tables\n  \\dv          list views\n  \\d+ table    describe table with details\n  \\x           toggle expanded output\n\nConnection string format:\n  postgres://user:pass@host:port/dbname?sslmode=disable\n\nQuick test:\n  SELECT version();\n  SELECT current_database();\n  SELECT current_user;',
    '2025-02-12 10:00:00+00',
    '2025-02-12 10:00:00+00'
);
