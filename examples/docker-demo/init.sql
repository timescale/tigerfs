-- Demo data for TigerFS (Scale Test)
-- ~1,000 users, 10 categories, ~200 products, ~8,000 orders
-- Demonstrates mixed PK types: SERIAL (users, products), TEXT (categories), UUIDv7 (orders)

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
