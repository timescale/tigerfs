-- Demo data for TigerFS (Scale Test)
-- ~1,000 users, ~200 products, ~8,000 orders

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    age INTEGER,
    active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Generate 1,000 users with varied data
INSERT INTO users (name, email, age, active, bio, created_at)
SELECT
    'User ' || i AS name,
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
FROM generate_series(1, 1000) AS i;

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    in_stock BOOLEAN DEFAULT true,
    description TEXT,
    category TEXT
);

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
    CASE (i % 4)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Home'
        WHEN 2 THEN 'Office'
        ELSE 'Outdoor'
    END AS category
FROM generate_series(1, 200) AS i;

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Generate 8,000 orders with realistic distribution
-- Power users (first 100) get more orders
INSERT INTO orders (user_id, product_id, quantity, total, status, created_at)
SELECT
    CASE
        WHEN i % 5 = 0 THEN 1 + (i % 100)  -- Power users
        ELSE 1 + (i % 1000)                 -- All users
    END AS user_id,
    1 + (i % 200) AS product_id,
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

-- Create indexes for common queries
CREATE INDEX idx_users_active ON users(active);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_in_stock ON products(in_stock);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
