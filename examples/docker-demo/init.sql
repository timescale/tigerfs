-- Demo data for TigerFS

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    age INTEGER,
    active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email, age, active, bio) VALUES
    ('Alice Smith', 'alice@example.com', 30, true, 'Software engineer who loves databases'),
    ('Bob Jones', 'bob@example.com', 25, true, 'DevOps specialist'),
    ('Charlie Brown', 'charlie@example.com', 35, false, NULL),
    ('Diana Prince', 'diana@example.com', 28, true, 'Full-stack developer'),
    ('Eve Wilson', 'eve@example.com', 32, true, 'Data scientist');

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    in_stock BOOLEAN DEFAULT true,
    description TEXT
);

INSERT INTO products (name, price, in_stock, description) VALUES
    ('Widget', 19.99, true, 'A fantastic widget'),
    ('Gadget', 29.99, true, 'An amazing gadget'),
    ('Doohickey', 39.99, false, 'The ultimate doohickey'),
    ('Thingamajig', 49.99, true, NULL);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO orders (user_id, product_id, quantity, total, status) VALUES
    (1, 1, 2, 39.98, 'completed'),
    (1, 2, 1, 29.99, 'completed'),
    (2, 3, 1, 39.99, 'pending'),
    (4, 1, 5, 99.95, 'shipped');
