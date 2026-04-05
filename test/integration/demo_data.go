package integration

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DemoDataConfig controls the size of demo data for tests.
type DemoDataConfig struct {
	UserCount     int // Number of users (default 100)
	ProductCount  int // Number of products (default 20)
	CategoryCount int // Number of categories (fixed at 10)
	OrderCount    int // Number of orders (default 200)
}

// DefaultDemoConfig returns a smaller dataset suitable for fast tests.
func DefaultDemoConfig() DemoDataConfig {
	return DemoDataConfig{
		UserCount:     100,
		ProductCount:  20,
		CategoryCount: 10,
		OrderCount:    200,
	}
}

// BaseTimestamp is the fixed timestamp used for all created_at fields.
// Using a fixed timestamp ensures deterministic test data.
const BaseTimestamp = "2024-01-15 10:00:00"

// seedDemoData creates test tables with deterministic data for command tests.
// All timestamps use BaseTimestamp to ensure reproducible tests.
func seedDemoData(ctx context.Context, connStr string, cfg DemoDataConfig) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer pool.Close()

	// Create categories table (text PK)
	if err := createCategoriesTable(ctx, pool); err != nil {
		return err
	}

	// Create users table (serial PK)
	if err := createUsersTable(ctx, pool, cfg.UserCount); err != nil {
		return err
	}

	// Create products table (serial PK, FK to categories)
	if err := createProductsTable(ctx, pool, cfg.ProductCount); err != nil {
		return err
	}

	// Create orders table (UUID PK, FKs to users and products)
	if err := createOrdersTable(ctx, pool, cfg.OrderCount, cfg.UserCount, cfg.ProductCount); err != nil {
		return err
	}

	// Create product_inventory table (composite PK: product_id + warehouse)
	if err := createProductInventoryTable(ctx, pool, cfg.ProductCount); err != nil {
		return err
	}

	// Create product_views hypertable (composite PK: time + product_id + user_id)
	if err := createProductViewsTable(ctx, pool, cfg.ProductCount, cfg.UserCount); err != nil {
		return err
	}

	// Create indexes for .by/ navigation
	if err := createIndexes(ctx, pool); err != nil {
		return err
	}

	// Create views
	if err := createViews(ctx, pool); err != nil {
		return err
	}

	return nil
}

func createCategoriesTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE categories (
			slug TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT,
			icon TEXT,
			display_order INTEGER NOT NULL,
			active BOOLEAN DEFAULT true,
			created_at TIMESTAMP DEFAULT '`+BaseTimestamp+`'::timestamp
		)
	`)
	if err != nil {
		return fmt.Errorf("create categories: %w", err)
	}

	// Insert fixed categories
	categories := []struct {
		slug, name, description, icon string
		order                         int
	}{
		{"electronics", "Electronics", "Gadgets, devices, and electronic accessories", "🔌", 1},
		{"home", "Home & Garden", "Furniture, decor, and garden supplies", "🏠", 2},
		{"clothing", "Clothing", "Apparel and fashion accessories", "👕", 3},
		{"books", "Books", "Physical and digital books", "📚", 4},
		{"toys", "Toys & Games", "Toys, games, and entertainment", "🎮", 5},
		{"food", "Food & Grocery", "Food items and groceries", "🍎", 6},
		{"health", "Health & Beauty", "Health, beauty, and personal care", "💊", 7},
		{"automotive", "Automotive", "Car parts and accessories", "🚗", 8},
		{"outdoor", "Outdoor & Sports", "Sporting goods and outdoor equipment", "⚽", 9},
		{"office", "Office Supplies", "Office and school supplies", "📎", 10},
	}

	for _, c := range categories {
		_, err := pool.Exec(ctx, `
			INSERT INTO categories (slug, name, description, icon, display_order)
			VALUES ($1, $2, $3, $4, $5)
		`, c.slug, c.name, c.description, c.icon, c.order)
		if err != nil {
			return fmt.Errorf("insert category %s: %w", c.slug, err)
		}
	}

	return nil
}

func createUsersTable(ctx context.Context, pool *pgxpool.Pool, count int) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			first_name TEXT NOT NULL,
			last_name TEXT NOT NULL,
			email TEXT NOT NULL UNIQUE,
			age INTEGER,
			active BOOLEAN DEFAULT true,
			bio TEXT,
			created_at TIMESTAMP DEFAULT '`+BaseTimestamp+`'::timestamp
		)
	`)
	if err != nil {
		return fmt.Errorf("create users: %w", err)
	}

	// First names and last names for deterministic generation
	firstNames := []string{"Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack", "Kate"}
	lastNames := []string{"Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Anderson"}
	bios := []string{"Software engineer", "Data scientist", "Product manager", "Designer", ""}

	for i := 1; i <= count; i++ {
		firstName := firstNames[(i-1)%len(firstNames)]
		lastName := lastNames[(i-1)%len(lastNames)]
		name := firstName + " " + lastName
		email := fmt.Sprintf("user%d@example.com", i)
		age := 18 + (i % 50)    // Ages 18-67
		active := (i % 10) != 0 // 90% active
		var bio *string
		if i%5 != 0 { // 80% have bio
			b := bios[(i-1)%len(bios)]
			if b != "" {
				bio = &b
			}
		}

		_, err := pool.Exec(ctx, `
			INSERT INTO users (id, name, first_name, last_name, email, age, active, bio)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, i, name, firstName, lastName, email, age, active, bio)
		if err != nil {
			return fmt.Errorf("insert user %d: %w", i, err)
		}
	}

	// Reset sequence
	_, err = pool.Exec(ctx, fmt.Sprintf("SELECT setval('users_id_seq', %d)", count))
	if err != nil {
		return fmt.Errorf("reset users sequence: %w", err)
	}

	return nil
}

func createProductsTable(ctx context.Context, pool *pgxpool.Pool, count int) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE products (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			price NUMERIC(10,2) NOT NULL,
			in_stock BOOLEAN DEFAULT true,
			description TEXT,
			category TEXT REFERENCES categories(slug)
		)
	`)
	if err != nil {
		return fmt.Errorf("create products: %w", err)
	}

	categories := []string{"electronics", "home", "clothing", "books", "toys", "food", "health", "automotive", "outdoor", "office"}
	productTypes := []string{"Gadget", "Gizmo", "Widget", "Device", "Tool"}

	for i := 1; i <= count; i++ {
		productType := productTypes[(i-1)%len(productTypes)]
		name := fmt.Sprintf("%s %d", productType, i)
		price := 10.50 + float64(i)*1.0 // Prices from 11.50
		inStock := (i % 7) != 0         // ~85% in stock
		category := categories[(i-1)%len(categories)]
		description := fmt.Sprintf("A high-quality %s for everyday use", strings.ToLower(productType))

		_, err := pool.Exec(ctx, `
			INSERT INTO products (id, name, price, in_stock, description, category)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, i, name, price, inStock, description, category)
		if err != nil {
			return fmt.Errorf("insert product %d: %w", i, err)
		}
	}

	// Reset sequence
	_, err = pool.Exec(ctx, fmt.Sprintf("SELECT setval('products_id_seq', %d)", count))
	if err != nil {
		return fmt.Errorf("reset products sequence: %w", err)
	}

	return nil
}

func createOrdersTable(ctx context.Context, pool *pgxpool.Pool, count, userCount, productCount int) error {
	_, err := pool.Exec(ctx, `
		CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
		CREATE TABLE orders (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
			user_id INTEGER REFERENCES users(id),
			product_id INTEGER REFERENCES products(id),
			quantity INTEGER NOT NULL,
			total NUMERIC(10,2) NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT '`+BaseTimestamp+`'::timestamp
		)
	`)
	if err != nil {
		return fmt.Errorf("create orders: %w", err)
	}

	statuses := []string{"pending", "processing", "shipped", "delivered", "cancelled"}

	for i := 1; i <= count; i++ {
		userID := ((i - 1) % userCount) + 1
		productID := ((i - 1) % productCount) + 1
		quantity := (i % 5) + 1 // 1-5
		total := float64(quantity) * (10.50 + float64(productID))
		status := statuses[(i-1)%len(statuses)]

		// Use deterministic UUID based on index
		uuid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)

		_, err := pool.Exec(ctx, `
			INSERT INTO orders (id, user_id, product_id, quantity, total, status)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, uuid, userID, productID, quantity, total, status)
		if err != nil {
			return fmt.Errorf("insert order %d: %w", i, err)
		}
	}

	return nil
}

func createProductInventoryTable(ctx context.Context, pool *pgxpool.Pool, productCount int) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE product_inventory (
			product_id INTEGER REFERENCES products(id),
			warehouse TEXT NOT NULL,
			quantity INTEGER NOT NULL DEFAULT 0,
			observed_at TIMESTAMP NOT NULL,
			last_restocked TIMESTAMP,
			PRIMARY KEY (product_id, warehouse)
		)
	`)
	if err != nil {
		return fmt.Errorf("create product_inventory: %w", err)
	}

	warehouses := []string{"east", "west", "central"}

	for i := 1; i <= productCount; i++ {
		for j, wh := range warehouses {
			// Vary quantity by warehouse
			quantity := 5 + (i*(j+1))%50

			// Observed within the last 3 days from base timestamp
			observedOffset := fmt.Sprintf("%d hours", (i%72)+(j*8))

			// Last restocked: some NULL (never restocked)
			var lastRestocked *string
			if i%5 != 0 { // 80% have been restocked
				offset := fmt.Sprintf("%d days", (i%60)+1)
				lr := fmt.Sprintf("'%s'::timestamp - '%s'::interval", BaseTimestamp, offset)
				lastRestocked = &lr
			}

			var lrSQL string
			if lastRestocked != nil {
				lrSQL = *lastRestocked
			} else {
				lrSQL = "NULL"
			}

			_, err := pool.Exec(ctx, fmt.Sprintf(`
				INSERT INTO product_inventory (product_id, warehouse, quantity, observed_at, last_restocked)
				VALUES ($1, $2, $3, '%s'::timestamp - '%s'::interval, %s)
			`, BaseTimestamp, observedOffset, lrSQL), i, wh, quantity)
			if err != nil {
				return fmt.Errorf("insert inventory product %d warehouse %s: %w", i, wh, err)
			}
		}
	}

	return nil
}

func createProductViewsTable(ctx context.Context, pool *pgxpool.Pool, productCount, userCount int) error {
	// Check if TimescaleDB is available
	var hasTimescale bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
		)
	`).Scan(&hasTimescale)
	if err != nil || !hasTimescale {
		// TimescaleDB not available -- skip silently
		return nil
	}

	// Create hypertable using modern TimescaleDB WITH clause syntax
	_, err = pool.Exec(ctx, `
		CREATE TABLE product_views (
			time TIMESTAMPTZ NOT NULL,
			product_id INTEGER NOT NULL REFERENCES products(id),
			user_id INTEGER NOT NULL REFERENCES users(id),
			duration_seconds INTEGER,
			PRIMARY KEY (time, product_id, user_id)
		) WITH (
			tsdb.hypertable,
			tsdb.segmentby = 'product_id',
			tsdb.orderby = 'time DESC'
		)
	`)
	if err != nil {
		return fmt.Errorf("create product_views hypertable: %w", err)
	}

	// Insert deterministic rows based on BaseTimestamp.
	// We generate ~2000 rows: 10 days x ~200 views/day.
	// Each row has a unique (time, product_id, user_id) triple.
	rowCount := 2000
	for i := 1; i <= rowCount; i++ {
		productID := ((i - 1) % productCount) + 1
		userID := ((i - 1) % userCount) + 1
		duration := 5 + (i % 300) // 5-304 seconds

		// Spread across 10 days before BaseTimestamp, ~200 rows per day
		dayOffset := (i - 1) / 200        // 0-9 days back
		hourOffset := ((i - 1) % 200) / 8 // 0-24 hours
		minOffset := (i - 1) % 60         // 0-59 minutes

		ts := fmt.Sprintf("'%s'::timestamptz - '%d days'::interval - '%d hours'::interval - '%d minutes'::interval",
			BaseTimestamp, dayOffset, hourOffset, minOffset)

		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO product_views (time, product_id, user_id, duration_seconds)
			VALUES (%s, $1, $2, $3)
			ON CONFLICT DO NOTHING
		`, ts), productID, userID, duration)
		if err != nil {
			return fmt.Errorf("insert product_view %d: %w", i, err)
		}
	}

	return nil
}

func createIndexes(ctx context.Context, pool *pgxpool.Pool) error {
	indexes := []string{
		"CREATE INDEX idx_users_email ON users(email)",
		"CREATE INDEX idx_users_last_first ON users(last_name, first_name)",
		"CREATE INDEX idx_users_age ON users(age)",
		"CREATE INDEX idx_products_category ON products(category)",
		"CREATE INDEX idx_orders_user ON orders(user_id)",
		"CREATE INDEX idx_orders_product ON orders(product_id)",
		"CREATE INDEX idx_orders_status ON orders(status)",
		"CREATE INDEX idx_orders_created ON orders(created_at)",
		"CREATE INDEX idx_inventory_warehouse ON product_inventory(warehouse)",
	}

	for _, idx := range indexes {
		if _, err := pool.Exec(ctx, idx); err != nil {
			return fmt.Errorf("create index: %w", err)
		}
	}

	return nil
}

func createViews(ctx context.Context, pool *pgxpool.Pool) error {
	// Active users view (updatable)
	_, err := pool.Exec(ctx, `
		CREATE VIEW active_users AS
		SELECT * FROM users WHERE active = true
	`)
	if err != nil {
		return fmt.Errorf("create active_users view: %w", err)
	}

	// Order summary view (read-only, joins multiple tables)
	_, err = pool.Exec(ctx, `
		CREATE VIEW order_summary AS
		SELECT
			o.id as order_id,
			o.created_at as order_date,
			o.status,
			o.quantity,
			o.total,
			u.id as user_id,
			u.name as user_name,
			u.email as user_email,
			p.id as product_id,
			p.name as product_name,
			p.price as product_price,
			c.name as category_name
		FROM orders o
		JOIN users u ON o.user_id = u.id
		JOIN products p ON o.product_id = p.id
		JOIN categories c ON p.category = c.slug
	`)
	if err != nil {
		return fmt.Errorf("create order_summary view: %w", err)
	}

	return nil
}

// Known test data values (for assertions)
var (
	// User 1: Bob Johnson
	User1Name      = "Bob Johnson"
	User1Email     = "user1@example.com"
	User1Age       = 19
	User1Active    = true
	User1Bio       = "Software engineer"
	User1FirstName = "Bob"
	User1LastName  = "Johnson"

	// User 5: Frank Garcia (no bio)
	User5Name   = "Frank Garcia"
	User5Email  = "user5@example.com"
	User5Age    = 23
	User5Active = true
	// User5Bio is NULL

	// User 10: Kate Anderson (inactive)
	User10Name   = "Kate Anderson"
	User10Email  = "user10@example.com"
	User10Age    = 28
	User10Active = false

	// Product 1
	Product1Name     = "Gadget 1"
	Product1Price    = "11.50"
	Product1InStock  = true
	Product1Category = "electronics"

	// Product 7 (out of stock)
	Product7Name     = "Gizmo 7"
	Product7Price    = "17.50"
	Product7InStock  = false
	Product7Category = "health"

	// Category: electronics
	CategoryElectronicsName        = "Electronics"
	CategoryElectronicsDescription = "Gadgets, devices, and electronic accessories"
	CategoryElectronicsOrder       = 1

	// Product inventory (composite PK: product_id, warehouse)
	// Product 1 in 3 warehouses: east, west, central
	Inventory1EastQuantity    = 6 // 5 + (1*1)%50 = 6
	Inventory1WestQuantity    = 7 // 5 + (1*2)%50 = 7
	Inventory1CentralQuantity = 8 // 5 + (1*3)%50 = 8

	// All warehouse names (sorted)
	AllWarehouses = []string{"central", "east", "west"}

	// All category slugs (sorted)
	AllCategorySlugs = []string{
		"automotive", "books", "clothing", "electronics", "food",
		"health", "home", "office", "outdoor", "toys",
	}
)
