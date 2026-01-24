package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// SetupTestDB starts a PostgreSQL container, creates test schema and seeds data
// Returns connection string and cleanup function
func SetupTestDB(t *testing.T) (string, func()) {
	t.Helper()

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		pgContainer.Terminate(ctx)
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Wait for PostgreSQL to be ready
	time.Sleep(2 * time.Second)

	// Create test schema and seed data
	if err := seedTestData(ctx, connStr); err != nil {
		pgContainer.Terminate(ctx)
		t.Fatalf("Failed to seed test data: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate PostgreSQL container: %v", err)
		}
	}

	return connStr, cleanup
}

// seedTestData creates test tables and inserts test data
func seedTestData(ctx context.Context, connStr string) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Create users table
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text NOT NULL,
			age int,
			active boolean DEFAULT true,
			created_at timestamp DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Insert test users
	_, err = pool.Exec(ctx, `
		INSERT INTO users (name, email, age, active) VALUES
		('Alice', 'alice@example.com', 30, true),
		('Bob', 'bob@example.com', 25, true),
		('Charlie', 'charlie@example.com', 35, false)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert test users: %w", err)
	}

	// Create products table
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS products (
			id serial PRIMARY KEY,
			name text NOT NULL,
			price numeric(10,2) NOT NULL,
			in_stock boolean DEFAULT true
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create products table: %w", err)
	}

	// Insert test products
	_, err = pool.Exec(ctx, `
		INSERT INTO products (name, price, in_stock) VALUES
		('Widget', 19.99, true),
		('Gadget', 29.99, true),
		('Doohickey', 39.99, false)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert test products: %w", err)
	}

	return nil
}
