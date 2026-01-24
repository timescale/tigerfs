package db

import (
	"context"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestGetRow(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	// Create test table and insert data
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_get_row (
			id serial PRIMARY KEY,
			name text,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_get_row (name, email) VALUES
		('Alice', 'alice@example.com'),
		('Bob', 'bob@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Get row by ID
	row, err := client.GetRow(ctx, "public", "test_get_row", "id", "1")
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	// Verify columns
	if len(row.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(row.Columns))
	}

	expectedColumns := []string{"id", "name", "email"}
	for i, col := range expectedColumns {
		if i >= len(row.Columns) {
			t.Errorf("Missing column %s at index %d", col, i)
			continue
		}
		if row.Columns[i] != col {
			t.Errorf("Expected column %s at index %d, got %s", col, i, row.Columns[i])
		}
	}

	// Verify values
	if len(row.Values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(row.Values))
	}

	t.Logf("Row: columns=%v, values=%v", row.Columns, row.Values)
}

func TestGetRow_NotFound(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	// Create test table (empty)
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_get_row_notfound (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row_notfound")
	}()

	// Try to get non-existent row
	_, err = client.GetRow(ctx, "public", "test_get_row_notfound", "id", "999")
	if err == nil {
		t.Fatal("Expected error for non-existent row, got nil")
	}

	// Error should mention "row not found"
	t.Logf("Got expected error: %v", err)
}

func TestGetRow_WithNULL(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	// Create test table with nullable columns
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_get_row_null (
			id serial PRIMARY KEY,
			name text,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_get_row_null")
	}()

	// Insert row with NULL email
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_get_row_null (name, email) VALUES ('Alice', NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Get row
	row, err := client.GetRow(ctx, "public", "test_get_row_null", "id", "1")
	if err != nil {
		t.Fatalf("GetRow() failed: %v", err)
	}

	// Check that email value is nil
	if len(row.Values) < 3 {
		t.Fatalf("Expected at least 3 values, got %d", len(row.Values))
	}

	emailValue := row.Values[2] // email is 3rd column
	if emailValue != nil {
		t.Errorf("Expected NULL email value, got %v", emailValue)
	}

	t.Logf("Row with NULL: columns=%v, values=%v", row.Columns, row.Values)
}

func TestClient_GetRow_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetRow(ctx, "public", "test_table", "id", "1")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}
