package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestGetPrimaryKey(t *testing.T) {
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

	// Create test table with single-column primary key
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_pk_single (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_pk_single")
	}()

	// Test single-column primary key
	pk, err := client.GetPrimaryKey(ctx, "public", "test_pk_single")
	if err != nil {
		t.Fatalf("GetPrimaryKey() failed: %v", err)
	}

	if len(pk.Columns) != 1 {
		t.Errorf("Expected 1 primary key column, got %d", len(pk.Columns))
	}

	if pk.Columns[0] != "id" {
		t.Errorf("Expected primary key column 'id', got '%s'", pk.Columns[0])
	}

	t.Logf("Found primary key: %v", pk.Columns)
}

func TestGetPrimaryKey_Composite(t *testing.T) {
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

	// Create test table with composite primary key
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_pk_composite (
			user_id int,
			order_id int,
			PRIMARY KEY (user_id, order_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_pk_composite")
	}()

	// Test composite primary key (should return error)
	_, err = client.GetPrimaryKey(ctx, "public", "test_pk_composite")
	if err == nil {
		t.Fatal("Expected error for composite primary key, got nil")
	}

	// Error should mention composite key
	t.Logf("Got expected error: %v", err)
}

func TestGetPrimaryKey_NoPrimaryKey(t *testing.T) {
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

	// Create test table without primary key
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_no_pk (
			id int,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_no_pk")
	}()

	// Test table without primary key (should return error)
	_, err = client.GetPrimaryKey(ctx, "public", "test_no_pk")
	if err == nil {
		t.Fatal("Expected error for table without primary key, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

func TestListRows(t *testing.T) {
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
		CREATE TABLE IF NOT EXISTS test_list_rows (
			id serial PRIMARY KEY,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_list_rows")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_list_rows (email) VALUES
		('user1@example.com'),
		('user2@example.com'),
		('user3@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// List rows with limit
	rows, err := client.ListRows(ctx, "public", "test_list_rows", "id", 10)
	if err != nil {
		t.Fatalf("ListRows() failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}

	// Check row values are strings
	for i, row := range rows {
		t.Logf("Row %d: %s", i, row)
		if row == "" {
			t.Errorf("Row %d has empty PK value", i)
		}
	}
}

func TestListRows_WithLimit(t *testing.T) {
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
		CREATE TABLE IF NOT EXISTS test_list_rows_limit (
			id serial PRIMARY KEY,
			data text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_list_rows_limit")
	}()

	// Insert 5 rows
	for i := 1; i <= 5; i++ {
		_, err = client.pool.Exec(ctx, `INSERT INTO test_list_rows_limit (data) VALUES ($1)`, fmt.Sprintf("data%d", i))
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// List with limit=2
	rows, err := client.ListRows(ctx, "public", "test_list_rows_limit", "id", 2)
	if err != nil {
		t.Fatalf("ListRows() failed: %v", err)
	}

	// Should only return 2 rows
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows (limit), got %d", len(rows))
	}

	t.Logf("Found %d rows with limit=2: %v", len(rows), rows)
}

func TestListRows_EmptyTable(t *testing.T) {
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

	// Create empty test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_list_rows_empty (
			id serial PRIMARY KEY
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_list_rows_empty")
	}()

	// List rows from empty table
	rows, err := client.ListRows(ctx, "public", "test_list_rows_empty", "id", 10)
	if err != nil {
		t.Fatalf("ListRows() failed: %v", err)
	}

	// Should return empty list, not error
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows for empty table, got %d", len(rows))
	}
}

func TestClient_GetPrimaryKey_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetPrimaryKey(ctx, "public", "test_table")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

func TestClient_ListRows_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.ListRows(ctx, "public", "test_table", "id", 10)
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}
