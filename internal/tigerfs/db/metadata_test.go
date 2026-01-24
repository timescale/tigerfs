package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestGetRowCount(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_row_count (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_row_count")
	}()

	// Insert test data
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_row_count (name) VALUES
		('Alice'),
		('Bob'),
		('Charlie')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Get row count
	count, err := client.GetRowCount(ctx, "public", "test_row_count")
	if err != nil {
		t.Fatalf("GetRowCount() failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected count=3, got %d", count)
	}

	t.Logf("Row count: %d", count)
}

func TestGetRowCount_EmptyTable(t *testing.T) {
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
		CREATE TABLE IF NOT EXISTS test_row_count_empty (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_row_count_empty")
	}()

	// Get row count
	count, err := client.GetRowCount(ctx, "public", "test_row_count_empty")
	if err != nil {
		t.Fatalf("GetRowCount() failed: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected count=0, got %d", count)
	}
}

func TestGetTableDDL(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_table_ddl (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text,
			age integer DEFAULT 0
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_table_ddl")
	}()

	// Get table DDL
	ddl, err := client.GetTableDDL(ctx, "public", "test_table_ddl")
	if err != nil {
		t.Fatalf("GetTableDDL() failed: %v", err)
	}

	// Verify DDL contains expected elements
	if !strings.Contains(ddl, "CREATE TABLE") {
		t.Error("DDL should contain CREATE TABLE")
	}

	if !strings.Contains(ddl, `"public"."test_table_ddl"`) {
		t.Error("DDL should contain schema-qualified table name")
	}

	if !strings.Contains(ddl, `"id"`) {
		t.Error("DDL should contain id column")
	}

	if !strings.Contains(ddl, `"name"`) {
		t.Error("DDL should contain name column")
	}

	if !strings.Contains(ddl, `"email"`) {
		t.Error("DDL should contain email column")
	}

	if !strings.Contains(ddl, `"age"`) {
		t.Error("DDL should contain age column")
	}

	if !strings.Contains(ddl, "NOT NULL") {
		t.Error("DDL should contain NOT NULL constraint")
	}

	if !strings.Contains(ddl, "PRIMARY KEY") {
		t.Error("DDL should contain PRIMARY KEY constraint")
	}

	t.Logf("Generated DDL:\n%s", ddl)
}

func TestGetTableDDL_CompositePrimaryKey(t *testing.T) {
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
		CREATE TABLE IF NOT EXISTS test_composite_pk (
			user_id integer,
			order_id integer,
			amount numeric,
			PRIMARY KEY (user_id, order_id)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_composite_pk")
	}()

	// Get table DDL
	ddl, err := client.GetTableDDL(ctx, "public", "test_composite_pk")
	if err != nil {
		t.Fatalf("GetTableDDL() failed: %v", err)
	}

	// Verify composite primary key
	if !strings.Contains(ddl, "PRIMARY KEY") {
		t.Error("DDL should contain PRIMARY KEY constraint")
	}

	// Should contain both columns in primary key
	if !strings.Contains(ddl, "user_id") || !strings.Contains(ddl, "order_id") {
		t.Error("DDL should contain both primary key columns")
	}

	t.Logf("Generated DDL with composite PK:\n%s", ddl)
}

func TestGetTableDDL_NoPrimaryKey(t *testing.T) {
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
			name text,
			value integer
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_no_pk")
	}()

	// Get table DDL
	ddl, err := client.GetTableDDL(ctx, "public", "test_no_pk")
	if err != nil {
		t.Fatalf("GetTableDDL() failed: %v", err)
	}

	// Should not contain PRIMARY KEY
	if strings.Contains(ddl, "PRIMARY KEY") {
		t.Error("DDL should not contain PRIMARY KEY for table without PK")
	}

	// Should still have CREATE TABLE and columns
	if !strings.Contains(ddl, "CREATE TABLE") {
		t.Error("DDL should contain CREATE TABLE")
	}

	if !strings.Contains(ddl, `"name"`) {
		t.Error("DDL should contain name column")
	}

	t.Logf("Generated DDL without PK:\n%s", ddl)
}

func TestClient_Metadata_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	// Test GetRowCount
	_, err := client.GetRowCount(ctx, "public", "test_table")
	if err == nil {
		t.Error("Expected error for nil pool in GetRowCount, got nil")
	}
	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}

	// Test GetTableDDL
	_, err = client.GetTableDDL(ctx, "public", "test_table")
	if err == nil {
		t.Error("Expected error for nil pool in GetTableDDL, got nil")
	}
	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}
