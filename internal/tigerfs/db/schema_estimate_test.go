package db

import (
	"context"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestGetTableRowCountEstimate tests getting row count estimate for a table
func TestGetTableRowCountEstimate(t *testing.T) {
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
	defer func() { _ = client.Close() }()

	// Create a test table with some rows
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_row_estimate (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_row_estimate")
	}()

	// Insert some rows
	_, err = client.pool.Exec(ctx, `
		INSERT INTO test_row_estimate (name)
		SELECT 'row_' || generate_series(1, 100)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test rows: %v", err)
	}

	// Run ANALYZE to update statistics
	_, err = client.pool.Exec(ctx, `ANALYZE test_row_estimate`)
	if err != nil {
		t.Fatalf("Failed to analyze table: %v", err)
	}

	// Get estimate
	estimate, err := client.GetTableRowCountEstimate(ctx, "public", "test_row_estimate")
	if err != nil {
		t.Fatalf("GetTableRowCountEstimate() failed: %v", err)
	}

	// Estimate should be approximately 100 (may not be exact)
	// Allow for some variance since reltuples is an estimate
	if estimate < 50 || estimate > 200 {
		t.Errorf("Expected estimate around 100, got %d", estimate)
	}

	t.Logf("Row count estimate for test_row_estimate: %d", estimate)
}

// TestGetTableRowCountEstimate_NonExistentTable tests estimate for non-existent table
func TestGetTableRowCountEstimate_NonExistentTable(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:    5,
		PoolMaxIdle: 2,
	}

	client, err := NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Get estimate for non-existent table
	estimate, err := client.GetTableRowCountEstimate(ctx, "public", "nonexistent_table_12345")
	if err != nil {
		t.Fatalf("GetTableRowCountEstimate() should not error for missing table: %v", err)
	}

	// Should return -1 for missing table
	if estimate != -1 {
		t.Errorf("Expected -1 for non-existent table, got %d", estimate)
	}
}

// TestGetTableRowCountEstimate_EmptyTable tests estimate for empty table
func TestGetTableRowCountEstimate_EmptyTable(t *testing.T) {
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
	defer func() { _ = client.Close() }()

	// Create an empty test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_row_estimate_empty (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_row_estimate_empty")
	}()

	// Run ANALYZE to update statistics
	_, err = client.pool.Exec(ctx, `ANALYZE test_row_estimate_empty`)
	if err != nil {
		t.Fatalf("Failed to analyze table: %v", err)
	}

	// Get estimate for empty table
	estimate, err := client.GetTableRowCountEstimate(ctx, "public", "test_row_estimate_empty")
	if err != nil {
		t.Fatalf("GetTableRowCountEstimate() failed: %v", err)
	}

	// Empty table should have estimate of 0
	if estimate != 0 {
		t.Errorf("Expected 0 for empty table, got %d", estimate)
	}
}
