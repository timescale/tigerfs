package db

import (
	"context"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestGetSchemas(t *testing.T) {
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
	defer client.Close()

	schemas, err := client.GetSchemas(ctx)
	if err != nil {
		t.Fatalf("GetSchemas() failed: %v", err)
	}

	// Should have at least "public" schema
	if len(schemas) == 0 {
		t.Error("Expected at least one schema (public)")
	}

	// Check that "public" schema exists
	hasPublic := false
	for _, schema := range schemas {
		if schema == "public" {
			hasPublic = true
			break
		}
	}

	if !hasPublic {
		t.Error("Expected 'public' schema in results")
	}

	// Verify no system schemas are included
	for _, schema := range schemas {
		if schema == "pg_catalog" || schema == "information_schema" || schema == "pg_toast" {
			t.Errorf("System schema '%s' should not be included", schema)
		}
	}

	t.Logf("Found %d schemas: %v", len(schemas), schemas)
}

func TestGetTables(t *testing.T) {
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

	// Create a test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_schema_discovery (
			id serial PRIMARY KEY,
			name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		// Cleanup test table
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_schema_discovery")
	}()

	// Query tables in public schema
	tables, err := client.GetTables(ctx, "public")
	if err != nil {
		t.Fatalf("GetTables() failed: %v", err)
	}

	// Should find our test table
	found := false
	for _, table := range tables {
		if table == "test_schema_discovery" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected to find test_schema_discovery table, got: %v", tables)
	}

	t.Logf("Found %d tables in public schema: %v", len(tables), tables)
}

func TestGetTables_NonexistentSchema(t *testing.T) {
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
	defer client.Close()

	// Query tables in nonexistent schema
	tables, err := client.GetTables(ctx, "nonexistent_schema")
	if err != nil {
		t.Fatalf("GetTables() failed: %v", err)
	}

	// Should return empty list, not error
	if len(tables) != 0 {
		t.Errorf("Expected empty list for nonexistent schema, got: %v", tables)
	}
}

func TestClient_GetSchemas_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetSchemas(ctx)
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}

func TestClient_GetTables_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetTables(ctx, "public")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}
