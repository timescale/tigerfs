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

func TestGetColumns(t *testing.T) {
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
		CREATE TABLE IF NOT EXISTS test_columns_discovery (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text,
			age integer,
			active boolean DEFAULT true
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		// Cleanup test table
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_columns_discovery")
	}()

	// Test GetColumns
	columns, err := client.GetColumns(ctx, "public", "test_columns_discovery")
	if err != nil {
		t.Fatalf("GetColumns() failed: %v", err)
	}

	// Should have 5 columns
	if len(columns) != 5 {
		t.Fatalf("Expected 5 columns, got %d", len(columns))
	}

	// Verify column names exist
	expectedColumns := map[string]bool{
		"id":     true,
		"name":   true,
		"email":  true,
		"age":    true,
		"active": true,
	}

	for _, col := range columns {
		if !expectedColumns[col.Name] {
			t.Errorf("Unexpected column: %s", col.Name)
		}
		delete(expectedColumns, col.Name)
	}

	if len(expectedColumns) > 0 {
		t.Errorf("Missing expected columns: %v", expectedColumns)
	}

	// Verify nullable flags
	for _, col := range columns {
		switch col.Name {
		case "id":
			if col.IsNullable {
				t.Error("Expected id to be NOT NULL")
			}
		case "name":
			if col.IsNullable {
				t.Error("Expected name to be NOT NULL")
			}
		case "email":
			if !col.IsNullable {
				t.Error("Expected email to be nullable")
			}
		case "age":
			if !col.IsNullable {
				t.Error("Expected age to be nullable")
			}
		case "active":
			if !col.IsNullable {
				t.Error("Expected active to be nullable (columns with defaults are nullable)")
			}
		}
	}

	t.Logf("Found %d columns: %v", len(columns), columns)
}

func TestGetColumns_OrderedByOrdinalPosition(t *testing.T) {
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

	// Create table with columns in non-alphabetical order
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_column_order (
			col3 text,
			col1 integer,
			col2 boolean
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		// Cleanup test table
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_column_order")
	}()

	// Test GetColumns
	columns, err := client.GetColumns(ctx, "public", "test_column_order")
	if err != nil {
		t.Fatalf("GetColumns() failed: %v", err)
	}

	// Verify columns returned in definition order (not alphabetical)
	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(columns))
	}

	expectedOrder := []string{"col3", "col1", "col2"}
	for i, col := range columns {
		if col.Name != expectedOrder[i] {
			t.Errorf("Expected column %d to be %s, got %s", i, expectedOrder[i], col.Name)
		}
	}
}

func TestGetColumns_NonexistentTable(t *testing.T) {
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

	// Test GetColumns on non-existent table
	columns, err := client.GetColumns(ctx, "public", "nonexistent_table")
	if err != nil {
		t.Fatalf("GetColumns() failed: %v", err)
	}

	// Should return empty list, not error
	if len(columns) != 0 {
		t.Errorf("Expected 0 columns for non-existent table, got %d", len(columns))
	}
}

func TestClient_GetColumns_NilPool(t *testing.T) {
	client := &Client{
		cfg: &config.Config{},
	}

	ctx := context.Background()

	_, err := client.GetColumns(ctx, "public", "test_table")
	if err == nil {
		t.Error("Expected error for nil pool, got nil")
	}

	if err.Error() != "database connection not initialized" {
		t.Errorf("Expected 'database connection not initialized', got: %v", err)
	}
}
