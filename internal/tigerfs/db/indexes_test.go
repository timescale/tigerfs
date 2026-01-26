package db

import (
	"context"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestGetIndexes verifies index discovery from pg_catalog.
func TestGetIndexes(t *testing.T) {
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

	// Create a test table with various index types
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_indexes (
			id serial PRIMARY KEY,
			email text UNIQUE,
			last_name text,
			first_name text,
			category text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create additional indexes
	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_test_category ON test_indexes(category)`)
	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_test_name ON test_indexes(last_name, first_name)`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_indexes")
	}()

	indexes, err := client.GetIndexes(ctx, "public", "test_indexes")
	if err != nil {
		t.Fatalf("GetIndexes() failed: %v", err)
	}

	// Should have at least 4 indexes: primary key, unique email, category, name composite
	if len(indexes) < 4 {
		t.Fatalf("Expected at least 4 indexes, got %d", len(indexes))
	}

	// Verify we can find each expected index
	foundPK := false
	foundUnique := false
	foundSingle := false
	foundComposite := false

	for _, idx := range indexes {
		t.Logf("Found index: %s (columns=%v, unique=%v, primary=%v)",
			idx.Name, idx.Columns, idx.IsUnique, idx.IsPrimary)

		if idx.IsPrimary {
			foundPK = true
			if len(idx.Columns) != 1 || idx.Columns[0] != "id" {
				t.Errorf("Primary key should be on 'id', got: %v", idx.Columns)
			}
		}

		if idx.IsUnique && !idx.IsPrimary && len(idx.Columns) == 1 && idx.Columns[0] == "email" {
			foundUnique = true
		}

		if idx.Name == "idx_test_category" {
			foundSingle = true
			if len(idx.Columns) != 1 || idx.Columns[0] != "category" {
				t.Errorf("Category index should have one column 'category', got: %v", idx.Columns)
			}
		}

		if idx.Name == "idx_test_name" {
			foundComposite = true
			if len(idx.Columns) != 2 {
				t.Errorf("Name index should have 2 columns, got: %d", len(idx.Columns))
			}
			if idx.Columns[0] != "last_name" || idx.Columns[1] != "first_name" {
				t.Errorf("Name index columns should be [last_name, first_name], got: %v", idx.Columns)
			}
		}
	}

	if !foundPK {
		t.Error("Primary key index not found")
	}
	if !foundUnique {
		t.Error("Unique email index not found")
	}
	if !foundSingle {
		t.Error("Single-column category index not found")
	}
	if !foundComposite {
		t.Error("Composite name index not found")
	}
}

// TestIndex_IsComposite verifies the IsComposite helper method.
func TestIndex_IsComposite(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		expected bool
	}{
		{"single column", []string{"email"}, false},
		{"two columns", []string{"last_name", "first_name"}, true},
		{"three columns", []string{"a", "b", "c"}, true},
		{"empty", []string{}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := Index{Columns: tc.columns}
			if idx.IsComposite() != tc.expected {
				t.Errorf("IsComposite() = %v, expected %v", idx.IsComposite(), tc.expected)
			}
		})
	}
}

// TestGetSingleColumnIndexes verifies filtering to single-column indexes.
func TestGetSingleColumnIndexes(t *testing.T) {
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

	// Create test table with mix of single and composite indexes
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_single_indexes (
			id serial PRIMARY KEY,
			email text,
			last_name text,
			first_name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_single_email ON test_single_indexes(email)`)
	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_single_name ON test_single_indexes(last_name, first_name)`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_single_indexes")
	}()

	indexes, err := client.GetSingleColumnIndexes(ctx, "public", "test_single_indexes")
	if err != nil {
		t.Fatalf("GetSingleColumnIndexes() failed: %v", err)
	}

	// Verify all returned indexes are single-column
	for _, idx := range indexes {
		if idx.IsComposite() {
			t.Errorf("GetSingleColumnIndexes returned composite index: %s with columns %v",
				idx.Name, idx.Columns)
		}
	}

	// Should have at least 2 single-column indexes (pk + email)
	if len(indexes) < 2 {
		t.Errorf("Expected at least 2 single-column indexes, got %d", len(indexes))
	}

	t.Logf("Found %d single-column indexes", len(indexes))
}

// TestGetCompositeIndexes verifies filtering to multi-column indexes.
func TestGetCompositeIndexes(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_composite_indexes (
			id serial PRIMARY KEY,
			email text,
			last_name text,
			first_name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_comp_email ON test_composite_indexes(email)`)
	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_comp_name ON test_composite_indexes(last_name, first_name)`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_composite_indexes")
	}()

	indexes, err := client.GetCompositeIndexes(ctx, "public", "test_composite_indexes")
	if err != nil {
		t.Fatalf("GetCompositeIndexes() failed: %v", err)
	}

	// Verify all returned indexes are composite
	for _, idx := range indexes {
		if !idx.IsComposite() {
			t.Errorf("GetCompositeIndexes returned single-column index: %s with columns %v",
				idx.Name, idx.Columns)
		}
	}

	// Should have at least 1 composite index (name)
	if len(indexes) < 1 {
		t.Errorf("Expected at least 1 composite index, got %d", len(indexes))
	}

	t.Logf("Found %d composite indexes", len(indexes))
}

// TestGetIndexByColumn verifies lookup by leading column.
func TestGetIndexByColumn(t *testing.T) {
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

	// Create test table
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_index_by_column (
			id serial PRIMARY KEY,
			email text,
			last_name text,
			first_name text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_byc_email ON test_index_by_column(email)`)
	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_byc_name ON test_index_by_column(last_name, first_name)`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_index_by_column")
	}()

	// Test finding index by email column
	idx, err := client.GetIndexByColumn(ctx, "public", "test_index_by_column", "email")
	if err != nil {
		t.Fatalf("GetIndexByColumn(email) failed: %v", err)
	}
	if idx == nil {
		t.Fatal("Expected to find index for email column")
	}
	if idx.Columns[0] != "email" {
		t.Errorf("Expected leading column 'email', got '%s'", idx.Columns[0])
	}

	// Test finding composite index by leading column
	idx, err = client.GetIndexByColumn(ctx, "public", "test_index_by_column", "last_name")
	if err != nil {
		t.Fatalf("GetIndexByColumn(last_name) failed: %v", err)
	}
	if idx == nil {
		t.Fatal("Expected to find index for last_name column")
	}
	if idx.Columns[0] != "last_name" {
		t.Errorf("Expected leading column 'last_name', got '%s'", idx.Columns[0])
	}

	// Test non-leading column (first_name is second in composite index)
	idx, err = client.GetIndexByColumn(ctx, "public", "test_index_by_column", "first_name")
	if err != nil {
		t.Fatalf("GetIndexByColumn(first_name) failed: %v", err)
	}
	if idx != nil {
		t.Error("Expected nil for non-leading column first_name")
	}

	// Test column with no index
	idx, err = client.GetIndexByColumn(ctx, "public", "test_index_by_column", "nonexistent")
	if err != nil {
		t.Fatalf("GetIndexByColumn(nonexistent) failed: %v", err)
	}
	if idx != nil {
		t.Error("Expected nil for nonexistent column")
	}
}

// TestGetIndexes_NonexistentTable verifies empty result for missing table.
func TestGetIndexes_NonexistentTable(t *testing.T) {
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

	indexes, err := client.GetIndexes(ctx, "public", "nonexistent_table_xyz")
	if err != nil {
		t.Fatalf("GetIndexes() failed: %v", err)
	}

	if len(indexes) != 0 {
		t.Errorf("Expected empty list for nonexistent table, got %d indexes", len(indexes))
	}
}

// TestClient_GetIndexes_NilPool verifies error on uninitialized client.
func TestClient_GetIndexes_NilPool(t *testing.T) {
	client := &Client{cfg: &config.Config{}}
	ctx := context.Background()

	_, err := client.GetIndexes(ctx, "public", "test")
	if err == nil {
		t.Error("Expected error for nil pool")
	}
	if err.Error() != "database connection not initialized" {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestClient_GetIndexByColumn_NilPool verifies error on uninitialized client.
func TestClient_GetIndexByColumn_NilPool(t *testing.T) {
	client := &Client{cfg: &config.Config{}}
	ctx := context.Background()

	_, err := client.GetIndexByColumn(ctx, "public", "test", "col")
	if err == nil {
		t.Error("Expected error for nil pool")
	}
}

// TestClient_GetSingleColumnIndexes_NilPool verifies error on uninitialized client.
func TestClient_GetSingleColumnIndexes_NilPool(t *testing.T) {
	client := &Client{cfg: &config.Config{}}
	ctx := context.Background()

	_, err := client.GetSingleColumnIndexes(ctx, "public", "test")
	if err == nil {
		t.Error("Expected error for nil pool")
	}
}

// TestClient_GetCompositeIndexes_NilPool verifies error on uninitialized client.
func TestClient_GetCompositeIndexes_NilPool(t *testing.T) {
	client := &Client{cfg: &config.Config{}}
	ctx := context.Background()

	_, err := client.GetCompositeIndexes(ctx, "public", "test")
	if err == nil {
		t.Error("Expected error for nil pool")
	}
}
