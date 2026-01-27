package db

import (
	"context"
	"strings"
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

// TestGetDistinctValues verifies retrieval of distinct column values.
func TestGetDistinctValues(t *testing.T) {
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

	// Create test table with some data
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_distinct_values (
			id serial PRIMARY KEY,
			category text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data with duplicate categories
	_, _ = client.pool.Exec(ctx, `
		INSERT INTO test_distinct_values (category) VALUES
		('electronics'), ('books'), ('electronics'), ('clothing'), ('books'), (NULL)
	`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_distinct_values")
	}()

	values, err := client.GetDistinctValues(ctx, "public", "test_distinct_values", "category", 100)
	if err != nil {
		t.Fatalf("GetDistinctValues() failed: %v", err)
	}

	// Should have 3 distinct non-null values
	if len(values) != 3 {
		t.Errorf("Expected 3 distinct values, got %d: %v", len(values), values)
	}

	// Verify expected values present
	expected := map[string]bool{"electronics": false, "books": false, "clothing": false}
	for _, v := range values {
		expected[v] = true
	}
	for k, found := range expected {
		if !found {
			t.Errorf("Expected value '%s' not found", k)
		}
	}

	t.Logf("Found %d distinct values: %v", len(values), values)
}

// TestGetDistinctValues_Limit verifies limit is respected.
func TestGetDistinctValues_Limit(t *testing.T) {
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

	// Create test table with many distinct values
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_distinct_limit (
			id serial PRIMARY KEY,
			value text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert 10 distinct values
	for i := 0; i < 10; i++ {
		_, _ = client.pool.Exec(ctx, `INSERT INTO test_distinct_limit (value) VALUES ($1)`,
			string(rune('A'+i)))
	}

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_distinct_limit")
	}()

	// Request only 3
	values, err := client.GetDistinctValues(ctx, "public", "test_distinct_limit", "value", 3)
	if err != nil {
		t.Fatalf("GetDistinctValues() failed: %v", err)
	}

	if len(values) != 3 {
		t.Errorf("Expected 3 values (limit), got %d", len(values))
	}
}

// TestGetRowsByIndexValue verifies retrieval of rows by index value.
func TestGetRowsByIndexValue(t *testing.T) {
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
		CREATE TABLE IF NOT EXISTS test_rows_by_index (
			id serial PRIMARY KEY,
			category text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, _ = client.pool.Exec(ctx, `
		INSERT INTO test_rows_by_index (category) VALUES
		('electronics'), ('books'), ('electronics'), ('books')
	`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_rows_by_index")
	}()

	// Query rows with category='electronics'
	pks, err := client.GetRowsByIndexValue(ctx, "public", "test_rows_by_index", "category", "electronics", "id", 100)
	if err != nil {
		t.Fatalf("GetRowsByIndexValue() failed: %v", err)
	}

	// Should have 2 rows with 'electronics'
	if len(pks) != 2 {
		t.Errorf("Expected 2 rows, got %d: %v", len(pks), pks)
	}

	// Query non-existent value
	pks, err = client.GetRowsByIndexValue(ctx, "public", "test_rows_by_index", "category", "nonexistent", "id", 100)
	if err != nil {
		t.Fatalf("GetRowsByIndexValue() failed: %v", err)
	}

	if len(pks) != 0 {
		t.Errorf("Expected 0 rows for nonexistent value, got %d", len(pks))
	}
}

// TestClient_GetDistinctValues_NilPool verifies error on uninitialized client.
func TestClient_GetDistinctValues_NilPool(t *testing.T) {
	client := &Client{cfg: &config.Config{}}
	ctx := context.Background()

	_, err := client.GetDistinctValues(ctx, "public", "test", "col", 100)
	if err == nil {
		t.Error("Expected error for nil pool")
	}
}

// TestClient_GetRowsByIndexValue_NilPool verifies error on uninitialized client.
func TestClient_GetRowsByIndexValue_NilPool(t *testing.T) {
	client := &Client{cfg: &config.Config{}}
	ctx := context.Background()

	_, err := client.GetRowsByIndexValue(ctx, "public", "test", "col", "val", "id", 100)
	if err == nil {
		t.Error("Expected error for nil pool")
	}
}

// TestGetRowsByIndexValue_UsesIndex verifies PostgreSQL uses the index for lookups.
// This is an integration test that requires a real database connection.
func TestGetRowsByIndexValue_UsesIndex(t *testing.T) {
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

	// Create test table with index
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_explain_index (
			id serial PRIMARY KEY,
			email text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create index on email column
	_, _ = client.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_test_explain_email ON test_explain_index(email)`)

	// Insert enough data to make index usage worthwhile
	// PostgreSQL may choose seq scan for very small tables
	for i := 0; i < 100; i++ {
		_, _ = client.pool.Exec(ctx, `INSERT INTO test_explain_index (email) VALUES ($1)`,
			"user"+string(rune('0'+i%10))+"@example.com")
	}

	// Analyze table to update statistics
	_, _ = client.pool.Exec(ctx, `ANALYZE test_explain_index`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_explain_index")
	}()

	// Run EXPLAIN on our query pattern
	query := `EXPLAIN SELECT "id" FROM "public"."test_explain_index" WHERE "email" = $1 ORDER BY "id" LIMIT $2`
	rows, err := client.pool.Query(ctx, query, "user1@example.com", 100)
	if err != nil {
		t.Fatalf("EXPLAIN query failed: %v", err)
	}
	defer rows.Close()

	// Collect EXPLAIN output
	var explainOutput []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("Failed to scan EXPLAIN output: %v", err)
		}
		explainOutput = append(explainOutput, line)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating EXPLAIN output: %v", err)
	}

	// Check that index is used (look for "Index Scan" or "Bitmap Index Scan")
	indexUsed := false
	for _, line := range explainOutput {
		if strings.Contains(line, "Index Scan") || strings.Contains(line, "Bitmap Index Scan") {
			indexUsed = true
			break
		}
	}

	if !indexUsed {
		t.Errorf("Expected index scan, but EXPLAIN output shows:\n%s", strings.Join(explainOutput, "\n"))
	} else {
		t.Logf("Verified index usage. EXPLAIN output:\n%s", strings.Join(explainOutput, "\n"))
	}
}

// TestGetDistinctValuesOrdered verifies ordered distinct value retrieval.
func TestGetDistinctValuesOrdered(t *testing.T) {
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

	// Create test table with ordered data
	_, err = client.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_distinct_ordered (
			id serial PRIMARY KEY,
			priority int
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, _ = client.pool.Exec(ctx, `
		INSERT INTO test_distinct_ordered (priority) VALUES
		(5), (3), (1), (4), (2), (5), (1)
	`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_distinct_ordered")
	}()

	// Test ascending order (first N)
	values, err := client.GetDistinctValuesOrdered(ctx, "public", "test_distinct_ordered", "priority", 3, true)
	if err != nil {
		t.Fatalf("GetDistinctValuesOrdered(ASC) failed: %v", err)
	}

	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	// Values should be 1, 2, 3 (first 3 in ascending order)
	if len(values) >= 3 {
		if values[0] != "1" || values[1] != "2" || values[2] != "3" {
			t.Errorf("Expected [1, 2, 3] ascending, got %v", values)
		}
	}

	// Test descending order (last N)
	values, err = client.GetDistinctValuesOrdered(ctx, "public", "test_distinct_ordered", "priority", 3, false)
	if err != nil {
		t.Fatalf("GetDistinctValuesOrdered(DESC) failed: %v", err)
	}

	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	// Values should be 5, 4, 3 (first 3 in descending order = last 3)
	if len(values) >= 3 {
		if values[0] != "5" || values[1] != "4" || values[2] != "3" {
			t.Errorf("Expected [5, 4, 3] descending, got %v", values)
		}
	}

	t.Logf("Ascending: %v, Descending: %v", values, values)
}

// TestGetRowsByIndexValueOrdered verifies ordered rows retrieval by index value.
func TestGetRowsByIndexValueOrdered(t *testing.T) {
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
		CREATE TABLE IF NOT EXISTS test_rows_ordered (
			id serial PRIMARY KEY,
			category text
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data - IDs will be 1, 2, 3, 4
	_, _ = client.pool.Exec(ctx, `
		INSERT INTO test_rows_ordered (category) VALUES
		('electronics'), ('books'), ('electronics'), ('electronics')
	`)

	defer func() {
		_, _ = client.pool.Exec(context.Background(), "DROP TABLE IF EXISTS test_rows_ordered")
	}()

	// Test ascending order - first 2 electronics rows
	pks, err := client.GetRowsByIndexValueOrdered(ctx, "public", "test_rows_ordered", "category", "electronics", "id", 2, true)
	if err != nil {
		t.Fatalf("GetRowsByIndexValueOrdered(ASC) failed: %v", err)
	}

	if len(pks) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(pks))
	}

	// First 2 electronics rows should be id=1 and id=3
	if len(pks) >= 2 {
		if pks[0] != "1" || pks[1] != "3" {
			t.Errorf("Expected [1, 3] ascending, got %v", pks)
		}
	}

	// Test descending order - last 2 electronics rows
	pks, err = client.GetRowsByIndexValueOrdered(ctx, "public", "test_rows_ordered", "category", "electronics", "id", 2, false)
	if err != nil {
		t.Fatalf("GetRowsByIndexValueOrdered(DESC) failed: %v", err)
	}

	if len(pks) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(pks))
	}

	// Last 2 electronics rows should be id=4 and id=3
	if len(pks) >= 2 {
		if pks[0] != "4" || pks[1] != "3" {
			t.Errorf("Expected [4, 3] descending, got %v", pks)
		}
	}

	t.Logf("Ascending: %v, Descending: %v", pks, pks)
}

// TestClient_GetDistinctValuesOrdered_NilPool verifies error on uninitialized client.
func TestClient_GetDistinctValuesOrdered_NilPool(t *testing.T) {
	client := &Client{cfg: &config.Config{}}
	ctx := context.Background()

	_, err := client.GetDistinctValuesOrdered(ctx, "public", "test", "col", 10, true)
	if err == nil {
		t.Error("Expected error for nil pool")
	}
}

// TestClient_GetRowsByIndexValueOrdered_NilPool verifies error on uninitialized client.
func TestClient_GetRowsByIndexValueOrdered_NilPool(t *testing.T) {
	client := &Client{cfg: &config.Config{}}
	ctx := context.Background()

	_, err := client.GetRowsByIndexValueOrdered(ctx, "public", "test", "col", "val", "id", 10, true)
	if err == nil {
		t.Error("Expected error for nil pool")
	}
}
