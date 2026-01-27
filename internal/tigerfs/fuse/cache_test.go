package fuse

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func getTestConnectionString(t *testing.T) string {
	t.Helper()

	if host := os.Getenv("PGHOST"); host != "" {
		return "postgres://localhost/postgres?sslmode=disable"
	}

	if connStr := os.Getenv("TEST_DATABASE_URL"); connStr != "" {
		return connStr
	}

	return ""
}

func TestNewMetadataCache(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	cache := NewMetadataCache(cfg, nil)

	if cache.cfg != cfg {
		t.Error("Expected config to be set")
	}

	if cache.defaultSchema != "public" {
		t.Errorf("Expected defaultSchema='public', got '%s'", cache.defaultSchema)
	}

	if len(cache.tables) != 0 {
		t.Error("Expected tables to be empty initially")
	}

	if !cache.lastFetch.IsZero() {
		t.Error("Expected lastFetch to be zero initially")
	}
}

func TestMetadataCache_NeedsRefresh(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 1 * time.Second,
	}

	cache := NewMetadataCache(cfg, nil)

	// Initially needs refresh
	if !cache.needsRefresh() {
		t.Error("Expected needsRefresh=true for new cache")
	}

	// Set lastFetch to now
	cache.lastFetch = time.Now()

	// Should not need refresh yet
	if cache.needsRefresh() {
		t.Error("Expected needsRefresh=false immediately after fetch")
	}

	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)

	// Should need refresh now
	if !cache.needsRefresh() {
		t.Error("Expected needsRefresh=true after interval expired")
	}
}

func TestMetadataCache_Invalidate(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	cache := NewMetadataCache(cfg, nil)

	// Set some data
	cache.tables = []string{"table1", "table2"}
	cache.lastFetch = time.Now()

	// Invalidate
	cache.Invalidate()

	// Check data is cleared
	if len(cache.tables) != 0 {
		t.Error("Expected tables to be empty after invalidate")
	}

	if !cache.lastFetch.IsZero() {
		t.Error("Expected lastFetch to be zero after invalidate")
	}

	if !cache.needsRefresh() {
		t.Error("Expected needsRefresh=true after invalidate")
	}
}

func TestMetadataCache_GetTables(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	client, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create a test table
	_, err = client.Query(ctx, `
		CREATE TABLE IF NOT EXISTS test_cache_table (
			id serial PRIMARY KEY
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_ = client.Exec(context.Background(), "DROP TABLE IF EXISTS test_cache_table")
	}()

	// Create cache
	cache := NewMetadataCache(cfg, client)

	// First call should fetch from database
	tables, err := cache.GetTables(ctx)
	if err != nil {
		t.Fatalf("GetTables() failed: %v", err)
	}

	// Should find our test table
	found := false
	for _, table := range tables {
		if table == "test_cache_table" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected to find test_cache_table, got: %v", tables)
	}

	// Second call should use cache (no database query)
	tables2, err := cache.GetTables(ctx)
	if err != nil {
		t.Fatalf("GetTables() failed on second call: %v", err)
	}

	// Should have same results
	if len(tables) != len(tables2) {
		t.Errorf("Expected same table count, got %d vs %d", len(tables), len(tables2))
	}

	t.Logf("Found %d tables: %v", len(tables), tables)
}

func TestMetadataCache_HasTable(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	client, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create a test table
	_, err = client.Query(ctx, `
		CREATE TABLE IF NOT EXISTS test_has_table (
			id serial PRIMARY KEY
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_ = client.Exec(context.Background(), "DROP TABLE IF EXISTS test_has_table")
	}()

	// Create cache
	cache := NewMetadataCache(cfg, client)

	// Check existing table
	exists, err := cache.HasTable(ctx, "test_has_table")
	if err != nil {
		t.Fatalf("HasTable() failed: %v", err)
	}

	if !exists {
		t.Error("Expected HasTable('test_has_table')=true")
	}

	// Check nonexistent table
	exists, err = cache.HasTable(ctx, "nonexistent_table")
	if err != nil {
		t.Fatalf("HasTable() failed: %v", err)
	}

	if exists {
		t.Error("Expected HasTable('nonexistent_table')=false")
	}
}

func TestMetadataCache_Refresh(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	client, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create cache
	cache := NewMetadataCache(cfg, client)

	// Refresh cache
	err = cache.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh() failed: %v", err)
	}

	// Check lastFetch was updated
	if cache.lastFetch.IsZero() {
		t.Error("Expected lastFetch to be set after Refresh()")
	}

	// Check we have some data
	cache.mu.RLock()
	tableCount := len(cache.tables)
	cache.mu.RUnlock()

	t.Logf("Cache contains %d tables after refresh", tableCount)
}

// TestMetadataCache_GetSchemas tests getting the list of all schemas
func TestMetadataCache_GetSchemas(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	client, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create cache
	cache := NewMetadataCache(cfg, client)

	// Get schemas
	schemas, err := cache.GetSchemas(ctx)
	if err != nil {
		t.Fatalf("GetSchemas() failed: %v", err)
	}

	// Should have at least 'public' schema
	foundPublic := false
	for _, schema := range schemas {
		if schema == "public" {
			foundPublic = true
			break
		}
	}

	if !foundPublic {
		t.Errorf("Expected to find 'public' schema, got: %v", schemas)
	}

	t.Logf("Found %d schemas: %v", len(schemas), schemas)
}

// TestMetadataCache_HasSchema tests checking if a schema exists
func TestMetadataCache_HasSchema(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	client, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create cache
	cache := NewMetadataCache(cfg, client)

	// Check 'public' schema exists
	exists, err := cache.HasSchema(ctx, "public")
	if err != nil {
		t.Fatalf("HasSchema() failed: %v", err)
	}

	if !exists {
		t.Error("Expected HasSchema('public')=true")
	}

	// Check nonexistent schema
	exists, err = cache.HasSchema(ctx, "nonexistent_schema_xyz")
	if err != nil {
		t.Fatalf("HasSchema() failed: %v", err)
	}

	if exists {
		t.Error("Expected HasSchema('nonexistent_schema_xyz')=false")
	}
}

// TestMetadataCache_GetTablesForSchema tests getting tables for a specific schema
func TestMetadataCache_GetTablesForSchema(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	client, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create a test table
	_, err = client.Query(ctx, `
		CREATE TABLE IF NOT EXISTS test_schema_table (
			id serial PRIMARY KEY
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_ = client.Exec(context.Background(), "DROP TABLE IF EXISTS test_schema_table")
	}()

	// Create cache
	cache := NewMetadataCache(cfg, client)

	// Get tables for public schema
	tables, err := cache.GetTablesForSchema(ctx, "public")
	if err != nil {
		t.Fatalf("GetTablesForSchema() failed: %v", err)
	}

	// Should find our test table
	found := false
	for _, table := range tables {
		if table == "test_schema_table" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected to find test_schema_table in public schema, got: %v", tables)
	}

	t.Logf("Found %d tables in public schema", len(tables))
}

// TestMetadataCache_CurrentSchema tests that empty defaultSchema resolves from PostgreSQL
func TestMetadataCache_CurrentSchema(t *testing.T) {
	connStr := getTestConnectionString(t)
	if connStr == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "", // Empty - should be resolved from PostgreSQL
		MetadataRefreshInterval: 30 * time.Second,
	}

	client, err := db.NewClient(ctx, cfg, connStr)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Create cache with empty defaultSchema
	cache := NewMetadataCache(cfg, client)

	// Before refresh, defaultSchema should be empty
	if cache.GetDefaultSchema() != "" {
		t.Errorf("Expected empty default schema before refresh, got '%s'", cache.GetDefaultSchema())
	}

	// Refresh should resolve the default schema from PostgreSQL
	err = cache.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh() failed: %v", err)
	}

	// After refresh, defaultSchema should be resolved (typically 'public')
	resolvedSchema := cache.GetDefaultSchema()
	if resolvedSchema == "" {
		t.Error("Expected defaultSchema to be resolved after refresh")
	}

	t.Logf("Resolved default schema from PostgreSQL: '%s'", resolvedSchema)
}

// TestMetadataCache_GetDefaultSchema tests the GetDefaultSchema method
func TestMetadataCache_GetDefaultSchema(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "myschema",
		MetadataRefreshInterval: 30 * time.Second,
	}

	cache := NewMetadataCache(cfg, nil)

	if cache.GetDefaultSchema() != "myschema" {
		t.Errorf("Expected 'myschema', got '%s'", cache.GetDefaultSchema())
	}
}

// TestMetadataCache_InvalidateSchemas tests that Invalidate clears schema caches
func TestMetadataCache_InvalidateSchemas(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	cache := NewMetadataCache(cfg, nil)

	// Pre-populate schema caches
	cache.schemas = []string{"public", "analytics"}
	cache.schemaTables["analytics"] = []string{"reports"}
	cache.schemaLastFetch["analytics"] = time.Now()

	// Invalidate
	cache.Invalidate()

	// Check schema data is cleared
	if len(cache.schemas) != 0 {
		t.Error("Expected schemas to be empty after invalidate")
	}

	if len(cache.schemaTables) != 0 {
		t.Error("Expected schemaTables to be empty after invalidate")
	}

	if len(cache.schemaLastFetch) != 0 {
		t.Error("Expected schemaLastFetch to be empty after invalidate")
	}
}
