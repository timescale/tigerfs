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
