package fs

import (
	"context"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

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
	if !cache.needsCatalogRefresh() {
		t.Error("Expected needsCatalogRefresh=true for new cache")
	}

	// Set lastFetch to now
	cache.lastFetch = time.Now()

	// Should not need refresh yet
	if cache.needsCatalogRefresh() {
		t.Error("Expected needsCatalogRefresh=false immediately after fetch")
	}

	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)

	// Should need refresh now
	if !cache.needsCatalogRefresh() {
		t.Error("Expected needsCatalogRefresh=true after interval expired")
	}
}

func TestMetadataCache_TwoTierRefresh(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:                     "public",
		MetadataRefreshInterval:           100 * time.Millisecond,
		StructuralMetadataRefreshInterval: 500 * time.Millisecond,
	}

	cache := NewMetadataCache(cfg, nil)

	// Both tiers need refresh initially
	if !cache.needsCatalogRefresh() {
		t.Error("Expected catalog to need refresh initially")
	}
	if !cache.needsStructuralRefresh() {
		t.Error("Expected structural to need refresh initially")
	}

	// Set both timestamps
	now := time.Now()
	cache.lastFetch = now
	cache.structuralLastFetch = now

	// Neither should need refresh immediately
	if cache.needsCatalogRefresh() {
		t.Error("Expected catalog to not need refresh immediately")
	}
	if cache.needsStructuralRefresh() {
		t.Error("Expected structural to not need refresh immediately")
	}

	// Wait for catalog to expire but not structural
	time.Sleep(150 * time.Millisecond)

	if !cache.needsCatalogRefresh() {
		t.Error("Expected catalog to need refresh after 150ms")
	}
	if cache.needsStructuralRefresh() {
		t.Error("Expected structural to NOT need refresh after 150ms")
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
	cache.structuralLastFetch = time.Now()

	// Add PK cache data
	cache.primaryKeys["public\x00table1"] = []string{"id"}
	cache.pkNegatives["public\x00view1"] = true

	// Invalidate
	cache.Invalidate()

	// Check data is cleared
	if len(cache.tables) != 0 {
		t.Error("Expected tables to be empty after invalidate")
	}

	if !cache.lastFetch.IsZero() {
		t.Error("Expected lastFetch to be zero after invalidate")
	}

	if !cache.structuralLastFetch.IsZero() {
		t.Error("Expected structuralLastFetch to be zero after invalidate")
	}

	if len(cache.primaryKeys) != 0 {
		t.Error("Expected primaryKeys to be empty after invalidate")
	}

	if len(cache.pkNegatives) != 0 {
		t.Error("Expected pkNegatives to be empty after invalidate")
	}
}

// ============================================================================
// Mock-based tests (ADR-004)
// ============================================================================

func TestMetadataCache_GetTables_WithMock(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		if schema == "public" {
			return []string{"users", "orders", "products"}, nil
		}
		return []string{}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	tables, err := cache.GetTables(context.Background())
	if err != nil {
		t.Fatalf("GetTables() error: %v", err)
	}

	if len(tables) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(tables))
	}

	// Verify caching - second call should use cache
	tables2, err := cache.GetTables(context.Background())
	if err != nil {
		t.Fatalf("Second GetTables() error: %v", err)
	}

	if len(tables2) != 3 {
		t.Errorf("Expected 3 tables from cache, got %d", len(tables2))
	}
}

func TestMetadataCache_GetTables_WithMock_Error(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		return nil, context.DeadlineExceeded
	}

	cache := NewMetadataCache(cfg, mock)

	_, err := cache.GetTables(context.Background())
	if err == nil {
		t.Error("Expected error from GetTables()")
	}
}

func TestMetadataCache_HasTable_WithMock(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		return []string{"users", "orders"}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	exists, err := cache.HasTable(context.Background(), "users")
	if err != nil {
		t.Fatalf("HasTable() error: %v", err)
	}
	if !exists {
		t.Error("Expected HasTable('users')=true")
	}

	exists, err = cache.HasTable(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("HasTable() error: %v", err)
	}
	if exists {
		t.Error("Expected HasTable('nonexistent')=false")
	}
}

func TestMetadataCache_HasTableOrView_WithMock(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		return []string{"users", "orders"}, nil
	}
	mock.MockSchemaReader.GetViewsFunc = func(ctx context.Context, schema string) ([]string, error) {
		return []string{"user_summary"}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	// Test table
	isTable, isView, err := cache.HasTableOrView(context.Background(), "users")
	if err != nil {
		t.Fatalf("HasTableOrView error: %v", err)
	}
	if !isTable || isView {
		t.Errorf("Expected isTable=true, isView=false for 'users', got %v, %v", isTable, isView)
	}

	// Test view
	isTable, isView, err = cache.HasTableOrView(context.Background(), "user_summary")
	if err != nil {
		t.Fatalf("HasTableOrView error: %v", err)
	}
	if isTable || !isView {
		t.Errorf("Expected isTable=false, isView=true for 'user_summary', got %v, %v", isTable, isView)
	}

	// Test nonexistent
	isTable, isView, err = cache.HasTableOrView(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("HasTableOrView error: %v", err)
	}
	if isTable || isView {
		t.Errorf("Expected isTable=false, isView=false for 'nonexistent', got %v, %v", isTable, isView)
	}
}

func TestMetadataCache_GetPrimaryKey_WithMock(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:                     "public",
		MetadataRefreshInterval:           30 * time.Second,
		StructuralMetadataRefreshInterval: 5 * time.Minute,
	}

	queryCount := 0
	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		queryCount++
		if table == "users" {
			return &db.PrimaryKey{Columns: []string{"id"}}, nil
		}
		return nil, context.DeadlineExceeded
	}

	cache := NewMetadataCache(cfg, mock)

	// First call should query DB
	pk, err := cache.GetPrimaryKey(context.Background(), "public", "users")
	if err != nil {
		t.Fatalf("GetPrimaryKey error: %v", err)
	}
	if len(pk.Columns) != 1 || pk.Columns[0] != "id" {
		t.Errorf("Expected PK [id], got %v", pk.Columns)
	}
	if queryCount != 1 {
		t.Errorf("Expected 1 DB query, got %d", queryCount)
	}

	// Second call should use cache
	pk, err = cache.GetPrimaryKey(context.Background(), "public", "users")
	if err != nil {
		t.Fatalf("Second GetPrimaryKey error: %v", err)
	}
	if queryCount != 1 {
		t.Errorf("Expected still 1 DB query (cached), got %d", queryCount)
	}
}

func TestMetadataCache_GetPrimaryKey_Negative(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:                     "public",
		MetadataRefreshInterval:           30 * time.Second,
		StructuralMetadataRefreshInterval: 5 * time.Minute,
	}

	queryCount := 0
	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		queryCount++
		return nil, context.DeadlineExceeded
	}

	cache := NewMetadataCache(cfg, mock)

	// First call should query DB and get error
	_, err := cache.GetPrimaryKey(context.Background(), "public", "view1")
	if err == nil {
		t.Fatal("Expected error for view without PK")
	}
	if queryCount != 1 {
		t.Errorf("Expected 1 DB query, got %d", queryCount)
	}

	// Second call should use cached negative
	_, err = cache.GetPrimaryKey(context.Background(), "public", "view1")
	if err == nil {
		t.Fatal("Expected cached error for view without PK")
	}
	if queryCount != 1 {
		t.Errorf("Expected still 1 DB query (negative cached), got %d", queryCount)
	}
}

func TestMetadataCache_InvalidatePrimaryKey(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:                     "public",
		MetadataRefreshInterval:           30 * time.Second,
		StructuralMetadataRefreshInterval: 5 * time.Minute,
	}

	queryCount := 0
	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetPrimaryKeyFunc = func(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
		queryCount++
		return &db.PrimaryKey{Columns: []string{"id"}}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	// Populate cache
	_, _ = cache.GetPrimaryKey(context.Background(), "public", "users")
	if queryCount != 1 {
		t.Fatalf("Expected 1 query, got %d", queryCount)
	}

	// Invalidate specific key
	cache.InvalidatePrimaryKey("public", "users")

	// Should query DB again
	_, _ = cache.GetPrimaryKey(context.Background(), "public", "users")
	if queryCount != 2 {
		t.Errorf("Expected 2 queries after invalidation, got %d", queryCount)
	}
}

func TestMetadataCache_CatalogRefreshClearsPKNegatives(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		return []string{"users"}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	// Set a PK negative
	cache.pkNegatives["public\x00view1"] = true

	// Refresh catalog
	err := cache.RefreshCatalog(context.Background())
	if err != nil {
		t.Fatalf("RefreshCatalog error: %v", err)
	}

	// PK negatives should be cleared
	if len(cache.pkNegatives) != 0 {
		t.Error("Expected pkNegatives to be cleared after catalog refresh")
	}
}

func TestMetadataCache_GetSchemas_WithMock(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:           "public",
		MetadataRefreshInterval: 30 * time.Second,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetSchemasFunc = func(ctx context.Context) ([]string, error) {
		return []string{"public", "analytics", "staging"}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	schemas, err := cache.GetSchemas(context.Background())
	if err != nil {
		t.Fatalf("GetSchemas() error: %v", err)
	}

	if len(schemas) != 3 {
		t.Errorf("Expected 3 schemas, got %d", len(schemas))
	}
}

func TestMetadataCache_Refresh_WithMock(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:                     "",
		MetadataRefreshInterval:           30 * time.Second,
		StructuralMetadataRefreshInterval: 5 * time.Minute,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetCurrentSchemaFunc = func(ctx context.Context) (string, error) {
		return "myschema", nil
	}
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		return []string{"table1", "table2"}, nil
	}
	mock.MockCountReader.GetRowCountEstimatesFunc = func(ctx context.Context, schema string, tables []string) (map[string]int64, error) {
		return map[string]int64{"table1": 100, "table2": 200}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	err := cache.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh() error: %v", err)
	}

	// Check that default schema was resolved
	if cache.GetDefaultSchema() != "myschema" {
		t.Errorf("Expected defaultSchema='myschema', got %q", cache.GetDefaultSchema())
	}

	// Check that tables were fetched
	if len(cache.tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(cache.tables))
	}

	// Check that lastFetch was updated
	if cache.lastFetch.IsZero() {
		t.Error("Expected lastFetch to be set")
	}

	// Check that structuralLastFetch was updated
	if cache.structuralLastFetch.IsZero() {
		t.Error("Expected structuralLastFetch to be set")
	}
}

func TestMetadataCache_GetRowCountEstimate_WithMock(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:                     "public",
		MetadataRefreshInterval:           30 * time.Second,
		StructuralMetadataRefreshInterval: 5 * time.Minute,
	}

	mock := db.NewMockDBClient()
	mock.MockCountReader.GetRowCountEstimatesFunc = func(ctx context.Context, schema string, tables []string) (map[string]int64, error) {
		return map[string]int64{
			"users":  1000,
			"orders": 50000,
		}, nil
	}
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		return []string{"users", "orders"}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	// Trigger cache population via full Refresh
	_ = cache.Refresh(context.Background())

	count, err := cache.GetRowCountEstimate(context.Background(), "users")
	if err != nil {
		t.Fatalf("GetRowCountEstimate('users') error: %v", err)
	}
	if count != 1000 {
		t.Errorf("Expected count=1000, got %d", count)
	}

	count, err = cache.GetRowCountEstimate(context.Background(), "orders")
	if err != nil {
		t.Fatalf("GetRowCountEstimate('orders') error: %v", err)
	}
	if count != 50000 {
		t.Errorf("Expected count=50000, got %d", count)
	}
}

func TestMetadataCache_GetTablePermissions_WithMock(t *testing.T) {
	cfg := &config.Config{
		DefaultSchema:                     "public",
		MetadataRefreshInterval:           30 * time.Second,
		StructuralMetadataRefreshInterval: 5 * time.Minute,
	}

	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetTablesFunc = func(ctx context.Context, schema string) ([]string, error) {
		return []string{"users", "other"}, nil
	}
	mock.MockSchemaReader.GetTablePermissionsFunc = func(ctx context.Context, schema, table string) (*db.TablePermissions, error) {
		if table == "users" {
			return &db.TablePermissions{
				CanSelect: true,
				CanInsert: true,
				CanUpdate: true,
				CanDelete: false,
			}, nil
		}
		return &db.TablePermissions{
			CanSelect: true,
			CanInsert: false,
			CanUpdate: false,
			CanDelete: false,
		}, nil
	}

	cache := NewMetadataCache(cfg, mock)

	// Trigger full refresh
	_ = cache.Refresh(context.Background())

	perms, err := cache.GetTablePermissions(context.Background(), "users")
	if err != nil {
		t.Fatalf("GetTablePermissions('users') error: %v", err)
	}
	if perms == nil {
		t.Fatal("Expected non-nil permissions")
	}
	if !perms.CanSelect || !perms.CanInsert || !perms.CanUpdate || perms.CanDelete {
		t.Errorf("Unexpected permissions: %+v", perms)
	}
}

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
