package fs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// MetadataCache caches database metadata (schemas, tables, views, columns, permissions)
// to avoid excessive queries to information_schema and system catalogs.
//
// Cache is split into two tiers with independent TTLs:
//   - Catalog tier (fast): schemas, tables, views — refreshed every MetadataRefreshInterval (default 10s)
//   - Structural tier (slow): row counts, permissions, primary keys — refreshed every StructuralMetadataRefreshInterval (default 5m)
//
// Explicit Invalidate() resets both tiers, causing a full refresh on next access.
type MetadataCache struct {
	mu sync.RWMutex

	cfg *config.Config // TigerFS configuration
	db  db.DBClient    // Database client for queries

	// Catalog tier — refreshed on fast TTL (MetadataRefreshInterval)
	lastFetch time.Time // When catalog was last refreshed

	// Structural tier — refreshed on slow TTL (StructuralMetadataRefreshInterval)
	structuralLastFetch time.Time // When structural data was last refreshed

	// Primary key cache — uses slow TTL (structural metadata).
	// Key: "schema\x00table"
	pkMu        sync.RWMutex
	primaryKeys map[string][]string // cached PK column names
	pkNegatives map[string]bool     // tables known to have no PK (views, tables without PK)

	// Column cache — columns don't change during a mount session.
	// Key: "schema\x00table"
	colMu   sync.RWMutex
	columns map[string][]db.Column

	// defaultSchema is the PostgreSQL schema for root-level tables.
	// Inherited from PostgreSQL's current_schema() if not explicitly configured.
	defaultSchema string

	// Multi-schema support (for .schemas/ directory)
	schemas           []string                                   // All user-defined schemas
	schemaTables      map[string][]string                        // schema -> table names
	schemaViews       map[string][]string                        // schema -> view names
	schemaRowCounts   map[string]map[string]int64                // schema -> table -> count
	schemaPermissions map[string]map[string]*db.TablePermissions // schema -> table -> permissions
	schemaLastFetch   map[string]time.Time                       // schema -> last fetch time
}

// NewMetadataCache creates a new metadata cache.
//
// Parameters:
//   - cfg: TigerFS configuration (determines refresh intervals)
//   - dbClient: Database client for queries (accepts db.DBClient interface)
//
// Returns an empty cache that will be populated on first access.
// If cfg.DefaultSchema is empty, it will be resolved from PostgreSQL's
// current_schema() on first refresh.
func NewMetadataCache(cfg *config.Config, dbClient db.DBClient) *MetadataCache {
	return &MetadataCache{
		cfg:               cfg,
		db:                dbClient,
		defaultSchema:     cfg.DefaultSchema,
		primaryKeys:       make(map[string][]string),
		pkNegatives:       make(map[string]bool),
		columns:           make(map[string][]db.Column),
		schemas:           []string{},
		schemaTables:      make(map[string][]string),
		schemaViews:       make(map[string][]string),
		schemaRowCounts:   make(map[string]map[string]int64),
		schemaPermissions: make(map[string]map[string]*db.TablePermissions),
		schemaLastFetch:   make(map[string]time.Time),
	}
}

// pkKey returns the cache key for a schema+table pair.
func pkKey(schema, table string) string {
	return schema + "\x00" + table
}

// GetTables returns the list of tables from the default schema.
// Uses cached data if still fresh, otherwise refreshes from database.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns a copy of the table list to prevent external modification.
// Returns error if the cache refresh fails.
func (c *MetadataCache) GetTables(ctx context.Context) ([]string, error) {
	if err := c.ensureCatalogFresh(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	schema := c.defaultSchema
	c.mu.RUnlock()

	return c.GetTablesForSchema(ctx, schema)
}

// GetViews returns the list of views from the default schema.
// Uses cached data if still fresh, otherwise refreshes from database.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns a copy of the view list to prevent external modification.
// Returns error if the cache refresh fails.
func (c *MetadataCache) GetViews(ctx context.Context) ([]string, error) {
	if err := c.ensureCatalogFresh(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	schema := c.defaultSchema
	c.mu.RUnlock()

	return c.GetViewsForSchema(ctx, schema)
}

// HasView checks if a view exists in the cache.
// Refreshes the cache if stale before checking.
//
// Parameters:
//   - ctx: Context for cancellation
//   - viewName: Name of the view to check
//
// Returns true if the view exists, false otherwise.
// Returns error if the cache refresh fails.
func (c *MetadataCache) HasView(ctx context.Context, viewName string) (bool, error) {
	views, err := c.GetViews(ctx)
	if err != nil {
		return false, err
	}

	for _, view := range views {
		if view == viewName {
			return true, nil
		}
	}
	return false, nil
}

// HasTable checks if a table exists in the cache.
// Refreshes the cache if stale before checking.
//
// Parameters:
//   - ctx: Context for cancellation
//   - tableName: Name of the table to check
//
// Returns true if the table exists, false otherwise.
// Returns error if the cache refresh fails.
func (c *MetadataCache) HasTable(ctx context.Context, tableName string) (bool, error) {
	tables, err := c.GetTables(ctx)
	if err != nil {
		return false, err
	}

	for _, table := range tables {
		if table == tableName {
			return true, nil
		}
	}
	return false, nil
}

// HasTableOrView checks if a name exists as a table or view in the default schema.
// Returns (isTable, isView, err). At most one of isTable/isView is true.
// Refreshes the catalog cache if stale.
func (c *MetadataCache) HasTableOrView(ctx context.Context, name string) (isTable, isView bool, err error) {
	c.mu.RLock()
	schema := c.defaultSchema
	c.mu.RUnlock()

	return c.HasTableOrViewInSchema(ctx, schema, name)
}

// HasTableOrViewInSchema checks if a name exists as a table or view in a specific schema.
// Lazy-loads schema data on first access.
func (c *MetadataCache) HasTableOrViewInSchema(ctx context.Context, schema, name string) (isTable, isView bool, err error) {
	tables, err := c.GetTablesForSchema(ctx, schema)
	if err != nil {
		return false, false, err
	}
	for _, t := range tables {
		if t == name {
			return true, false, nil
		}
	}
	views, err := c.GetViewsForSchema(ctx, schema)
	if err != nil {
		return false, false, err
	}
	for _, v := range views {
		if v == name {
			return false, true, nil
		}
	}
	return false, false, nil
}

// GetPrimaryKey returns the cached primary key columns for a table.
// On cache miss, queries the database and caches the result.
// Tables without a primary key (views, etc.) are cached as negatives
// to avoid repeated DB queries.
//
// Uses the structural TTL for expiration.
func (c *MetadataCache) GetPrimaryKey(ctx context.Context, schema, table string) (*db.PrimaryKey, error) {
	key := pkKey(schema, table)

	c.pkMu.RLock()
	if cols, ok := c.primaryKeys[key]; ok {
		c.pkMu.RUnlock()
		return &db.PrimaryKey{Columns: cols}, nil
	}
	if c.pkNegatives[key] {
		c.pkMu.RUnlock()
		return nil, fmt.Errorf("table %s.%s has no primary key (cached)", schema, table)
	}
	c.pkMu.RUnlock()

	// Cache miss — query the DB
	pk, err := c.db.GetPrimaryKey(ctx, schema, table)
	if err != nil {
		// Cache the negative result
		c.pkMu.Lock()
		c.pkNegatives[key] = true
		c.pkMu.Unlock()
		return nil, err
	}

	// Cache the positive result
	c.pkMu.Lock()
	c.primaryKeys[key] = pk.Columns
	c.pkMu.Unlock()

	return pk, nil
}

// GetColumns returns the cached column metadata for a table.
// On cache miss, queries the database and caches the result.
// Column metadata doesn't change during a mount session.
func (c *MetadataCache) GetColumns(ctx context.Context, schema, table string) ([]db.Column, error) {
	key := pkKey(schema, table)

	c.colMu.RLock()
	if cols, ok := c.columns[key]; ok {
		c.colMu.RUnlock()
		return cols, nil
	}
	c.colMu.RUnlock()

	// Cache miss — query the DB
	cols, err := c.db.GetColumns(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	c.colMu.Lock()
	c.columns[key] = cols
	c.colMu.Unlock()

	return cols, nil
}

// InvalidatePrimaryKey removes a specific PK entry from the cache.
// Call after DDL that might change a table's primary key.
func (c *MetadataCache) InvalidatePrimaryKey(schema, table string) {
	key := pkKey(schema, table)
	c.pkMu.Lock()
	delete(c.primaryKeys, key)
	delete(c.pkNegatives, key)
	c.pkMu.Unlock()
}

// ensureCatalogFresh checks if the catalog tier needs refresh and refreshes if needed.
func (c *MetadataCache) ensureCatalogFresh(ctx context.Context) error {
	c.mu.RLock()
	needsRefresh := c.needsCatalogRefresh()
	c.mu.RUnlock()

	if needsRefresh {
		logging.Debug("Metadata cache catalog expired, refreshing")
		return c.RefreshCatalog(ctx)
	}
	return nil
}

// ensureStructuralFresh checks if the structural tier needs refresh and refreshes if needed.
func (c *MetadataCache) ensureStructuralFresh(ctx context.Context) error {
	c.mu.RLock()
	needsRefresh := c.needsStructuralRefresh()
	c.mu.RUnlock()

	if needsRefresh {
		logging.Debug("Metadata cache structural data expired, refreshing")
		return c.RefreshStructural(ctx)
	}
	return nil
}

// needsCatalogRefresh checks if the catalog tier has expired.
// IMPORTANT: Must be called with at least read lock held.
func (c *MetadataCache) needsCatalogRefresh() bool {
	if c.lastFetch.IsZero() {
		return true
	}
	return time.Since(c.lastFetch) > c.cfg.MetadataRefreshInterval
}

// needsStructuralRefresh checks if the structural tier has expired.
// IMPORTANT: Must be called with at least read lock held.
func (c *MetadataCache) needsStructuralRefresh() bool {
	if c.structuralLastFetch.IsZero() {
		return true
	}
	interval := c.cfg.StructuralMetadataRefreshInterval
	if interval == 0 {
		interval = 5 * time.Minute // fallback default
	}
	return time.Since(c.structuralLastFetch) > interval
}

// RefreshCatalog refreshes the catalog tier: schemas, tables, views.
// This is the fast tier, called frequently (default every 10s).
func (c *MetadataCache) RefreshCatalog(ctx context.Context) error {
	// Resolve default schema from PostgreSQL if not explicitly configured
	c.mu.RLock()
	needsSchemaResolution := c.defaultSchema == ""
	c.mu.RUnlock()

	if needsSchemaResolution {
		currentSchema, err := c.db.GetCurrentSchema(ctx)
		if err != nil {
			logging.Warn("Failed to get current_schema, defaulting to 'public'",
				zap.Error(err))
			currentSchema = "public"
		}
		c.mu.Lock()
		c.defaultSchema = currentSchema
		c.mu.Unlock()
		logging.Info("Resolved default schema from PostgreSQL",
			zap.String("schema", currentSchema))
	}

	logging.Debug("Refreshing metadata cache (catalog)")

	// Query all schemas
	schemas, err := c.db.GetSchemas(ctx)
	if err != nil {
		logging.Warn("Failed to get schemas, continuing without",
			zap.Error(err))
		schemas = []string{}
	}

	c.mu.Lock()
	c.schemas = schemas
	c.lastFetch = time.Now()
	c.mu.Unlock()

	// Clear PK negatives on catalog refresh — a previously-view-only name might now be a table.
	c.pkMu.Lock()
	c.pkNegatives = make(map[string]bool)
	c.pkMu.Unlock()

	logging.Debug("Metadata cache catalog refreshed",
		zap.Int("schema_count", len(schemas)))

	return nil
}

// fetchSchemaStructural fetches row counts and permissions for a set of tables.
// Shared by RefreshStructural (default schema) and RefreshSchema (other schemas).
func (c *MetadataCache) fetchSchemaStructural(ctx context.Context, schema string, tables []string) (map[string]int64, map[string]*db.TablePermissions) {
	// Query row count estimates for all tables in one batch
	rowCounts, err := c.db.GetRowCountEstimates(ctx, schema, tables)
	if err != nil {
		logging.Warn("Failed to get row count estimates, continuing without",
			zap.String("schema", schema),
			zap.Error(err))
		rowCounts = make(map[string]int64)
	}

	// Query permissions for all tables in one batch
	permissions, err := c.db.GetTablePermissionsBatch(ctx, schema, tables)
	if err != nil {
		logging.Warn("Failed to get batch permissions, continuing without",
			zap.String("schema", schema),
			zap.Error(err))
		permissions = make(map[string]*db.TablePermissions)
	}

	return rowCounts, permissions
}

// RefreshStructural refreshes the structural tier: row counts, permissions.
// This is the slow tier, called infrequently (default every 5m).
func (c *MetadataCache) RefreshStructural(ctx context.Context) error {
	// Ensure catalog is fresh first (need table list for structural queries)
	if err := c.ensureCatalogFresh(ctx); err != nil {
		return err
	}

	c.mu.RLock()
	schema := c.defaultSchema
	c.mu.RUnlock()

	// Ensure default schema tables are loaded (on-demand via RefreshSchema)
	tables, err := c.GetTablesForSchema(ctx, schema)
	if err != nil {
		return err
	}

	logging.Debug("Refreshing metadata cache (structural)", zap.String("schema", schema))

	rowCounts, permissions := c.fetchSchemaStructural(ctx, schema, tables)

	c.mu.Lock()
	c.schemaRowCounts[schema] = rowCounts
	c.schemaPermissions[schema] = permissions
	c.structuralLastFetch = time.Now()
	c.mu.Unlock()

	logging.Debug("Metadata cache structural refreshed",
		zap.Int("permissions_count", len(permissions)))

	return nil
}

// Refresh updates all tiers from the database.
// Called by Invalidate() to force a full refresh.
func (c *MetadataCache) Refresh(ctx context.Context) error {
	if err := c.RefreshCatalog(ctx); err != nil {
		return err
	}
	return c.RefreshStructural(ctx)
}

// GetRowCountEstimate returns the cached row count estimate for a table.
// The estimate comes from pg_class.reltuples, which is updated by VACUUM/ANALYZE.
//
// Parameters:
//   - ctx: Context for cancellation (used if cache needs refresh)
//   - tableName: Name of the table to get count for
//
// Returns the estimated row count, or -1 if the table is not in cache.
// Callers should treat -1 as "unknown" and may fall back to exact count.
func (c *MetadataCache) GetRowCountEstimate(ctx context.Context, tableName string) (int64, error) {
	if err := c.ensureStructuralFresh(ctx); err != nil {
		return -1, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if counts, ok := c.schemaRowCounts[c.defaultSchema]; ok {
		if count, ok := counts[tableName]; ok {
			return count, nil
		}
	}
	return -1, nil
}

// GetTablePermissions returns the cached permissions for a table.
// Returns nil if the table is not in cache or permissions weren't fetched.
//
// Parameters:
//   - ctx: Context for cancellation (used if cache needs refresh)
//   - tableName: Name of the table to get permissions for
//
// Returns the cached permissions, or nil if not available.
func (c *MetadataCache) GetTablePermissions(ctx context.Context, tableName string) (*db.TablePermissions, error) {
	if err := c.ensureStructuralFresh(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if perms, ok := c.schemaPermissions[c.defaultSchema]; ok {
		if p, ok := perms[tableName]; ok {
			return p, nil
		}
	}
	return nil, nil
}

// GetDefaultSchema returns the resolved default schema name.
// If not yet resolved (e.g., cache not refreshed), returns empty string.
func (c *MetadataCache) GetDefaultSchema() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.defaultSchema
}

// GetSchemas returns the list of all user-defined schemas.
// Uses cached data if still fresh, otherwise refreshes from database.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns a copy of the schema list to prevent external modification.
func (c *MetadataCache) GetSchemas(ctx context.Context) ([]string, error) {
	if err := c.ensureCatalogFresh(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	schemasCopy := make([]string, len(c.schemas))
	copy(schemasCopy, c.schemas)
	return schemasCopy, nil
}

// HasSchema checks if a schema exists in the cache.
// Refreshes the cache if stale before checking.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schemaName: Name of the schema to check
//
// Returns true if the schema exists, false otherwise.
func (c *MetadataCache) HasSchema(ctx context.Context, schemaName string) (bool, error) {
	schemas, err := c.GetSchemas(ctx)
	if err != nil {
		return false, err
	}

	for _, schema := range schemas {
		if schema == schemaName {
			return true, nil
		}
	}

	return false, nil
}

// GetTablesForSchema returns the list of tables for a specific schema.
// Lazy-loads and caches on first access, using the schema-specific cache.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schemaName: Name of the schema to get tables for
//
// Returns a copy of the table list to prevent external modification.
func (c *MetadataCache) GetTablesForSchema(ctx context.Context, schemaName string) ([]string, error) {
	if err := c.ensureCatalogFresh(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	// Check if we have cached data for this schema
	if tables, ok := c.schemaTables[schemaName]; ok {
		lastFetch := c.schemaLastFetch[schemaName]
		age := time.Since(lastFetch)
		if age <= c.cfg.MetadataRefreshInterval {
			tablesCopy := make([]string, len(tables))
			copy(tablesCopy, tables)
			c.mu.RUnlock()
			return tablesCopy, nil
		}
	}
	c.mu.RUnlock()

	// Lazy-load tables for this schema
	return c.RefreshSchema(ctx, schemaName)
}

// GetViewsForSchema returns the list of views for a specific schema.
// Lazy-loads and caches on first access, using the schema-specific cache.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schemaName: Name of the schema to get views for
//
// Returns a copy of the view list to prevent external modification.
func (c *MetadataCache) GetViewsForSchema(ctx context.Context, schemaName string) ([]string, error) {
	if err := c.ensureCatalogFresh(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	// Check if we have cached data for this schema
	if views, ok := c.schemaViews[schemaName]; ok {
		lastFetch := c.schemaLastFetch[schemaName]
		age := time.Since(lastFetch)
		if age <= c.cfg.MetadataRefreshInterval {
			viewsCopy := make([]string, len(views))
			copy(viewsCopy, views)
			c.mu.RUnlock()
			return viewsCopy, nil
		}
	}
	c.mu.RUnlock()

	// Lazy-load data for this schema (also loads views)
	_, err := c.RefreshSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	// Now return the views
	c.mu.RLock()
	defer c.mu.RUnlock()
	views := c.schemaViews[schemaName]
	viewsCopy := make([]string, len(views))
	copy(viewsCopy, views)
	return viewsCopy, nil
}

// RefreshSchema loads (or refreshes) cached data for a specific schema.
// Called automatically by GetTablesForSchema when cache is stale.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schemaName: Name of the schema to refresh
//
// Returns the table list for the schema.
func (c *MetadataCache) RefreshSchema(ctx context.Context, schemaName string) ([]string, error) {
	logging.Debug("Refreshing schema cache", zap.String("schema", schemaName))

	// Query tables for this schema
	tables, err := c.db.GetTables(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	// Query views for this schema
	views, err := c.db.GetViews(ctx, schemaName)
	if err != nil {
		logging.Warn("Failed to get views for schema, continuing without",
			zap.String("schema", schemaName),
			zap.Error(err))
		views = []string{}
	}

	// Fetch row counts + permissions in batch (shared with RefreshStructural)
	rowCounts, permissions := c.fetchSchemaStructural(ctx, schemaName, tables)

	c.mu.Lock()
	c.schemaTables[schemaName] = tables
	c.schemaViews[schemaName] = views
	c.schemaRowCounts[schemaName] = rowCounts
	c.schemaPermissions[schemaName] = permissions
	c.schemaLastFetch[schemaName] = time.Now()
	c.mu.Unlock()

	logging.Debug("Schema cache refreshed",
		zap.String("schema", schemaName),
		zap.Int("table_count", len(tables)),
		zap.Int("view_count", len(views)))

	// Return a copy
	tablesCopy := make([]string, len(tables))
	copy(tablesCopy, tables)
	return tablesCopy, nil
}

// GetRowCountEstimateForSchema returns the cached row count estimate for a table in a specific schema.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schemaName: Name of the schema
//   - tableName: Name of the table
//
// Returns the estimated row count, or -1 if not in cache.
func (c *MetadataCache) GetRowCountEstimateForSchema(ctx context.Context, schemaName, tableName string) (int64, error) {
	// Ensure schema data is loaded
	_, err := c.GetTablesForSchema(ctx, schemaName)
	if err != nil {
		return -1, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if counts, ok := c.schemaRowCounts[schemaName]; ok {
		if count, ok := counts[tableName]; ok {
			return count, nil
		}
	}
	return -1, nil
}

// GetTablePermissionsForSchema returns the cached permissions for a table in a specific schema.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schemaName: Name of the schema
//   - tableName: Name of the table
//
// Returns the cached permissions, or nil if not available.
func (c *MetadataCache) GetTablePermissionsForSchema(ctx context.Context, schemaName, tableName string) (*db.TablePermissions, error) {
	// Ensure schema data is loaded
	_, err := c.GetTablesForSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if perms, ok := c.schemaPermissions[schemaName]; ok {
		if p, ok := perms[tableName]; ok {
			return p, nil
		}
	}
	return nil, nil
}

// Invalidate clears the cache, forcing a refresh on next access.
// Call this when you know metadata has changed (e.g., after DDL operations).
func (c *MetadataCache) Invalidate() {
	c.mu.Lock()
	logging.Debug("Invalidating metadata cache")
	c.lastFetch = time.Time{}
	c.structuralLastFetch = time.Time{}

	// Clear all schema caches (includes default schema)
	c.schemas = []string{}
	c.schemaTables = make(map[string][]string)
	c.schemaViews = make(map[string][]string)
	c.schemaRowCounts = make(map[string]map[string]int64)
	c.schemaPermissions = make(map[string]map[string]*db.TablePermissions)
	c.schemaLastFetch = make(map[string]time.Time)
	c.mu.Unlock()

	// Clear PK caches
	c.pkMu.Lock()
	c.primaryKeys = make(map[string][]string)
	c.pkNegatives = make(map[string]bool)
	c.pkMu.Unlock()

	// Clear column cache
	c.colMu.Lock()
	c.columns = make(map[string][]db.Column)
	c.colMu.Unlock()
}
