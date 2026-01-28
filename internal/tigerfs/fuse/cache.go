package fuse

import (
	"context"
	"sync"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// MetadataCache caches database metadata (schemas, tables, columns, permissions)
// to avoid excessive queries to information_schema and system catalogs.
// All cached data is refreshed together based on MetadataRefreshInterval.
type MetadataCache struct {
	mu sync.RWMutex

	cfg *config.Config // TigerFS configuration
	db  db.DBClient    // Database client for queries

	// Cached data - all refreshed together
	tables           []string                        // List of table names in default schema
	tableRowCounts   map[string]int64                // table name -> estimated row count
	tablePermissions map[string]*db.TablePermissions // table name -> user permissions
	lastFetch        time.Time                       // When cache was last refreshed

	// defaultSchema is the PostgreSQL schema for root-level tables.
	// Inherited from PostgreSQL's current_schema() if not explicitly configured.
	defaultSchema string

	// Multi-schema support (for .schemas/ directory)
	schemas           []string                                   // All user-defined schemas
	schemaTables      map[string][]string                        // schema -> table names
	schemaRowCounts   map[string]map[string]int64                // schema -> table -> count
	schemaPermissions map[string]map[string]*db.TablePermissions // schema -> table -> permissions
	schemaLastFetch   map[string]time.Time                       // schema -> last fetch time
}

// NewMetadataCache creates a new metadata cache.
//
// Parameters:
//   - cfg: TigerFS configuration (determines refresh interval)
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
		tables:            []string{},
		tableRowCounts:    make(map[string]int64),
		tablePermissions:  make(map[string]*db.TablePermissions),
		schemas:           []string{},
		schemaTables:      make(map[string][]string),
		schemaRowCounts:   make(map[string]map[string]int64),
		schemaPermissions: make(map[string]map[string]*db.TablePermissions),
		schemaLastFetch:   make(map[string]time.Time),
	}
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
	c.mu.RLock()
	needsRefresh := c.needsRefresh()
	c.mu.RUnlock()

	if needsRefresh {
		logging.Debug("Metadata cache expired, refreshing")
		if err := c.Refresh(ctx); err != nil {
			return nil, err
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy to prevent external modification
	tablesCopy := make([]string, len(c.tables))
	copy(tablesCopy, c.tables)

	return tablesCopy, nil
}

// Refresh updates the cache from the database.
// Fetches schemas, tables, row count estimates, and permissions.
// Non-critical data (row counts, permissions) failures are logged but don't
// cause the refresh to fail, ensuring the cache remains usable.
//
// If defaultSchema is empty, queries PostgreSQL's current_schema() to determine it.
//
// Returns error only if critical queries (schemas, tables) fail.
func (c *MetadataCache) Refresh(ctx context.Context) error {
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

	c.mu.RLock()
	schema := c.defaultSchema
	c.mu.RUnlock()

	logging.Debug("Refreshing metadata cache", zap.String("schema", schema))

	// Query all schemas
	schemas, err := c.db.GetSchemas(ctx)
	if err != nil {
		logging.Warn("Failed to get schemas, continuing without",
			zap.Error(err))
		schemas = []string{}
	}

	// Query tables from default schema
	tables, err := c.db.GetTables(ctx, schema)
	if err != nil {
		return err
	}

	// Query row count estimates for all tables
	rowCounts, err := c.db.GetRowCountEstimates(ctx, schema, tables)
	if err != nil {
		// Log warning but don't fail - row counts are optional
		logging.Warn("Failed to get row count estimates, continuing without",
			zap.Error(err))
		rowCounts = make(map[string]int64)
	}

	// Query permissions for all tables
	// Done individually since there's no batch API for has_table_privilege
	permissions := make(map[string]*db.TablePermissions)
	for _, table := range tables {
		perms, err := c.db.GetTablePermissions(ctx, schema, table)
		if err != nil {
			logging.Warn("Failed to get permissions for table, continuing without",
				zap.String("table", table),
				zap.Error(err))
			continue
		}
		permissions[table] = perms
	}

	c.mu.Lock()
	c.schemas = schemas
	c.tables = tables
	c.tableRowCounts = rowCounts
	c.tablePermissions = permissions
	c.lastFetch = time.Now()
	c.mu.Unlock()

	logging.Debug("Metadata cache refreshed",
		zap.Int("schema_count", len(schemas)),
		zap.Int("table_count", len(tables)),
		zap.Int("permissions_count", len(permissions)),
		zap.Strings("tables", tables))

	return nil
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

// needsRefresh checks if the cache needs to be refreshed.
// Returns true if the cache has never been populated or has expired
// based on MetadataRefreshInterval config setting.
//
// IMPORTANT: Must be called with at least read lock held.
func (c *MetadataCache) needsRefresh() bool {
	// If never fetched, needs refresh
	if c.lastFetch.IsZero() {
		return true
	}

	// Check if cache has expired
	age := time.Since(c.lastFetch)
	return age > c.cfg.MetadataRefreshInterval
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
	// Ensure cache is fresh
	c.mu.RLock()
	needsRefresh := c.needsRefresh()
	c.mu.RUnlock()

	if needsRefresh {
		if err := c.Refresh(ctx); err != nil {
			return -1, err
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if count, ok := c.tableRowCounts[tableName]; ok {
		return count, nil
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
	// Ensure cache is fresh
	c.mu.RLock()
	needsRefresh := c.needsRefresh()
	c.mu.RUnlock()

	if needsRefresh {
		if err := c.Refresh(ctx); err != nil {
			return nil, err
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if perms, ok := c.tablePermissions[tableName]; ok {
		return perms, nil
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
	c.mu.RLock()
	needsRefresh := c.needsRefresh()
	c.mu.RUnlock()

	if needsRefresh {
		logging.Debug("Metadata cache expired, refreshing for schemas")
		if err := c.Refresh(ctx); err != nil {
			return nil, err
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy to prevent external modification
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
// For the default schema, returns the cached tables.
// For other schemas, lazy-loads and caches on first access.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schemaName: Name of the schema to get tables for
//
// Returns a copy of the table list to prevent external modification.
func (c *MetadataCache) GetTablesForSchema(ctx context.Context, schemaName string) ([]string, error) {
	// Ensure main cache is fresh
	c.mu.RLock()
	needsRefresh := c.needsRefresh()
	defaultSchema := c.defaultSchema
	c.mu.RUnlock()

	if needsRefresh {
		if err := c.Refresh(ctx); err != nil {
			return nil, err
		}
	}

	// For default schema, return cached tables
	c.mu.RLock()
	if schemaName == defaultSchema {
		tablesCopy := make([]string, len(c.tables))
		copy(tablesCopy, c.tables)
		c.mu.RUnlock()
		return tablesCopy, nil
	}

	// For other schemas, check if we have cached data
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

	// Query row count estimates
	rowCounts, err := c.db.GetRowCountEstimates(ctx, schemaName, tables)
	if err != nil {
		logging.Warn("Failed to get row count estimates for schema",
			zap.String("schema", schemaName),
			zap.Error(err))
		rowCounts = make(map[string]int64)
	}

	// Query permissions for all tables
	permissions := make(map[string]*db.TablePermissions)
	for _, table := range tables {
		perms, err := c.db.GetTablePermissions(ctx, schemaName, table)
		if err != nil {
			logging.Warn("Failed to get permissions for table in schema",
				zap.String("schema", schemaName),
				zap.String("table", table),
				zap.Error(err))
			continue
		}
		permissions[table] = perms
	}

	c.mu.Lock()
	c.schemaTables[schemaName] = tables
	c.schemaRowCounts[schemaName] = rowCounts
	c.schemaPermissions[schemaName] = permissions
	c.schemaLastFetch[schemaName] = time.Now()
	c.mu.Unlock()

	logging.Debug("Schema cache refreshed",
		zap.String("schema", schemaName),
		zap.Int("table_count", len(tables)))

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

	// Check if this is the default schema
	if schemaName == c.defaultSchema {
		if count, ok := c.tableRowCounts[tableName]; ok {
			return count, nil
		}
		return -1, nil
	}

	// Check schema-specific cache
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

	// Check if this is the default schema
	if schemaName == c.defaultSchema {
		if perms, ok := c.tablePermissions[tableName]; ok {
			return perms, nil
		}
		return nil, nil
	}

	// Check schema-specific cache
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
	defer c.mu.Unlock()

	logging.Debug("Invalidating metadata cache")
	c.tables = []string{}
	c.tableRowCounts = make(map[string]int64)
	c.tablePermissions = make(map[string]*db.TablePermissions)
	c.lastFetch = time.Time{}

	// Clear multi-schema caches
	c.schemas = []string{}
	c.schemaTables = make(map[string][]string)
	c.schemaRowCounts = make(map[string]map[string]int64)
	c.schemaPermissions = make(map[string]map[string]*db.TablePermissions)
	c.schemaLastFetch = make(map[string]time.Time)
}
