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
	db  *db.Client     // Database client for queries

	// Cached data - all refreshed together
	tables           []string                       // List of table names in default schema
	tableRowCounts   map[string]int64               // table name -> estimated row count
	tablePermissions map[string]*db.TablePermissions // table name -> user permissions
	lastFetch        time.Time                      // When cache was last refreshed

	// defaultSchema is the PostgreSQL schema to cache (typically "public").
	// Used for single-schema mode where tables are shown at root level.
	defaultSchema string
}

// NewMetadataCache creates a new metadata cache.
//
// Parameters:
//   - cfg: TigerFS configuration (determines refresh interval)
//   - dbClient: Database client for queries
//
// Returns an empty cache that will be populated on first access.
func NewMetadataCache(cfg *config.Config, dbClient *db.Client) *MetadataCache {
	return &MetadataCache{
		cfg:              cfg,
		db:               dbClient,
		defaultSchema:    cfg.DefaultSchema,
		tables:           []string{},
		tableRowCounts:   make(map[string]int64),
		tablePermissions: make(map[string]*db.TablePermissions),
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
// Fetches tables, row count estimates, and permissions for all tables.
// Non-critical data (row counts, permissions) failures are logged but don't
// cause the refresh to fail, ensuring the cache remains usable.
//
// Returns error only if the table list query fails.
func (c *MetadataCache) Refresh(ctx context.Context) error {
	logging.Debug("Refreshing metadata cache", zap.String("schema", c.defaultSchema))

	// Query tables from default schema
	tables, err := c.db.GetTables(ctx, c.defaultSchema)
	if err != nil {
		return err
	}

	// Query row count estimates for all tables
	rowCounts, err := c.db.GetRowCountEstimates(ctx, c.defaultSchema, tables)
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
		perms, err := c.db.GetTablePermissions(ctx, c.defaultSchema, table)
		if err != nil {
			logging.Warn("Failed to get permissions for table, continuing without",
				zap.String("table", table),
				zap.Error(err))
			continue
		}
		permissions[table] = perms
	}

	c.mu.Lock()
	c.tables = tables
	c.tableRowCounts = rowCounts
	c.tablePermissions = permissions
	c.lastFetch = time.Now()
	c.mu.Unlock()

	logging.Debug("Metadata cache refreshed",
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
}
