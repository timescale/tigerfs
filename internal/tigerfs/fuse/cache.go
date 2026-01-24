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

// MetadataCache caches database metadata (schemas, tables, columns)
// to avoid excessive queries to information_schema
type MetadataCache struct {
	mu sync.RWMutex

	cfg *config.Config
	db  *db.Client

	// Cached data
	tables    []string
	lastFetch time.Time

	// Default schema for flattening (typically "public")
	defaultSchema string
}

// NewMetadataCache creates a new metadata cache
func NewMetadataCache(cfg *config.Config, dbClient *db.Client) *MetadataCache {
	return &MetadataCache{
		cfg:           cfg,
		db:            dbClient,
		defaultSchema: cfg.DefaultSchema,
		tables:        []string{},
	}
}

// GetTables returns the list of tables from the default schema
// Uses cached data if still fresh, otherwise refreshes from database
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

// Refresh updates the cache from the database
func (c *MetadataCache) Refresh(ctx context.Context) error {
	logging.Debug("Refreshing metadata cache", zap.String("schema", c.defaultSchema))

	// Query tables from default schema
	tables, err := c.db.GetTables(ctx, c.defaultSchema)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.tables = tables
	c.lastFetch = time.Now()
	c.mu.Unlock()

	logging.Debug("Metadata cache refreshed",
		zap.Int("table_count", len(tables)),
		zap.Strings("tables", tables))

	return nil
}

// HasTable checks if a table exists in the cache
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

// needsRefresh checks if the cache needs to be refreshed
// Must be called with at least read lock held
func (c *MetadataCache) needsRefresh() bool {
	// If never fetched, needs refresh
	if c.lastFetch.IsZero() {
		return true
	}

	// Check if cache has expired
	age := time.Since(c.lastFetch)
	return age > c.cfg.MetadataRefreshInterval
}

// Invalidate clears the cache, forcing a refresh on next access
func (c *MetadataCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	logging.Debug("Invalidating metadata cache")
	c.tables = []string{}
	c.lastFetch = time.Time{}
}
