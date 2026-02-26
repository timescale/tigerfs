package fs

import (
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TestMetadataCacheConfig holds configuration for creating a pre-populated
// MetadataCache in tests. This is used by tests in external packages (e.g., fuse)
// that need a cache with known state without hitting a real database.
type TestMetadataCacheConfig struct {
	Cfg           *config.Config
	DB            db.DBClient
	DefaultSchema string

	// Catalog tier data
	Tables  []string
	Views   []string
	Schemas []string

	// Multi-schema data
	SchemaTables      map[string][]string
	SchemaViews       map[string][]string
	SchemaRowCounts   map[string]map[string]int64
	SchemaPermissions map[string]map[string]*db.TablePermissions
	SchemaLastFetch   map[string]time.Time
}

// NewTestMetadataCache creates a pre-populated MetadataCache for testing.
// The cache is initialized with the provided data and a recent lastFetch timestamp
// to prevent automatic refresh attempts.
//
// This should only be used in tests. For production code, use NewMetadataCache.
func NewTestMetadataCache(tc TestMetadataCacheConfig) *MetadataCache {
	if tc.Tables == nil {
		tc.Tables = []string{}
	}
	if tc.Views == nil {
		tc.Views = []string{}
	}
	if tc.Schemas == nil {
		tc.Schemas = []string{}
	}
	if tc.SchemaTables == nil {
		tc.SchemaTables = make(map[string][]string)
	}
	if tc.SchemaViews == nil {
		tc.SchemaViews = make(map[string][]string)
	}
	if tc.SchemaRowCounts == nil {
		tc.SchemaRowCounts = make(map[string]map[string]int64)
	}
	if tc.SchemaPermissions == nil {
		tc.SchemaPermissions = make(map[string]map[string]*db.TablePermissions)
	}
	if tc.SchemaLastFetch == nil {
		tc.SchemaLastFetch = make(map[string]time.Time)
	}

	return &MetadataCache{
		cfg:               tc.Cfg,
		db:                tc.DB,
		defaultSchema:     tc.DefaultSchema,
		tables:            tc.Tables,
		views:             tc.Views,
		schemas:           tc.Schemas,
		tableRowCounts:    make(map[string]int64),
		tablePermissions:  make(map[string]*db.TablePermissions),
		primaryKeys:       make(map[string][]string),
		pkNegatives:       make(map[string]bool),
		schemaTables:      tc.SchemaTables,
		schemaViews:       tc.SchemaViews,
		schemaRowCounts:   tc.SchemaRowCounts,
		schemaPermissions: tc.SchemaPermissions,
		schemaLastFetch:   tc.SchemaLastFetch,
		lastFetch:         time.Now(),
	}
}
