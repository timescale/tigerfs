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
	SchemaTables              map[string][]string
	SchemaViews               map[string][]string
	SchemaRowCounts           map[string]map[string]int64
	SchemaPermissions         map[string]map[string]*db.TablePermissions
	SchemaLastFetch           map[string]time.Time
	SchemaStructuralLastFetch map[string]time.Time
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
	if tc.SchemaStructuralLastFetch == nil {
		tc.SchemaStructuralLastFetch = make(map[string]time.Time)
	}

	// Store Tables/Views in schema-specific maps for the default schema.
	// This unifies default and non-default schema handling.
	// Always populate, even for empty slices — prevents fallthrough to RefreshSchema
	// which would need a real DB client.
	defaultSchema := tc.DefaultSchema
	if _, ok := tc.SchemaTables[defaultSchema]; !ok {
		tc.SchemaTables[defaultSchema] = tc.Tables
	}
	if _, ok := tc.SchemaViews[defaultSchema]; !ok {
		tc.SchemaViews[defaultSchema] = tc.Views
	}
	if _, ok := tc.SchemaLastFetch[defaultSchema]; !ok {
		tc.SchemaLastFetch[defaultSchema] = time.Now()
	}
	if _, ok := tc.SchemaStructuralLastFetch[defaultSchema]; !ok {
		tc.SchemaStructuralLastFetch[defaultSchema] = time.Now()
	}

	return &MetadataCache{
		cfg:                       tc.Cfg,
		db:                        tc.DB,
		defaultSchema:             defaultSchema,
		schemas:                   tc.Schemas,
		primaryKeys:               make(map[string][]string),
		pkNegatives:               make(map[string]bool),
		columns:                   make(map[string][]db.Column),
		schemaTables:              tc.SchemaTables,
		schemaViews:               tc.SchemaViews,
		schemaRowCounts:           tc.SchemaRowCounts,
		schemaPermissions:         tc.SchemaPermissions,
		schemaLastFetch:           tc.SchemaLastFetch,
		schemaStructuralLastFetch: tc.SchemaStructuralLastFetch,
		lastFetch:                 time.Now(),
		structuralLastFetch:       time.Now(),
	}
}
