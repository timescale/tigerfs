package db

import (
	"context"
)

// DDLExecutor provides DDL execution capabilities.
// Used by control files (.test, .commit) for DDL validation and execution.
type DDLExecutor interface {
	// Exec executes a SQL statement (INSERT, UPDATE, DELETE, DDL).
	Exec(ctx context.Context, sql string, args ...interface{}) error

	// ExecInTransaction executes a SQL statement within a transaction that is
	// always rolled back. Used to validate DDL without persisting changes.
	ExecInTransaction(ctx context.Context, sql string, args ...interface{}) error
}

// SchemaReader provides schema and table metadata operations.
// Used by cache, root node, and schema navigation.
type SchemaReader interface {
	// GetCurrentSchema returns PostgreSQL's current_schema() for this connection.
	GetCurrentSchema(ctx context.Context) (string, error)

	// GetSchemas returns all user-defined schemas (excluding system schemas).
	GetSchemas(ctx context.Context) ([]string, error)

	// GetTables returns all tables in a given schema.
	GetTables(ctx context.Context, schema string) ([]string, error)

	// GetViews returns all views in a given schema.
	GetViews(ctx context.Context, schema string) ([]string, error)

	// IsViewUpdatable checks if a view supports INSERT/UPDATE/DELETE operations.
	IsViewUpdatable(ctx context.Context, schema, view string) (bool, error)

	// GetColumns returns all columns for a table in schema order.
	GetColumns(ctx context.Context, schema, table string) ([]Column, error)

	// GetPrimaryKey discovers the primary key for a table.
	GetPrimaryKey(ctx context.Context, schema, table string) (*PrimaryKey, error)

	// GetTablePermissions returns the current user's permissions on a table.
	GetTablePermissions(ctx context.Context, schema, table string) (*TablePermissions, error)

	// GetTablePermissionsBatch returns permissions for multiple tables in one query.
	// Eliminates N serial round-trips during schema metadata refresh.
	GetTablePermissionsBatch(ctx context.Context, schema string, tables []string) (map[string]*TablePermissions, error)

	// GetViewComment returns the raw comment string for a view, or empty if none.
	// Used by synthesized apps to detect format markers (e.g., "tigerfs:md").
	GetViewComment(ctx context.Context, schema, view string) (string, error)

	// GetViewCommentsBatch returns comments for all views in a schema.
	// Returns a map of view name to comment string (views without comments are omitted).
	GetViewCommentsBatch(ctx context.Context, schema string) (map[string]string, error)
}

// RowReader provides row-level read operations.
// Used by row and column file nodes.
type RowReader interface {
	// GetRow fetches a single row by primary key.
	GetRow(ctx context.Context, schema, table, pkColumn, pkValue string) (*Row, error)

	// GetColumn fetches a single column value from a row by primary key.
	GetColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error)

	// ListRows returns a list of primary key values from a table.
	ListRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)

	// ListAllRows returns all primary key values from a table without any limit.
	ListAllRows(ctx context.Context, schema, table, pkColumn string) ([]string, error)
}

// RowWriter provides row-level write operations.
// Used by row and column file nodes for inserts, updates, and deletes.
type RowWriter interface {
	// InsertRow inserts a new row with the given column values.
	// Returns the inserted primary key value (useful for auto-generated PKs).
	InsertRow(ctx context.Context, schema, table string, columns []string, values []interface{}) (string, error)

	// UpdateRow updates an existing row with the given column values.
	UpdateRow(ctx context.Context, schema, table, pkColumn, pkValue string, columns []string, values []interface{}) error

	// UpdateColumn updates a single column value for a row by primary key.
	UpdateColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error

	// UpdateColumnCAS performs a compare-and-swap update on a column.
	// Only updates if the current value of whereColumn matches whereValue.
	// Returns "row not found" error if no row matches (either PK doesn't exist
	// or the current value has already changed). This enables atomic rename:
	// two concurrent renames of the same file will have exactly one succeed.
	UpdateColumnCAS(ctx context.Context, schema, table, pkColumn, pkValue, setColumn, newValue, whereColumn, whereValue string) error

	// DeleteRow deletes a row by primary key.
	DeleteRow(ctx context.Context, schema, table, pkColumn, pkValue string) error
}

// IndexReader provides index metadata and lookup operations.
// Used by index navigation nodes (.email/, .last_name.first_name/).
type IndexReader interface {
	// GetIndexes retrieves all indexes for a table from PostgreSQL catalog.
	GetIndexes(ctx context.Context, schema, table string) ([]Index, error)

	// GetIndexByColumn finds the first index whose leading column matches.
	GetIndexByColumn(ctx context.Context, schema, table, column string) (*Index, error)

	// GetSingleColumnIndexes returns indexes with exactly one column.
	GetSingleColumnIndexes(ctx context.Context, schema, table string) ([]Index, error)

	// GetCompositeIndexes returns indexes with multiple columns.
	GetCompositeIndexes(ctx context.Context, schema, table string) ([]Index, error)

	// GetDistinctValues retrieves distinct values for an indexed column.
	GetDistinctValues(ctx context.Context, schema, table, column string, limit int) ([]string, error)

	// GetDistinctValuesOrdered retrieves distinct values with explicit ordering.
	GetDistinctValuesOrdered(ctx context.Context, schema, table, column string, limit int, ascending bool) ([]string, error)

	// GetDistinctValuesFiltered retrieves distinct values filtered by conditions.
	GetDistinctValuesFiltered(ctx context.Context, schema, table, targetColumn string, filterColumns, filterValues []string, limit int) ([]string, error)

	// GetRowsByIndexValue retrieves primary keys of rows matching an indexed column value.
	GetRowsByIndexValue(ctx context.Context, schema, table, column, value, pkColumn string, limit int) ([]string, error)

	// GetRowsByIndexValueOrdered retrieves primary keys with explicit ordering.
	GetRowsByIndexValueOrdered(ctx context.Context, schema, table, column, value, pkColumn string, limit int, ascending bool) ([]string, error)

	// GetRowsByCompositeIndex retrieves primary keys matching multiple column conditions.
	GetRowsByCompositeIndex(ctx context.Context, schema, table string, columns, values []string, pkColumn string, limit int) ([]string, error)
}

// CountReader provides row count operations.
// Used by table nodes and .count metadata file.
type CountReader interface {
	// GetRowCount returns the number of rows in a table (exact count).
	GetRowCount(ctx context.Context, schema, table string) (int64, error)

	// GetRowCountSmart returns the row count using an adaptive strategy.
	// For small tables, performs exact COUNT(*). For large tables, returns estimate.
	GetRowCountSmart(ctx context.Context, schema, table string) (int64, error)

	// GetRowCountEstimates returns fast row count estimates for multiple tables.
	GetRowCountEstimates(ctx context.Context, schema string, tables []string) (map[string]int64, error)

	// GetTableRowCountEstimate returns the estimated row count for a single table.
	// Uses pg_class.reltuples for fast estimation. Returns -1 if table not found.
	GetTableRowCountEstimate(ctx context.Context, schema, table string) (int64, error)
}

// DDLReader provides DDL generation operations.
// Used by .ddl metadata file and DDL templates.
type DDLReader interface {
	// GetTableDDL returns the CREATE TABLE statement for a table.
	GetTableDDL(ctx context.Context, schema, table string) (string, error)

	// GetFullDDL returns complete DDL for a table including indexes, constraints, etc.
	GetFullDDL(ctx context.Context, schema, table string) (string, error)

	// GetIndexDDL returns CREATE INDEX statements for a table.
	GetIndexDDL(ctx context.Context, schema, table string) (string, error)

	// GetForeignKeyDDL returns ALTER TABLE statements for foreign key constraints.
	GetForeignKeyDDL(ctx context.Context, schema, table string) (string, error)

	// GetCheckConstraintDDL returns ALTER TABLE statements for check constraints.
	GetCheckConstraintDDL(ctx context.Context, schema, table string) (string, error)

	// GetTriggerDDL returns CREATE TRIGGER statements for a table.
	GetTriggerDDL(ctx context.Context, schema, table string) (string, error)

	// GetTableComments returns COMMENT statements for table and columns.
	GetTableComments(ctx context.Context, schema, table string) (string, error)

	// GetReferencingForeignKeys returns foreign keys that reference the specified table.
	GetReferencingForeignKeys(ctx context.Context, schema, table string) ([]ForeignKeyRef, error)

	// GetSchemaTableCount returns the number of tables in a schema.
	GetSchemaTableCount(ctx context.Context, schema string) (int, error)

	// GetViewDefinition returns the SQL definition of a view.
	GetViewDefinition(ctx context.Context, schema, view string) (string, error)

	// GetDependentViews returns views that depend on the specified view or table.
	GetDependentViews(ctx context.Context, schema, name string) ([]string, error)
}

// PaginationReader provides pagination query operations.
// Used by .first/N, .last/N, .sample/N, and .order/<column>/ navigation paths.
type PaginationReader interface {
	// GetFirstNRows returns the first N primary key values ordered by PK ascending.
	GetFirstNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)

	// GetLastNRows returns the last N primary key values ordered by PK descending.
	GetLastNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)

	// GetRandomSampleRows returns approximately N random primary key values.
	GetRandomSampleRows(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error)

	// GetFirstNRowsOrdered returns the first N primary key values ordered by a specified column ascending.
	GetFirstNRowsOrdered(ctx context.Context, schema, table, pkColumn, orderColumn string, limit int) ([]string, error)

	// GetLastNRowsOrdered returns the last N primary key values ordered by a specified column descending.
	GetLastNRowsOrdered(ctx context.Context, schema, table, pkColumn, orderColumn string, limit int) ([]string, error)
}

// ExportReader provides bulk data export operations.
// Used by .export/ capability for materializing table data.
type ExportReader interface {
	// GetAllRows returns all rows from a table up to the specified limit.
	// Returns columns, rows (as [][]interface{}), and error.
	GetAllRows(ctx context.Context, schema, table string, limit int) ([]string, [][]interface{}, error)

	// GetFirstNRowsWithData returns the first N rows with full data ordered by PK ascending.
	GetFirstNRowsWithData(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error)

	// GetLastNRowsWithData returns the last N rows with full data ordered by PK descending.
	GetLastNRowsWithData(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error)

	// RowExistsByColumns checks if any row matches the given column=value conditions.
	// Generates: SELECT 1 FROM "schema"."table" WHERE "col1" = $1 AND "col2" = $2 LIMIT 1
	RowExistsByColumns(ctx context.Context, schema, table string, columns []string, values []interface{}) (bool, error)

	// GetRowByColumns returns a single row matching column=value conditions.
	// Returns all columns. Returns (nil, nil, nil) if no match.
	// Generates: SELECT * FROM "schema"."table" WHERE "col1" = $1 AND "col2" = $2 LIMIT 1
	GetRowByColumns(ctx context.Context, schema, table string, columns []string, values []interface{}) ([]string, []interface{}, error)
}

// ImportWriter provides bulk data import operations.
// Used by .import/ capability for bulk data loading.
type ImportWriter interface {
	// ImportOverwrite replaces all table data with new rows.
	// Executes TRUNCATE followed by bulk INSERT in a single transaction.
	ImportOverwrite(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error

	// ImportSync performs upsert operations for all rows.
	// Uses INSERT ... ON CONFLICT DO UPDATE. Requires table to have a primary key.
	ImportSync(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error

	// ImportAppend inserts new rows without modifying existing data.
	// Fails on primary key conflicts (duplicate key errors).
	ImportAppend(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error
}

// PipelineReader provides pipeline query operations for capability chaining.
// Used by PipelineNode to execute queries with accumulated filters, ordering, and limits.
type PipelineReader interface {
	// QueryRowsPipeline executes a pipeline query and returns primary key values.
	QueryRowsPipeline(ctx context.Context, params QueryParams) ([]string, error)

	// QueryRowsWithDataPipeline executes a pipeline query and returns full row data.
	QueryRowsWithDataPipeline(ctx context.Context, params QueryParams) ([]string, [][]interface{}, error)
}

// HistoryReader provides operations for versioned history queries.
// Used by .history/ virtual directory for reading past versions of synth app files.
type HistoryReader interface {
	// HasExtension checks if a PostgreSQL extension is installed in the database.
	HasExtension(ctx context.Context, extName string) (bool, error)

	// TableExists checks if a table exists in the given schema.
	TableExists(ctx context.Context, schema, table string) (bool, error)

	// QueryHistoryByFilename queries the history table for versions of a file by filename.
	QueryHistoryByFilename(ctx context.Context, schema, historyTable, filename string, limit int) ([]string, [][]interface{}, error)

	// QueryHistoryByID queries the history table for versions of a row by its UUID.
	QueryHistoryByID(ctx context.Context, schema, historyTable, rowID string, limit int) ([]string, [][]interface{}, error)

	// QueryHistoryDistinctFilenames returns distinct filenames from the history table.
	QueryHistoryDistinctFilenames(ctx context.Context, schema, historyTable string, limit int) ([]string, error)

	// QueryHistoryDistinctIDs returns distinct row UUIDs from the history table.
	QueryHistoryDistinctIDs(ctx context.Context, schema, historyTable string, limit int) ([]string, error)

	// QueryHistoryVersionByTime finds a history row matching a version ID timestamp.
	QueryHistoryVersionByTime(ctx context.Context, schema, historyTable, filterColumn, filterValue string, targetTime interface{}, limit int) ([]string, [][]interface{}, error)
}

// HierarchyWriter provides operations for hierarchical directory management.
// Used by synth views with filetype column for directory rename, child checks, etc.
type HierarchyWriter interface {
	// RenameByPrefix atomically renames all rows where column starts with oldPrefix.
	RenameByPrefix(ctx context.Context, schema, table, column, oldPrefix, newPrefix string) (int64, error)

	// HasChildrenWithPrefix checks if any rows have column values starting with prefix + "/".
	HasChildrenWithPrefix(ctx context.Context, schema, table, column, prefix string) (bool, error)

	// InsertIfNotExists inserts a row only if no conflict (ON CONFLICT DO NOTHING).
	InsertIfNotExists(ctx context.Context, schema, table string, columns []string, values []interface{}) error
}

// DBClient is the composite interface combining all database capabilities.
// FUSE nodes that need multiple capabilities can accept this interface.
// *db.Client satisfies this interface automatically.
type DBClient interface {
	DDLExecutor
	SchemaReader
	RowReader
	RowWriter
	IndexReader
	CountReader
	DDLReader
	PaginationReader
	ExportReader
	ImportWriter
	PipelineReader
	HierarchyWriter
	HistoryReader
}

// Compile-time verification that *Client implements DBClient
var _ DBClient = (*Client)(nil)
