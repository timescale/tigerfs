package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// GetCurrentSchema returns PostgreSQL's current_schema() for this connection.
// This is the first schema in search_path that exists and is accessible.
// Used to determine the default schema when not explicitly configured.
//
// Parameters:
//   - ctx: Context for cancellation
//   - dbtx: DBTX (pool or transaction)
//
// Returns the current schema name, or error on database failure.
func GetCurrentSchema(ctx context.Context, dbtx DBTX) (string, error) {
	logging.Debug("Querying current_schema from PostgreSQL")

	var schema string
	err := dbtx.QueryRow(ctx, "SELECT current_schema()").Scan(&schema)
	if err != nil {
		return "", fmt.Errorf("failed to query current_schema: %w", err)
	}

	logging.Debug("Got current schema", zap.String("schema", schema))
	return schema, nil
}

// GetCurrentSchema is a convenience wrapper around GetCurrentSchema for Client.
func (c *Client) GetCurrentSchema(ctx context.Context) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetCurrentSchema(ctx, q)
}

// GetSchemas returns all user-defined schemas (excluding system schemas)
func GetSchemas(ctx context.Context, dbtx DBTX) ([]string, error) {
	logging.Debug("Querying schemas from information_schema")

	query := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY schema_name
	`

	rows, err := dbtx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return nil, fmt.Errorf("failed to scan schema name: %w", err)
		}
		schemas = append(schemas, schemaName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schemas: %w", err)
	}

	logging.Debug("Found schemas", zap.Int("count", len(schemas)), zap.Strings("schemas", schemas))
	return schemas, nil
}

// GetTables returns all user tables in a given schema.
// Excludes tables owned by extensions (e.g., pg_buffercache internal tables)
// via pg_depend deptype='e'. Hypertables are NOT excluded — TimescaleDB
// tracks them in its own catalog, not pg_depend.
func GetTables(ctx context.Context, dbtx DBTX, schema string) ([]string, error) {
	logging.Debug("Querying tables from information_schema", zap.String("schema", schema))

	query := `
		SELECT t.table_name
		FROM information_schema.tables t
		WHERE t.table_schema = $1 AND t.table_type = 'BASE TABLE'
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			JOIN pg_class c ON d.objid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = t.table_schema
			AND c.relname = t.table_name
			AND d.deptype = 'e'
		)
		ORDER BY t.table_name
	`

	rows, err := dbtx.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	logging.Debug("Found tables",
		zap.String("schema", schema),
		zap.Int("count", len(tables)),
		zap.Strings("tables", tables))

	return tables, nil
}

// GetSchemasWithClient is a convenience wrapper around GetSchemas for Client
func (c *Client) GetSchemas(ctx context.Context) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetSchemas(ctx, q)
}

// GetTables is a convenience wrapper around GetTables for Client
func (c *Client) GetTables(ctx context.Context, schema string) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetTables(ctx, q, schema)
}

// GetViews returns all user views in a given schema.
// Excludes views owned by extensions (e.g., pg_buffercache, pg_buffercache_numa)
// via pg_depend deptype='e'.
func GetViews(ctx context.Context, dbtx DBTX, schema string) ([]string, error) {
	logging.Debug("Querying views from information_schema", zap.String("schema", schema))

	query := `
		SELECT t.table_name
		FROM information_schema.tables t
		WHERE t.table_schema = $1 AND t.table_type = 'VIEW'
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			JOIN pg_class c ON d.objid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = t.table_schema
			AND c.relname = t.table_name
			AND d.deptype = 'e'
		)
		ORDER BY t.table_name
	`

	rows, err := dbtx.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query views: %w", err)
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var viewName string
		if err := rows.Scan(&viewName); err != nil {
			return nil, fmt.Errorf("failed to scan view name: %w", err)
		}
		views = append(views, viewName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating views: %w", err)
	}

	logging.Debug("Found views",
		zap.String("schema", schema),
		zap.Int("count", len(views)),
		zap.Strings("views", views))

	return views, nil
}

// GetViews is a convenience wrapper around GetViews for Client
func (c *Client) GetViews(ctx context.Context, schema string) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetViews(ctx, q, schema)
}

// IsViewUpdatable checks if a view supports INSERT/UPDATE/DELETE operations.
// PostgreSQL determines this based on the view definition - simple views on
// single tables are typically updatable, while views with JOINs, aggregates,
// DISTINCT, GROUP BY, etc. are not.
func IsViewUpdatable(ctx context.Context, dbtx DBTX, schema, view string) (bool, error) {
	logging.Debug("Checking if view is updatable",
		zap.String("schema", schema),
		zap.String("view", view))

	query := `
		SELECT is_updatable
		FROM information_schema.views
		WHERE table_schema = $1 AND table_name = $2
	`

	var isUpdatable string
	err := dbtx.QueryRow(ctx, query, schema, view).Scan(&isUpdatable)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, fmt.Errorf("view %s.%s not found", schema, view)
		}
		return false, fmt.Errorf("failed to check if view is updatable: %w", err)
	}

	// information_schema returns 'YES' or 'NO'
	updatable := isUpdatable == "YES"
	logging.Debug("View updatable status",
		zap.String("schema", schema),
		zap.String("view", view),
		zap.Bool("updatable", updatable))

	return updatable, nil
}

// IsViewUpdatable is a convenience wrapper around IsViewUpdatable for Client
func (c *Client) IsViewUpdatable(ctx context.Context, schema, view string) (result bool, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return false, err
	}
	defer func() { done(retErr) }()
	return IsViewUpdatable(ctx, q, schema, view)
}

// Column represents metadata about a table column
type Column struct {
	Name       string
	DataType   string
	MaxLength  int // character_maximum_length (0 if not applicable)
	IsNullable bool
	Default    string // Column default value (empty string if no default)
}

// GetColumns returns all columns for a table in schema order.
// Fetches column_default and character_maximum_length so callers
// can generate DDL without a separate query.
func GetColumns(ctx context.Context, dbtx DBTX, schema, table string) ([]Column, error) {
	logging.Debug("Querying columns from information_schema",
		zap.String("schema", schema),
		zap.String("table", table))

	query := `
		SELECT column_name, data_type, is_nullable,
		       COALESCE(character_maximum_length, 0),
		       COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := dbtx.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var isNullableStr string
		if err := rows.Scan(&col.Name, &col.DataType, &isNullableStr, &col.MaxLength, &col.Default); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		col.IsNullable = (isNullableStr == "YES")
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	logging.Debug("Found columns",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("count", len(columns)))

	return columns, nil
}

// GetColumns is a convenience wrapper around GetColumns for Client
func (c *Client) GetColumns(ctx context.Context, schema, table string) (result []Column, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetColumns(ctx, q, schema, table)
}

// GetRowCount returns the number of rows in a table
func GetRowCount(ctx context.Context, dbtx DBTX, schema, table string) (int64, error) {
	logging.Debug("Getting row count",
		zap.String("schema", schema),
		zap.String("table", table))

	query := fmt.Sprintf(
		`SELECT COUNT(*) FROM %s`,
		qt(schema, table),
	)

	var count int64
	err := dbtx.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}

	logging.Debug("Got row count",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int64("count", count))

	return count, nil
}

// GetRowCount is a convenience wrapper around GetRowCount for Client
func (c *Client) GetRowCount(ctx context.Context, schema, table string) (result int64, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { done(retErr) }()
	return GetRowCount(ctx, q, schema, table)
}

// SmallTableThreshold is the row count below which exact COUNT(*) is used.
// For tables with more rows, pg_class.reltuples estimate is returned instead
// to avoid expensive full table scans.
const SmallTableThreshold = 100000

// GetTableRowCountEstimate returns the estimated row count for a single table from pg_class.reltuples.
// This is a fast operation that uses PostgreSQL statistics rather than scanning the table.
// The estimate is updated by VACUUM and ANALYZE operations.
//
// This is the canonical function for getting row count estimates. Other functions
// like GetRowCountSmart use this internally.
//
// Parameters:
//   - ctx: Context for cancellation
//   - dbtx: DBTX (pool or transaction)
//   - schema: PostgreSQL schema name
//   - table: Table name
//
// Returns the estimated row count, or -1 if the table is not found in pg_class.
func GetTableRowCountEstimate(ctx context.Context, dbtx DBTX, schema, table string) (int64, error) {
	logging.Debug("Getting table row count estimate",
		zap.String("schema", schema),
		zap.String("table", table))

	query := `
		SELECT reltuples::bigint
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`

	var estimate int64
	err := dbtx.QueryRow(ctx, query, schema, table).Scan(&estimate)
	if err != nil {
		if err == pgx.ErrNoRows {
			logging.Debug("Table not found in pg_class",
				zap.String("schema", schema),
				zap.String("table", table))
			return -1, nil
		}
		return 0, fmt.Errorf("failed to get row count estimate: %w", err)
	}

	logging.Debug("Got table row count estimate",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int64("estimate", estimate))

	return estimate, nil
}

// GetTableRowCountEstimate is a convenience wrapper for Client.
func (c *Client) GetTableRowCountEstimate(ctx context.Context, schema, table string) (result int64, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { done(retErr) }()
	return GetTableRowCountEstimate(ctx, q, schema, table)
}

// GetRowCountSmart returns the row count using an adaptive strategy.
// For small tables (< 100K rows estimated), performs exact COUNT(*).
// For large tables (>= 100K rows estimated), returns the pg_class.reltuples estimate.
//
// This provides accurate counts for small tables while avoiding expensive
// full table scans on large tables. The estimate from reltuples is updated
// by VACUUM and ANALYZE operations.
//
// Parameters:
//   - ctx: Context for cancellation
//   - dbtx: DBTX (pool or transaction)
//   - schema: PostgreSQL schema name
//   - table: Table name to count
//
// Returns the row count (exact or estimated), or error on database failure.
func GetRowCountSmart(ctx context.Context, dbtx DBTX, schema, table string) (int64, error) {
	logging.Debug("Getting smart row count",
		zap.String("schema", schema),
		zap.String("table", table))

	// Get the pg_class.reltuples estimate using shared helper
	estimate, err := GetTableRowCountEstimate(ctx, dbtx, schema, table)
	if err != nil {
		return 0, err
	}

	// Table not found in pg_class - fall back to exact count
	if estimate == -1 {
		logging.Debug("Table not found in pg_class, using exact count",
			zap.String("schema", schema),
			zap.String("table", table))
		return GetRowCount(ctx, dbtx, schema, table)
	}

	// For small tables, use exact count for accuracy
	if estimate < SmallTableThreshold {
		logging.Debug("Small table detected, using exact count",
			zap.String("table", table),
			zap.Int64("estimate", estimate),
			zap.Int64("threshold", SmallTableThreshold))
		return GetRowCount(ctx, dbtx, schema, table)
	}

	// For large tables, return the estimate to avoid expensive full scan
	logging.Debug("Large table detected, returning estimate",
		zap.String("table", table),
		zap.Int64("estimate", estimate),
		zap.Int64("threshold", SmallTableThreshold))
	return estimate, nil
}

// GetRowCountSmart is a convenience wrapper for Client.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schema: PostgreSQL schema name
//   - table: Table name to count
//
// Returns the row count (exact or estimated), or error on database failure.
func (c *Client) GetRowCountSmart(ctx context.Context, schema, table string) (result int64, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { done(retErr) }()
	return GetRowCountSmart(ctx, q, schema, table)
}

// GetRowCountEstimates returns fast row count estimates for multiple tables using pg_class.
// Uses reltuples from PostgreSQL statistics, avoiding full table scans.
// Returns a map of table name to estimated row count.
func GetRowCountEstimates(ctx context.Context, dbtx DBTX, schema string, tables []string) (map[string]int64, error) {
	if len(tables) == 0 {
		return make(map[string]int64), nil
	}

	logging.Debug("Getting row count estimates",
		zap.String("schema", schema),
		zap.Int("table_count", len(tables)))

	query := `
		SELECT c.relname, COALESCE(c.reltuples::bigint, 0)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = ANY($2)
	`

	rows, err := dbtx.Query(ctx, query, schema, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count estimates: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var tableName string
		var count int64
		if err := rows.Scan(&tableName, &count); err != nil {
			return nil, fmt.Errorf("failed to scan row count estimate: %w", err)
		}
		result[tableName] = count
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating row count estimates: %w", err)
	}

	logging.Debug("Got row count estimates",
		zap.String("schema", schema),
		zap.Int("table_count", len(result)))

	return result, nil
}

// GetRowCountEstimates is a convenience wrapper for Client.
func (c *Client) GetRowCountEstimates(ctx context.Context, schema string, tables []string) (result map[string]int64, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetRowCountEstimates(ctx, q, schema, tables)
}

// GetTableDDL returns the CREATE TABLE statement for a table
// Constructs DDL from information_schema
func GetTableDDL(ctx context.Context, dbtx DBTX, schema, table string) (string, error) {
	logging.Debug("Getting table DDL",
		zap.String("schema", schema),
		zap.String("table", table))

	// Get column definitions
	query := `
		SELECT
			column_name,
			data_type,
			character_maximum_length,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := dbtx.Query(ctx, query, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query column definitions: %w", err)
	}
	defer rows.Close()

	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", qt(schema, table)))

	columnDefs := []string{}
	for rows.Next() {
		var colName, dataType string
		var maxLength *int
		var isNullable, colDefault *string

		if err := rows.Scan(&colName, &dataType, &maxLength, &isNullable, &colDefault); err != nil {
			return "", fmt.Errorf("failed to scan column definition: %w", err)
		}

		colDef := fmt.Sprintf("  %s %s", qi(colName), dataType)

		// Add length for character types
		if maxLength != nil && *maxLength > 0 {
			colDef += fmt.Sprintf("(%d)", *maxLength)
		}

		// Add NOT NULL constraint
		if isNullable != nil && *isNullable == "NO" {
			colDef += " NOT NULL"
		}

		// Add DEFAULT
		if colDefault != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *colDefault)
		}

		columnDefs = append(columnDefs, colDef)
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating column definitions: %w", err)
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))

	// Get primary key constraint
	pkQuery := `
		SELECT string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position)
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
			AND tc.table_name = kcu.table_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
	`

	var pkColumns *string
	if err := dbtx.QueryRow(ctx, pkQuery, schema, table).Scan(&pkColumns); err != nil && err != pgx.ErrNoRows {
		return "", fmt.Errorf("failed to query primary key: %w", err)
	}

	if pkColumns != nil {
		ddl.WriteString(fmt.Sprintf(",\n  PRIMARY KEY (%s)", *pkColumns))
	}

	ddl.WriteString("\n);\n")

	logging.Debug("Generated table DDL",
		zap.String("schema", schema),
		zap.String("table", table))

	return ddl.String(), nil
}

// GetTableDDL is a convenience wrapper around GetTableDDL for Client
func (c *Client) GetTableDDL(ctx context.Context, schema, table string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetTableDDL(ctx, q, schema, table)
}

// FormatTableDDL generates a CREATE TABLE DDL statement from pre-fetched metadata.
// Uses cached columns and PK — no DB queries needed. This produces the same output
// as GetTableDDL but without requiring a database connection.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - columns: Column metadata (from GetColumns or MetadataCache.GetColumns)
//   - pk: Primary key info (nil if table has no PK)
//
// Returns the DDL string.
func FormatTableDDL(schema, table string, columns []Column, pk *PrimaryKey) string {
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", qt(schema, table)))

	columnDefs := make([]string, 0, len(columns))
	for _, col := range columns {
		colDef := fmt.Sprintf("  %s %s", qi(col.Name), col.DataType)

		// Add length for character types
		if col.MaxLength > 0 {
			colDef += fmt.Sprintf("(%d)", col.MaxLength)
		}

		// Add NOT NULL constraint
		if !col.IsNullable {
			colDef += " NOT NULL"
		}

		// Add DEFAULT
		if col.Default != "" {
			colDef += fmt.Sprintf(" DEFAULT %s", col.Default)
		}

		columnDefs = append(columnDefs, colDef)
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))

	if pk != nil && len(pk.Columns) > 0 {
		ddl.WriteString(fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(pk.Columns, ", ")))
	}

	ddl.WriteString("\n);\n")

	return ddl.String()
}

// GetIndexDDL returns CREATE INDEX statements for a table.
// Includes both regular and unique indexes, excluding primary key constraints
// (which are already shown in the CREATE TABLE statement).
func GetIndexDDL(ctx context.Context, dbtx DBTX, schema, table string) (string, error) {
	logging.Debug("Getting index DDL",
		zap.String("schema", schema),
		zap.String("table", table))

	// Query index definitions from pg_indexes
	// Exclude primary key indexes (they're part of CREATE TABLE)
	query := `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
		AND indexname NOT IN (
			SELECT conname FROM pg_constraint
			WHERE conrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass
			AND contype = 'p'
		)
		ORDER BY indexname
	`

	rows, err := dbtx.Query(ctx, query, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	var ddl strings.Builder
	for rows.Next() {
		var indexDef string
		if err := rows.Scan(&indexDef); err != nil {
			return "", fmt.Errorf("failed to scan index definition: %w", err)
		}
		ddl.WriteString(indexDef)
		ddl.WriteString(";\n")
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading index rows: %w", err)
	}

	return ddl.String(), nil
}

// GetIndexDDL is a convenience wrapper for Client
func (c *Client) GetIndexDDL(ctx context.Context, schema, table string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetIndexDDL(ctx, q, schema, table)
}

// GetForeignKeyDDL returns ALTER TABLE statements for foreign key constraints.
func GetForeignKeyDDL(ctx context.Context, dbtx DBTX, schema, table string) (string, error) {
	logging.Debug("Getting foreign key DDL",
		zap.String("schema", schema),
		zap.String("table", table))

	query := `
		SELECT
			'ALTER TABLE "' || nsp.nspname || '"."' || cls.relname || '" ADD CONSTRAINT "' || con.conname || '" ' ||
			pg_get_constraintdef(con.oid) || ';' AS fk_ddl
		FROM pg_constraint con
		JOIN pg_class cls ON con.conrelid = cls.oid
		JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
		WHERE nsp.nspname = $1
		AND cls.relname = $2
		AND con.contype = 'f'
		ORDER BY con.conname
	`

	rows, err := dbtx.Query(ctx, query, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	var ddl strings.Builder
	for rows.Next() {
		var fkDef string
		if err := rows.Scan(&fkDef); err != nil {
			return "", fmt.Errorf("failed to scan foreign key definition: %w", err)
		}
		ddl.WriteString(fkDef)
		ddl.WriteString("\n")
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading foreign key rows: %w", err)
	}

	return ddl.String(), nil
}

// GetForeignKeyDDL is a convenience wrapper for Client
func (c *Client) GetForeignKeyDDL(ctx context.Context, schema, table string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetForeignKeyDDL(ctx, q, schema, table)
}

// GetCheckConstraintDDL returns ALTER TABLE statements for check constraints.
func GetCheckConstraintDDL(ctx context.Context, dbtx DBTX, schema, table string) (string, error) {
	logging.Debug("Getting check constraint DDL",
		zap.String("schema", schema),
		zap.String("table", table))

	query := `
		SELECT
			'ALTER TABLE "' || nsp.nspname || '"."' || cls.relname || '" ADD CONSTRAINT "' || con.conname || '" ' ||
			pg_get_constraintdef(con.oid) || ';' AS check_ddl
		FROM pg_constraint con
		JOIN pg_class cls ON con.conrelid = cls.oid
		JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
		WHERE nsp.nspname = $1
		AND cls.relname = $2
		AND con.contype = 'c'
		ORDER BY con.conname
	`

	rows, err := dbtx.Query(ctx, query, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query check constraints: %w", err)
	}
	defer rows.Close()

	var ddl strings.Builder
	for rows.Next() {
		var checkDef string
		if err := rows.Scan(&checkDef); err != nil {
			return "", fmt.Errorf("failed to scan check constraint definition: %w", err)
		}
		ddl.WriteString(checkDef)
		ddl.WriteString("\n")
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading check constraint rows: %w", err)
	}

	return ddl.String(), nil
}

// GetCheckConstraintDDL is a convenience wrapper for Client
func (c *Client) GetCheckConstraintDDL(ctx context.Context, schema, table string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetCheckConstraintDDL(ctx, q, schema, table)
}

// GetTriggerDDL returns CREATE TRIGGER statements for a table.
func GetTriggerDDL(ctx context.Context, dbtx DBTX, schema, table string) (string, error) {
	logging.Debug("Getting trigger DDL",
		zap.String("schema", schema),
		zap.String("table", table))

	query := `
		SELECT pg_get_triggerdef(t.oid, true) || ';' AS trigger_ddl
		FROM pg_trigger t
		JOIN pg_class cls ON t.tgrelid = cls.oid
		JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
		WHERE nsp.nspname = $1
		AND cls.relname = $2
		AND NOT t.tgisinternal
		ORDER BY t.tgname
	`

	rows, err := dbtx.Query(ctx, query, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query triggers: %w", err)
	}
	defer rows.Close()

	var ddl strings.Builder
	for rows.Next() {
		var triggerDef string
		if err := rows.Scan(&triggerDef); err != nil {
			return "", fmt.Errorf("failed to scan trigger definition: %w", err)
		}
		ddl.WriteString(triggerDef)
		ddl.WriteString("\n")
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading trigger rows: %w", err)
	}

	return ddl.String(), nil
}

// GetTriggerDDL is a convenience wrapper for Client
func (c *Client) GetTriggerDDL(ctx context.Context, schema, table string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetTriggerDDL(ctx, q, schema, table)
}

// GetTableComments returns COMMENT statements for table and columns.
func GetTableComments(ctx context.Context, dbtx DBTX, schema, table string) (string, error) {
	logging.Debug("Getting table comments",
		zap.String("schema", schema),
		zap.String("table", table))

	var ddl strings.Builder

	// Get table comment
	tableCommentQuery := `
		SELECT obj_description((quote_ident($1) || '.' || quote_ident($2))::regclass, 'pg_class')
	`
	var tableComment *string
	err := dbtx.QueryRow(ctx, tableCommentQuery, schema, table).Scan(&tableComment)
	if err != nil {
		return "", fmt.Errorf("failed to query table comment: %w", err)
	}

	if tableComment != nil && *tableComment != "" {
		ddl.WriteString(fmt.Sprintf("COMMENT ON TABLE %s IS '%s';\n",
			qt(schema, table), strings.ReplaceAll(*tableComment, "'", "''")))
	}

	// Get column comments
	columnCommentQuery := `
		SELECT a.attname, d.description
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_attribute a ON a.attrelid = c.oid
		JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
		WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attnum > 0
		AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	rows, err := dbtx.Query(ctx, columnCommentQuery, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query column comments: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var colName, comment string
		if err := rows.Scan(&colName, &comment); err != nil {
			return "", fmt.Errorf("failed to scan column comment: %w", err)
		}
		ddl.WriteString(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';\n",
			qt(schema, table), qi(colName), strings.ReplaceAll(comment, "'", "''")))
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading column comment rows: %w", err)
	}

	return ddl.String(), nil
}

// GetTableComments is a convenience wrapper for Client
func (c *Client) GetTableComments(ctx context.Context, schema, table string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetTableComments(ctx, q, schema, table)
}

// GetFullDDL returns complete DDL for a table including:
// - CREATE TABLE statement
// - Indexes
// - Foreign keys
// - Check constraints
// - Triggers
// - Comments
// Sections are only included if they have content.
func GetFullDDL(ctx context.Context, dbtx DBTX, schema, table string) (string, error) {
	logging.Debug("Getting full DDL",
		zap.String("schema", schema),
		zap.String("table", table))

	var ddl strings.Builder

	// Table definition
	tableDDL, err := GetTableDDL(ctx, dbtx, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get table DDL: %w", err)
	}
	ddl.WriteString("-- Table\n")
	ddl.WriteString(tableDDL)
	ddl.WriteString("\n")

	// Indexes
	indexDDL, err := GetIndexDDL(ctx, dbtx, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get index DDL: %w", err)
	}
	if indexDDL != "" {
		ddl.WriteString("-- Indexes\n")
		ddl.WriteString(indexDDL)
		ddl.WriteString("\n")
	}

	// Foreign keys
	fkDDL, err := GetForeignKeyDDL(ctx, dbtx, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get foreign key DDL: %w", err)
	}
	if fkDDL != "" {
		ddl.WriteString("-- Foreign Keys\n")
		ddl.WriteString(fkDDL)
		ddl.WriteString("\n")
	}

	// Check constraints
	checkDDL, err := GetCheckConstraintDDL(ctx, dbtx, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get check constraint DDL: %w", err)
	}
	if checkDDL != "" {
		ddl.WriteString("-- Check Constraints\n")
		ddl.WriteString(checkDDL)
		ddl.WriteString("\n")
	}

	// Triggers
	triggerDDL, err := GetTriggerDDL(ctx, dbtx, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get trigger DDL: %w", err)
	}
	if triggerDDL != "" {
		ddl.WriteString("-- Triggers\n")
		ddl.WriteString(triggerDDL)
		ddl.WriteString("\n")
	}

	// Comments
	commentsDDL, err := GetTableComments(ctx, dbtx, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get table comments: %w", err)
	}
	if commentsDDL != "" {
		ddl.WriteString("-- Comments\n")
		ddl.WriteString(commentsDDL)
	}

	return ddl.String(), nil
}

// GetFullDDL is a convenience wrapper for Client
func (c *Client) GetFullDDL(ctx context.Context, schema, table string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetFullDDL(ctx, q, schema, table)
}

// ForeignKeyRef represents a foreign key that references a table.
// Used to show dependencies when generating delete templates.
type ForeignKeyRef struct {
	// ConstraintName is the name of the foreign key constraint
	ConstraintName string
	// SourceSchema is the schema of the table containing the FK
	SourceSchema string
	// SourceTable is the table containing the FK
	SourceTable string
	// SourceColumns are the columns in the source table
	SourceColumns []string
	// TargetColumns are the columns in the target (referenced) table
	TargetColumns []string
	// EstimatedRows is the approximate row count in the source table
	EstimatedRows int64
}

// GetReferencingForeignKeys returns foreign keys that reference the specified table.
// This is the reverse of GetForeignKeyDDL - it finds tables that depend on this table.
func GetReferencingForeignKeys(ctx context.Context, dbtx DBTX, schema, table string) ([]ForeignKeyRef, error) {
	logging.Debug("Getting referencing foreign keys",
		zap.String("schema", schema),
		zap.String("table", table))

	query := `
		SELECT
			con.conname AS constraint_name,
			src_nsp.nspname AS source_schema,
			src_cls.relname AS source_table,
			array_agg(src_att.attname ORDER BY src_att.attnum) AS source_columns,
			array_agg(tgt_att.attname ORDER BY tgt_att.attnum) AS target_columns,
			COALESCE(src_cls.reltuples::bigint, 0) AS estimated_rows
		FROM pg_constraint con
		JOIN pg_class src_cls ON con.conrelid = src_cls.oid
		JOIN pg_namespace src_nsp ON src_cls.relnamespace = src_nsp.oid
		JOIN pg_class tgt_cls ON con.confrelid = tgt_cls.oid
		JOIN pg_namespace tgt_nsp ON tgt_cls.relnamespace = tgt_nsp.oid
		JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS src_cols(attnum, ord) ON true
		JOIN pg_attribute src_att ON src_att.attrelid = src_cls.oid AND src_att.attnum = src_cols.attnum
		JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS tgt_cols(attnum, ord) ON src_cols.ord = tgt_cols.ord
		JOIN pg_attribute tgt_att ON tgt_att.attrelid = tgt_cls.oid AND tgt_att.attnum = tgt_cols.attnum
		WHERE tgt_nsp.nspname = $1
		AND tgt_cls.relname = $2
		AND con.contype = 'f'
		GROUP BY con.conname, src_nsp.nspname, src_cls.relname, src_cls.reltuples
		ORDER BY src_nsp.nspname, src_cls.relname, con.conname
	`

	rows, err := dbtx.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query referencing foreign keys: %w", err)
	}
	defer rows.Close()

	var refs []ForeignKeyRef
	for rows.Next() {
		var ref ForeignKeyRef
		if err := rows.Scan(
			&ref.ConstraintName,
			&ref.SourceSchema,
			&ref.SourceTable,
			&ref.SourceColumns,
			&ref.TargetColumns,
			&ref.EstimatedRows,
		); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key ref: %w", err)
		}
		refs = append(refs, ref)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading foreign key refs: %w", err)
	}

	logging.Debug("Found referencing foreign keys",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("count", len(refs)))

	return refs, nil
}

// GetReferencingForeignKeys is a convenience wrapper for Client
func (c *Client) GetReferencingForeignKeys(ctx context.Context, schema, table string) (result []ForeignKeyRef, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetReferencingForeignKeys(ctx, q, schema, table)
}

// GetSchemaTableCount returns the number of tables in a schema.
// Used to determine if a schema is empty when generating delete templates.
func GetSchemaTableCount(ctx context.Context, dbtx DBTX, schema string) (int, error) {
	logging.Debug("Getting schema table count",
		zap.String("schema", schema))

	query := `
		SELECT COUNT(*)
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1
		AND c.relkind = 'r'
	`

	var count int
	err := dbtx.QueryRow(ctx, query, schema).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema table count: %w", err)
	}

	return count, nil
}

// GetSchemaTableCount is a convenience wrapper for Client
func (c *Client) GetSchemaTableCount(ctx context.Context, schema string) (result int, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { done(retErr) }()
	return GetSchemaTableCount(ctx, q, schema)
}

// GetViewComment returns the raw comment string for a view.
// Returns empty string if the view has no comment.
// Used to detect synthesized format markers (e.g., "tigerfs:md").
func GetViewComment(ctx context.Context, dbtx DBTX, schema, view string) (string, error) {
	logging.Debug("Getting view comment",
		zap.String("schema", schema),
		zap.String("view", view))

	query := `
		SELECT COALESCE(obj_description(c.oid, 'pg_class'), '')
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1
		AND c.relname = $2
		AND c.relkind = 'v'
	`

	var comment string
	err := dbtx.QueryRow(ctx, query, schema, view).Scan(&comment)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("failed to get view comment: %w", err)
	}

	return comment, nil
}

// GetViewComment is a convenience wrapper for Client.
func (c *Client) GetViewComment(ctx context.Context, schema, view string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetViewComment(ctx, q, schema, view)
}

// GetViewCommentsBatch returns comments for all views in a schema.
// Returns a map of view name to comment string. Views without comments are omitted.
func GetViewCommentsBatch(ctx context.Context, dbtx DBTX, schema string) (map[string]string, error) {
	logging.Debug("Getting view comments batch",
		zap.String("schema", schema))

	query := `
		SELECT c.relname, obj_description(c.oid, 'pg_class')
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1
		AND c.relkind = 'v'
		AND obj_description(c.oid, 'pg_class') IS NOT NULL
	`

	rows, err := dbtx.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query view comments: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var viewName, comment string
		if err := rows.Scan(&viewName, &comment); err != nil {
			return nil, fmt.Errorf("failed to scan view comment: %w", err)
		}
		if comment != "" {
			result[viewName] = comment
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating view comments: %w", err)
	}

	logging.Debug("Got view comments",
		zap.String("schema", schema),
		zap.Int("count", len(result)))

	return result, nil
}

// GetViewCommentsBatch is a convenience wrapper for Client.
func (c *Client) GetViewCommentsBatch(ctx context.Context, schema string) (result map[string]string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetViewCommentsBatch(ctx, q, schema)
}

// GetViewDefinition returns the SQL definition of a view.
func GetViewDefinition(ctx context.Context, dbtx DBTX, schema, view string) (string, error) {
	logging.Debug("Getting view definition",
		zap.String("schema", schema),
		zap.String("view", view))

	query := `
		SELECT pg_get_viewdef(c.oid, true)
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1
		AND c.relname = $2
		AND c.relkind = 'v'
	`

	var definition string
	err := dbtx.QueryRow(ctx, query, schema, view).Scan(&definition)
	if err != nil {
		return "", fmt.Errorf("failed to get view definition: %w", err)
	}

	return definition, nil
}

// GetViewDefinition is a convenience wrapper for Client
func (c *Client) GetViewDefinition(ctx context.Context, schema, view string) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return GetViewDefinition(ctx, q, schema, view)
}

// GetDependentViews returns views that depend on the specified view or table.
func GetDependentViews(ctx context.Context, dbtx DBTX, schema, name string) ([]string, error) {
	logging.Debug("Getting dependent views",
		zap.String("schema", schema),
		zap.String("name", name))

	query := `
		SELECT DISTINCT dep_cls.relname
		FROM pg_depend d
		JOIN pg_class ref_cls ON d.refobjid = ref_cls.oid
		JOIN pg_namespace ref_nsp ON ref_cls.relnamespace = ref_nsp.oid
		JOIN pg_class dep_cls ON d.objid = dep_cls.oid
		WHERE ref_nsp.nspname = $1
		AND ref_cls.relname = $2
		AND dep_cls.relkind = 'v'
		AND d.deptype = 'n'
		ORDER BY dep_cls.relname
	`

	rows, err := dbtx.Query(ctx, query, schema, name)
	if err != nil {
		return nil, fmt.Errorf("failed to query dependent views: %w", err)
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var viewName string
		if err := rows.Scan(&viewName); err != nil {
			return nil, fmt.Errorf("failed to scan dependent view: %w", err)
		}
		views = append(views, viewName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading dependent views: %w", err)
	}

	return views, nil
}

// GetDependentViews is a convenience wrapper for Client
func (c *Client) GetDependentViews(ctx context.Context, schema, name string) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetDependentViews(ctx, q, schema, name)
}
