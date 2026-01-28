package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// GetCurrentSchema returns PostgreSQL's current_schema() for this connection.
// This is the first schema in search_path that exists and is accessible.
// Used to determine the default schema when not explicitly configured.
//
// Parameters:
//   - ctx: Context for cancellation
//   - pool: PostgreSQL connection pool
//
// Returns the current schema name, or error on database failure.
func GetCurrentSchema(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	logging.Debug("Querying current_schema from PostgreSQL")

	var schema string
	err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema)
	if err != nil {
		return "", fmt.Errorf("failed to query current_schema: %w", err)
	}

	logging.Debug("Got current schema", zap.String("schema", schema))
	return schema, nil
}

// GetCurrentSchema is a convenience wrapper around GetCurrentSchema for Client.
func (c *Client) GetCurrentSchema(ctx context.Context) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetCurrentSchema(ctx, c.pool)
}

// GetSchemas returns all user-defined schemas (excluding system schemas)
func GetSchemas(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	logging.Debug("Querying schemas from information_schema")

	query := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY schema_name
	`

	rows, err := pool.Query(ctx, query)
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

// GetTables returns all tables in a given schema
func GetTables(ctx context.Context, pool *pgxpool.Pool, schema string) ([]string, error) {
	logging.Debug("Querying tables from information_schema", zap.String("schema", schema))

	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := pool.Query(ctx, query, schema)
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
func (c *Client) GetSchemas(ctx context.Context) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetSchemas(ctx, c.pool)
}

// GetTables is a convenience wrapper around GetTables for Client
func (c *Client) GetTables(ctx context.Context, schema string) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetTables(ctx, c.pool, schema)
}

// Column represents metadata about a table column
type Column struct {
	Name       string
	DataType   string
	IsNullable bool
	Default    string // Column default value (empty string if no default)
}

// GetColumns returns all columns for a table in schema order
func GetColumns(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]Column, error) {
	logging.Debug("Querying columns from information_schema",
		zap.String("schema", schema),
		zap.String("table", table))

	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := pool.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var isNullableStr string
		if err := rows.Scan(&col.Name, &col.DataType, &isNullableStr); err != nil {
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
func (c *Client) GetColumns(ctx context.Context, schema, table string) ([]Column, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetColumns(ctx, c.pool, schema, table)
}

// GetRowCount returns the number of rows in a table
func GetRowCount(ctx context.Context, pool *pgxpool.Pool, schema, table string) (int64, error) {
	logging.Debug("Getting row count",
		zap.String("schema", schema),
		zap.String("table", table))

	query := fmt.Sprintf(
		`SELECT COUNT(*) FROM "%s"."%s"`,
		schema, table,
	)

	var count int64
	err := pool.QueryRow(ctx, query).Scan(&count)
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
func (c *Client) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	if c.pool == nil {
		return 0, fmt.Errorf("database connection not initialized")
	}
	return GetRowCount(ctx, c.pool, schema, table)
}

// SmallTableThreshold is the row count below which exact COUNT(*) is used.
// For tables with more rows, pg_class.reltuples estimate is returned instead
// to avoid expensive full table scans.
const SmallTableThreshold = 100000

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
//   - pool: PostgreSQL connection pool
//   - schema: PostgreSQL schema name
//   - table: Table name to count
//
// Returns the row count (exact or estimated), or error on database failure.
func GetRowCountSmart(ctx context.Context, pool *pgxpool.Pool, schema, table string) (int64, error) {
	logging.Debug("Getting smart row count",
		zap.String("schema", schema),
		zap.String("table", table))

	// First, get the pg_class.reltuples estimate
	estimateQuery := `
		SELECT COALESCE(c.reltuples::bigint, 0)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`

	var estimate int64
	err := pool.QueryRow(ctx, estimateQuery, schema, table).Scan(&estimate)
	if err != nil {
		if err == pgx.ErrNoRows {
			// Table not found in pg_class - fall back to exact count
			logging.Debug("Table not found in pg_class, using exact count",
				zap.String("schema", schema),
				zap.String("table", table))
			return GetRowCount(ctx, pool, schema, table)
		}
		return 0, fmt.Errorf("failed to get row count estimate: %w", err)
	}

	logging.Debug("Got row count estimate from pg_class",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int64("estimate", estimate))

	// For small tables, use exact count for accuracy
	if estimate < SmallTableThreshold {
		logging.Debug("Small table detected, using exact count",
			zap.String("table", table),
			zap.Int64("estimate", estimate),
			zap.Int64("threshold", SmallTableThreshold))
		return GetRowCount(ctx, pool, schema, table)
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
func (c *Client) GetRowCountSmart(ctx context.Context, schema, table string) (int64, error) {
	if c.pool == nil {
		return 0, fmt.Errorf("database connection not initialized")
	}
	return GetRowCountSmart(ctx, c.pool, schema, table)
}

// GetRowCountEstimates returns fast row count estimates for multiple tables using pg_class.
// Uses reltuples from PostgreSQL statistics, avoiding full table scans.
// Returns a map of table name to estimated row count.
func GetRowCountEstimates(ctx context.Context, pool *pgxpool.Pool, schema string, tables []string) (map[string]int64, error) {
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

	rows, err := pool.Query(ctx, query, schema, tables)
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
func (c *Client) GetRowCountEstimates(ctx context.Context, schema string, tables []string) (map[string]int64, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetRowCountEstimates(ctx, c.pool, schema, tables)
}

// GetTableDDL returns the CREATE TABLE statement for a table
// Constructs DDL from information_schema
func GetTableDDL(ctx context.Context, pool *pgxpool.Pool, schema, table string) (string, error) {
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

	rows, err := pool.Query(ctx, query, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query column definitions: %w", err)
	}
	defer rows.Close()

	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (\n", schema, table))

	columnDefs := []string{}
	for rows.Next() {
		var colName, dataType string
		var maxLength *int
		var isNullable, colDefault *string

		if err := rows.Scan(&colName, &dataType, &maxLength, &isNullable, &colDefault); err != nil {
			return "", fmt.Errorf("failed to scan column definition: %w", err)
		}

		colDef := fmt.Sprintf("  \"%s\" %s", colName, dataType)

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
	if err := pool.QueryRow(ctx, pkQuery, schema, table).Scan(&pkColumns); err != nil && err != pgx.ErrNoRows {
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
func (c *Client) GetTableDDL(ctx context.Context, schema, table string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetTableDDL(ctx, c.pool, schema, table)
}

// GetIndexDDL returns CREATE INDEX statements for a table.
// Includes both regular and unique indexes, excluding primary key constraints
// (which are already shown in the CREATE TABLE statement).
func GetIndexDDL(ctx context.Context, pool *pgxpool.Pool, schema, table string) (string, error) {
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

	rows, err := pool.Query(ctx, query, schema, table)
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
func (c *Client) GetIndexDDL(ctx context.Context, schema, table string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetIndexDDL(ctx, c.pool, schema, table)
}

// GetForeignKeyDDL returns ALTER TABLE statements for foreign key constraints.
func GetForeignKeyDDL(ctx context.Context, pool *pgxpool.Pool, schema, table string) (string, error) {
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

	rows, err := pool.Query(ctx, query, schema, table)
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
func (c *Client) GetForeignKeyDDL(ctx context.Context, schema, table string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetForeignKeyDDL(ctx, c.pool, schema, table)
}

// GetCheckConstraintDDL returns ALTER TABLE statements for check constraints.
func GetCheckConstraintDDL(ctx context.Context, pool *pgxpool.Pool, schema, table string) (string, error) {
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

	rows, err := pool.Query(ctx, query, schema, table)
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
func (c *Client) GetCheckConstraintDDL(ctx context.Context, schema, table string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetCheckConstraintDDL(ctx, c.pool, schema, table)
}

// GetTriggerDDL returns CREATE TRIGGER statements for a table.
func GetTriggerDDL(ctx context.Context, pool *pgxpool.Pool, schema, table string) (string, error) {
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

	rows, err := pool.Query(ctx, query, schema, table)
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
func (c *Client) GetTriggerDDL(ctx context.Context, schema, table string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetTriggerDDL(ctx, c.pool, schema, table)
}

// GetTableComments returns COMMENT statements for table and columns.
func GetTableComments(ctx context.Context, pool *pgxpool.Pool, schema, table string) (string, error) {
	logging.Debug("Getting table comments",
		zap.String("schema", schema),
		zap.String("table", table))

	var ddl strings.Builder

	// Get table comment
	tableCommentQuery := `
		SELECT obj_description((quote_ident($1) || '.' || quote_ident($2))::regclass, 'pg_class')
	`
	var tableComment *string
	err := pool.QueryRow(ctx, tableCommentQuery, schema, table).Scan(&tableComment)
	if err != nil {
		return "", fmt.Errorf("failed to query table comment: %w", err)
	}

	if tableComment != nil && *tableComment != "" {
		ddl.WriteString(fmt.Sprintf("COMMENT ON TABLE \"%s\".\"%s\" IS '%s';\n",
			schema, table, strings.ReplaceAll(*tableComment, "'", "''")))
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

	rows, err := pool.Query(ctx, columnCommentQuery, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query column comments: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var colName, comment string
		if err := rows.Scan(&colName, &comment); err != nil {
			return "", fmt.Errorf("failed to scan column comment: %w", err)
		}
		ddl.WriteString(fmt.Sprintf("COMMENT ON COLUMN \"%s\".\"%s\".\"%s\" IS '%s';\n",
			schema, table, colName, strings.ReplaceAll(comment, "'", "''")))
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading column comment rows: %w", err)
	}

	return ddl.String(), nil
}

// GetTableComments is a convenience wrapper for Client
func (c *Client) GetTableComments(ctx context.Context, schema, table string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetTableComments(ctx, c.pool, schema, table)
}

// GetFullDDL returns complete DDL for a table including:
// - CREATE TABLE statement
// - Indexes
// - Foreign keys
// - Check constraints
// - Triggers
// - Comments
// Sections are only included if they have content.
func GetFullDDL(ctx context.Context, pool *pgxpool.Pool, schema, table string) (string, error) {
	logging.Debug("Getting full DDL",
		zap.String("schema", schema),
		zap.String("table", table))

	var ddl strings.Builder

	// Table definition
	tableDDL, err := GetTableDDL(ctx, pool, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get table DDL: %w", err)
	}
	ddl.WriteString("-- Table\n")
	ddl.WriteString(tableDDL)
	ddl.WriteString("\n")

	// Indexes
	indexDDL, err := GetIndexDDL(ctx, pool, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get index DDL: %w", err)
	}
	if indexDDL != "" {
		ddl.WriteString("-- Indexes\n")
		ddl.WriteString(indexDDL)
		ddl.WriteString("\n")
	}

	// Foreign keys
	fkDDL, err := GetForeignKeyDDL(ctx, pool, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get foreign key DDL: %w", err)
	}
	if fkDDL != "" {
		ddl.WriteString("-- Foreign Keys\n")
		ddl.WriteString(fkDDL)
		ddl.WriteString("\n")
	}

	// Check constraints
	checkDDL, err := GetCheckConstraintDDL(ctx, pool, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get check constraint DDL: %w", err)
	}
	if checkDDL != "" {
		ddl.WriteString("-- Check Constraints\n")
		ddl.WriteString(checkDDL)
		ddl.WriteString("\n")
	}

	// Triggers
	triggerDDL, err := GetTriggerDDL(ctx, pool, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to get trigger DDL: %w", err)
	}
	if triggerDDL != "" {
		ddl.WriteString("-- Triggers\n")
		ddl.WriteString(triggerDDL)
		ddl.WriteString("\n")
	}

	// Comments
	commentsDDL, err := GetTableComments(ctx, pool, schema, table)
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
func (c *Client) GetFullDDL(ctx context.Context, schema, table string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetFullDDL(ctx, c.pool, schema, table)
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
func GetReferencingForeignKeys(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]ForeignKeyRef, error) {
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

	rows, err := pool.Query(ctx, query, schema, table)
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
func (c *Client) GetReferencingForeignKeys(ctx context.Context, schema, table string) ([]ForeignKeyRef, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetReferencingForeignKeys(ctx, c.pool, schema, table)
}

// GetSchemaTableCount returns the number of tables in a schema.
// Used to determine if a schema is empty when generating delete templates.
func GetSchemaTableCount(ctx context.Context, pool *pgxpool.Pool, schema string) (int, error) {
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
	err := pool.QueryRow(ctx, query, schema).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema table count: %w", err)
	}

	return count, nil
}

// GetSchemaTableCount is a convenience wrapper for Client
func (c *Client) GetSchemaTableCount(ctx context.Context, schema string) (int, error) {
	if c.pool == nil {
		return 0, fmt.Errorf("database connection not initialized")
	}
	return GetSchemaTableCount(ctx, c.pool, schema)
}

// GetViewDefinition returns the SQL definition of a view.
func GetViewDefinition(ctx context.Context, pool *pgxpool.Pool, schema, view string) (string, error) {
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
	err := pool.QueryRow(ctx, query, schema, view).Scan(&definition)
	if err != nil {
		return "", fmt.Errorf("failed to get view definition: %w", err)
	}

	return definition, nil
}

// GetViewDefinition is a convenience wrapper for Client
func (c *Client) GetViewDefinition(ctx context.Context, schema, view string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return GetViewDefinition(ctx, c.pool, schema, view)
}

// GetDependentViews returns views that depend on the specified view or table.
func GetDependentViews(ctx context.Context, pool *pgxpool.Pool, schema, name string) ([]string, error) {
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

	rows, err := pool.Query(ctx, query, schema, name)
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
func (c *Client) GetDependentViews(ctx context.Context, schema, name string) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetDependentViews(ctx, c.pool, schema, name)
}
