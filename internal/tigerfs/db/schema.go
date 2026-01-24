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
