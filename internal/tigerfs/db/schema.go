package db

import (
	"context"
	"fmt"

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
