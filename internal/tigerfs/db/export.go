package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// GetAllRows returns all rows from a table up to the specified limit.
// Returns columns, rows (as [][]interface{}), and error.
// Used for bulk export operations.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - limit: Maximum number of rows to return
//
// Returns:
//   - columns: Column names in database order
//   - rows: Row values as [][]interface{}
//   - error: Any database error
func GetAllRows(ctx context.Context, pool *pgxpool.Pool, schema, table string, limit int) ([]string, [][]interface{}, error) {
	logging.Debug("Getting all rows for export",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT * FROM "%s"."%s" LIMIT $1`,
		schema, table,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query rows: %w", err)
	}
	defer rows.Close()

	// Get column names from field descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	// Read all rows
	var result [][]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan row values: %w", err)
		}
		result = append(result, values)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Got all rows for export",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("row_count", len(result)))

	return columns, result, nil
}

// GetAllRows is a convenience wrapper for Client
func (c *Client) GetAllRows(ctx context.Context, schema, table string, limit int) ([]string, [][]interface{}, error) {
	if c.pool == nil {
		return nil, nil, fmt.Errorf("database connection not initialized")
	}
	return GetAllRows(ctx, c.pool, schema, table, limit)
}

// GetFirstNRowsWithData returns the first N rows ordered by primary key ascending.
// Returns full row data, not just primary keys.
// Used for bulk export with .first/N/ pagination.
func GetFirstNRowsWithData(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error) {
	logging.Debug("Getting first N rows with data",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT * FROM "%s"."%s" ORDER BY "%s" ASC LIMIT $1`,
		schema, table, pkColumn,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query first N rows: %w", err)
	}
	defer rows.Close()

	// Get column names
	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	// Read all rows
	var result [][]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan row values: %w", err)
		}
		result = append(result, values)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Got first N rows with data",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("row_count", len(result)))

	return columns, result, nil
}

// GetFirstNRowsWithData is a convenience wrapper for Client
func (c *Client) GetFirstNRowsWithData(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error) {
	if c.pool == nil {
		return nil, nil, fmt.Errorf("database connection not initialized")
	}
	return GetFirstNRowsWithData(ctx, c.pool, schema, table, pkColumn, limit)
}

// GetLastNRowsWithData returns the last N rows ordered by primary key descending.
// Returns full row data, not just primary keys.
// Used for bulk export with .last/N/ pagination.
func GetLastNRowsWithData(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error) {
	logging.Debug("Getting last N rows with data",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT * FROM "%s"."%s" ORDER BY "%s" DESC LIMIT $1`,
		schema, table, pkColumn,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query last N rows: %w", err)
	}
	defer rows.Close()

	// Get column names
	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	// Read all rows
	var result [][]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan row values: %w", err)
		}
		result = append(result, values)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Got last N rows with data",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("row_count", len(result)))

	return columns, result, nil
}

// GetLastNRowsWithData is a convenience wrapper for Client
func (c *Client) GetLastNRowsWithData(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, [][]interface{}, error) {
	if c.pool == nil {
		return nil, nil, fmt.Errorf("database connection not initialized")
	}
	return GetLastNRowsWithData(ctx, c.pool, schema, table, pkColumn, limit)
}

// RowExistsByColumns checks if any row matches the given column=value conditions.
// Generates: SELECT 1 FROM "schema"."table" WHERE "col1" = $1 AND "col2" = $2 LIMIT 1
func (c *Client) RowExistsByColumns(ctx context.Context, schema, table string, columns []string, values []interface{}) (bool, error) {
	if c.pool == nil {
		return false, fmt.Errorf("database connection not initialized")
	}

	// Build WHERE clause
	var whereParts []string
	for i, col := range columns {
		whereParts = append(whereParts, fmt.Sprintf(`"%s" = $%d`, col, i+1))
	}

	query := fmt.Sprintf(
		`SELECT 1 FROM "%s"."%s" WHERE %s LIMIT 1`,
		schema, table, strings.Join(whereParts, " AND "),
	)

	logging.Debug("Checking row existence by columns",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("columns", columns))

	var dummy int
	err := c.pool.QueryRow(ctx, query, values...).Scan(&dummy)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return false, nil
		}
		return false, fmt.Errorf("failed to check row existence: %w", err)
	}

	return true, nil
}

// GetRowByColumns returns a single row matching column=value conditions.
// Returns all columns. Returns (nil, nil, nil) if no match.
// Generates: SELECT * FROM "schema"."table" WHERE "col1" = $1 AND "col2" = $2 LIMIT 1
func (c *Client) GetRowByColumns(ctx context.Context, schema, table string, columns []string, values []interface{}) ([]string, []interface{}, error) {
	if c.pool == nil {
		return nil, nil, fmt.Errorf("database connection not initialized")
	}

	// Build WHERE clause
	var whereParts []string
	for i, col := range columns {
		whereParts = append(whereParts, fmt.Sprintf(`"%s" = $%d`, col, i+1))
	}

	query := fmt.Sprintf(
		`SELECT * FROM "%s"."%s" WHERE %s LIMIT 1`,
		schema, table, strings.Join(whereParts, " AND "),
	)

	logging.Debug("Getting row by columns",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("columns", columns))

	rows, err := c.pool.Query(ctx, query, values...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query row by columns: %w", err)
	}
	defer rows.Close()

	// Get column names from field descriptions
	fieldDescriptions := rows.FieldDescriptions()
	colNames := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		colNames[i] = string(fd.Name)
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error reading row: %w", err)
		}
		return nil, nil, nil // No match
	}

	rowValues, err := rows.Values()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scan row values: %w", err)
	}

	return colNames, rowValues, nil
}
