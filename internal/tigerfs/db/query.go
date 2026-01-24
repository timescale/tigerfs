package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// Row represents a database row with column names and values
type Row struct {
	Columns []string
	Values  []interface{}
}

// GetRow fetches a single row by primary key
func GetRow(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn, pkValue string) (*Row, error) {
	logging.Debug("Querying row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.String("pk_value", pkValue))

	// Build query with proper quoting for identifiers
	// SELECT * FROM "schema"."table" WHERE "pk_column" = $1
	query := fmt.Sprintf(
		`SELECT * FROM "%s"."%s" WHERE "%s" = $1`,
		schema, table, pkColumn,
	)

	rows, err := pool.Query(ctx, query, pkValue)
	if err != nil {
		return nil, fmt.Errorf("failed to query row: %w", err)
	}
	defer rows.Close()

	// Get column names from field descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	// Read the single row
	if !rows.Next() {
		// No rows found
		return nil, fmt.Errorf("row not found")
	}

	// Scan values
	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("failed to scan row values: %w", err)
	}

	// Check for unexpected additional rows
	if rows.Next() {
		logging.Warn("Multiple rows returned for primary key lookup",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.String("pk_column", pkColumn),
			zap.String("pk_value", pkValue))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading row: %w", err)
	}

	logging.Debug("Row fetched successfully",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("column_count", len(columns)))

	return &Row{
		Columns: columns,
		Values:  values,
	}, nil
}

// GetRow is a convenience wrapper for Client
func (c *Client) GetRow(ctx context.Context, schema, table, pkColumn, pkValue string) (*Row, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetRow(ctx, c.pool, schema, table, pkColumn, pkValue)
}

// GetColumn fetches a single column value from a row by primary key
// Returns the column value and whether it was NULL
func GetColumn(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
	logging.Debug("Querying column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.String("pk_value", pkValue),
		zap.String("column", columnName))

	// Build query with proper quoting for identifiers
	// SELECT "column" FROM "schema"."table" WHERE "pk_column" = $1
	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" WHERE "%s" = $1`,
		columnName, schema, table, pkColumn,
	)

	var value interface{}
	err := pool.QueryRow(ctx, query, pkValue).Scan(&value)
	if err != nil {
		return nil, fmt.Errorf("failed to query column: %w", err)
	}

	logging.Debug("Column fetched successfully",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("column", columnName),
		zap.Bool("is_null", value == nil))

	return value, nil
}

// GetColumn is a convenience wrapper for Client
func (c *Client) GetColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetColumn(ctx, c.pool, schema, table, pkColumn, pkValue, columnName)
}

// UpdateColumn updates a single column value for a row by primary key
// Empty string is treated as NULL
func UpdateColumn(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn, pkValue, columnName, newValue string) error {
	logging.Debug("Updating column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.String("pk_value", pkValue),
		zap.String("column", columnName),
		zap.Bool("is_null", newValue == ""))

	// Build UPDATE query with proper quoting for identifiers
	// UPDATE "schema"."table" SET "column" = $1 WHERE "pk_column" = $2
	query := fmt.Sprintf(
		`UPDATE "%s"."%s" SET "%s" = $1 WHERE "%s" = $2`,
		schema, table, columnName, pkColumn,
	)

	// Convert empty string to NULL
	var value interface{}
	if newValue == "" {
		value = nil
	} else {
		value = newValue
	}

	// Execute update
	cmdTag, err := pool.Exec(ctx, query, value, pkValue)
	if err != nil {
		return fmt.Errorf("failed to update column: %w", err)
	}

	// Check if any rows were affected
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("row not found")
	}

	logging.Debug("Column updated successfully",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("column", columnName),
		zap.Int64("rows_affected", cmdTag.RowsAffected()))

	return nil
}

// UpdateColumn is a convenience wrapper for Client
func (c *Client) UpdateColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}
	return UpdateColumn(ctx, c.pool, schema, table, pkColumn, pkValue, columnName, newValue)
}
