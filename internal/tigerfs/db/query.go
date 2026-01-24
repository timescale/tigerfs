package db

import (
	"context"
	"fmt"
	"strings"

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

// InsertRow inserts a new row with the given column values
// Returns the inserted primary key value (useful for auto-generated PKs)
func InsertRow(ctx context.Context, pool *pgxpool.Pool, schema, table string, columns []string, values []interface{}) (string, error) {
	logging.Debug("Inserting row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("column_count", len(columns)))

	if len(columns) == 0 {
		return "", fmt.Errorf("no columns provided for insert")
	}

	if len(columns) != len(values) {
		return "", fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(values))
	}

	// Build column list: ("col1", "col2", "col3")
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = fmt.Sprintf(`"%s"`, col)
	}
	columnList := strings.Join(quotedColumns, ", ")

	// Build placeholder list: ($1, $2, $3)
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	placeholderList := strings.Join(placeholders, ", ")

	// Build INSERT query with RETURNING for primary key
	query := fmt.Sprintf(
		`INSERT INTO "%s"."%s" (%s) VALUES (%s) RETURNING *`,
		schema, table, columnList, placeholderList,
	)

	// Execute insert
	rows, err := pool.Query(ctx, query, values...)
	if err != nil {
		return "", fmt.Errorf("failed to insert row: %w", err)
	}
	defer rows.Close()

	// Read returned row to get PK
	if !rows.Next() {
		return "", fmt.Errorf("insert did not return a row")
	}

	// Get first column value (typically the PK)
	returnedValues, err := rows.Values()
	if err != nil {
		return "", fmt.Errorf("failed to scan returned values: %w", err)
	}

	if len(returnedValues) == 0 {
		return "", fmt.Errorf("insert returned no values")
	}

	// Convert first value (PK) to string
	pkValue := fmt.Sprintf("%v", returnedValues[0])

	logging.Debug("Row inserted successfully",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_value", pkValue))

	return pkValue, nil
}

// InsertRow is a convenience wrapper for Client
func (c *Client) InsertRow(ctx context.Context, schema, table string, columns []string, values []interface{}) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("database connection not initialized")
	}
	return InsertRow(ctx, c.pool, schema, table, columns, values)
}

// UpdateRow updates an existing row with the given column values
func UpdateRow(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn, pkValue string, columns []string, values []interface{}) error {
	logging.Debug("Updating row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.String("pk_value", pkValue),
		zap.Int("column_count", len(columns)))

	if len(columns) == 0 {
		return fmt.Errorf("no columns provided for update")
	}

	if len(columns) != len(values) {
		return fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(values))
	}

	// Build SET clause: "col1" = $1, "col2" = $2, ...
	setClauses := make([]string, len(columns))
	for i, col := range columns {
		setClauses[i] = fmt.Sprintf(`"%s" = $%d`, col, i+1)
	}
	setClause := strings.Join(setClauses, ", ")

	// Build UPDATE query
	// UPDATE "schema"."table" SET "col1" = $1, "col2" = $2 WHERE "pk_column" = $N
	query := fmt.Sprintf(
		`UPDATE "%s"."%s" SET %s WHERE "%s" = $%d`,
		schema, table, setClause, pkColumn, len(values)+1,
	)

	// Append PK value to values list
	allValues := append(values, pkValue)

	// Execute update
	cmdTag, err := pool.Exec(ctx, query, allValues...)
	if err != nil {
		return fmt.Errorf("failed to update row: %w", err)
	}

	// Check if any rows were affected
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("row not found")
	}

	logging.Debug("Row updated successfully",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int64("rows_affected", cmdTag.RowsAffected()))

	return nil
}

// UpdateRow is a convenience wrapper for Client
func (c *Client) UpdateRow(ctx context.Context, schema, table, pkColumn, pkValue string, columns []string, values []interface{}) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}
	return UpdateRow(ctx, c.pool, schema, table, pkColumn, pkValue, columns, values)
}
