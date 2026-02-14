package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
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

// UpdateColumnCAS performs a compare-and-swap update on a column.
// Only updates the row if whereColumn still has whereValue (atomic check).
// Returns "row not found" if no row matches, enabling safe concurrent renames.
func (c *Client) UpdateColumnCAS(ctx context.Context, schema, table, pkColumn, pkValue, setColumn, newValue, whereColumn, whereValue string) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}

	logging.Debug("CAS updating column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.String("pk_value", pkValue),
		zap.String("set_column", setColumn),
		zap.String("where_column", whereColumn))

	// UPDATE "schema"."table" SET "setColumn" = $1 WHERE "pkColumn" = $2 AND "whereColumn" = $3
	query := fmt.Sprintf(
		`UPDATE "%s"."%s" SET "%s" = $1 WHERE "%s" = $2 AND "%s" = $3`,
		schema, table, setColumn, pkColumn, whereColumn,
	)

	cmdTag, err := c.pool.Exec(ctx, query, newValue, pkValue, whereValue)
	if err != nil {
		return fmt.Errorf("failed to update column: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("row not found")
	}

	return nil
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

	// Convert first value (PK) to string using format helper
	// This properly handles UUID and other PostgreSQL types
	pkValue, err := format.ConvertValueToText(returnedValues[0])
	if err != nil {
		return "", fmt.Errorf("failed to convert returned primary key: %w", err)
	}

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

// DeleteRow deletes a row by primary key
func DeleteRow(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn, pkValue string) error {
	logging.Debug("Deleting row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.String("pk_value", pkValue))

	// Build DELETE query with proper quoting for identifiers
	// DELETE FROM "schema"."table" WHERE "pk_column" = $1
	query := fmt.Sprintf(
		`DELETE FROM "%s"."%s" WHERE "%s" = $1`,
		schema, table, pkColumn,
	)

	// Execute delete
	cmdTag, err := pool.Exec(ctx, query, pkValue)
	if err != nil {
		return fmt.Errorf("failed to delete row: %w", err)
	}

	// Check if any rows were affected
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("row not found")
	}

	logging.Debug("Row deleted successfully",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int64("rows_affected", cmdTag.RowsAffected()))

	return nil
}

// DeleteRow is a convenience wrapper for Client
func (c *Client) DeleteRow(ctx context.Context, schema, table, pkColumn, pkValue string) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}
	return DeleteRow(ctx, c.pool, schema, table, pkColumn, pkValue)
}

// GetFirstNRows returns the first N primary key values ordered by PK ascending.
// Used by .first/N/ pagination paths.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pkColumn: Primary key column name
//   - limit: Maximum number of rows to return
//
// Returns primary keys as strings, or error on database failure.
func GetFirstNRows(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn string, limit int) ([]string, error) {
	logging.Debug("Getting first N rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" ORDER BY "%s" ASC LIMIT $1`,
		pkColumn, schema, table, pkColumn,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query first N rows: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk interface{}
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pkStr, err := format.ConvertValueToText(pk)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary key value: %w", err)
		}
		pks = append(pks, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Got first N rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("count", len(pks)))

	return pks, nil
}

// GetFirstNRows is a convenience wrapper for Client
func (c *Client) GetFirstNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetFirstNRows(ctx, c.pool, schema, table, pkColumn, limit)
}

// GetLastNRows returns the last N primary key values ordered by PK descending.
// Used by .last/N/ pagination paths.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pkColumn: Primary key column name
//   - limit: Maximum number of rows to return
//
// Returns primary keys as strings (highest values first), or error on database failure.
func GetLastNRows(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn string, limit int) ([]string, error) {
	logging.Debug("Getting last N rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" ORDER BY "%s" DESC LIMIT $1`,
		pkColumn, schema, table, pkColumn,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query last N rows: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk interface{}
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pkStr, err := format.ConvertValueToText(pk)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary key value: %w", err)
		}
		pks = append(pks, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Got last N rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("count", len(pks)))

	return pks, nil
}

// GetLastNRows is a convenience wrapper for Client
func (c *Client) GetLastNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetLastNRows(ctx, c.pool, schema, table, pkColumn, limit)
}

// GetRandomSampleRows returns approximately N random primary key values.
// Used by .sample/N/ paths for random sampling of large tables.
//
// For tables with estimated row count available, uses TABLESAMPLE BERNOULLI
// for efficient block-level sampling. Falls back to ORDER BY RANDOM() for
// small tables or when row count is unknown.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pkColumn: Primary key column name
//   - limit: Target number of rows to return (approximate)
//   - estimatedRows: Estimated total row count (-1 if unknown)
//
// Returns primary keys as strings. The actual count may vary from the
// requested limit due to the probabilistic nature of TABLESAMPLE.
func GetRandomSampleRows(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error) {
	logging.Debug("Getting random sample rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.Int("limit", limit),
		zap.Int64("estimated_rows", estimatedRows))

	var query string

	// Use TABLESAMPLE for large tables (more efficient than ORDER BY RANDOM())
	// BERNOULLI samples at the row level with given percentage
	if estimatedRows > 1000 {
		// Calculate percentage to get approximately 'limit' rows
		// Add 20% buffer since BERNOULLI is probabilistic
		percentage := float64(limit) * 1.2 / float64(estimatedRows) * 100.0
		if percentage > 100 {
			percentage = 100
		}
		if percentage < 0.001 {
			percentage = 0.001 // Minimum percentage to avoid empty results
		}

		query = fmt.Sprintf(
			`SELECT "%s" FROM "%s"."%s" TABLESAMPLE BERNOULLI(%f) LIMIT $1`,
			pkColumn, schema, table, percentage,
		)
	} else {
		// For small tables or unknown size, use ORDER BY RANDOM()
		query = fmt.Sprintf(
			`SELECT "%s" FROM "%s"."%s" ORDER BY RANDOM() LIMIT $1`,
			pkColumn, schema, table,
		)
	}

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query random sample: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk interface{}
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pkStr, err := format.ConvertValueToText(pk)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary key value: %w", err)
		}
		pks = append(pks, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Got random sample rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("requested", limit),
		zap.Int("returned", len(pks)))

	return pks, nil
}

// GetRandomSampleRows is a convenience wrapper for Client
func (c *Client) GetRandomSampleRows(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetRandomSampleRows(ctx, c.pool, schema, table, pkColumn, limit, estimatedRows)
}

// GetFirstNRowsOrdered returns the first N primary key values ordered by a specified column ascending.
// Used by .order/<column>/.first/N/ paths for custom ordering.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pkColumn: Primary key column name (returned in results)
//   - orderColumn: Column to order by (ascending)
//   - limit: Maximum number of rows to return
//
// Returns primary key values as strings, ordered by orderColumn ASC.
func GetFirstNRowsOrdered(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn, orderColumn string, limit int) ([]string, error) {
	logging.Debug("Getting first N rows ordered by column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.String("order_column", orderColumn),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" ORDER BY "%s" ASC NULLS LAST, "%s" ASC LIMIT $1`,
		pkColumn, schema, table, orderColumn, pkColumn,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query first N rows ordered: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk interface{}
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pkStr, err := format.ConvertValueToText(pk)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary key value: %w", err)
		}
		pks = append(pks, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Got first N rows ordered",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("order_column", orderColumn),
		zap.Int("count", len(pks)))

	return pks, nil
}

// GetFirstNRowsOrdered is a convenience wrapper for Client
func (c *Client) GetFirstNRowsOrdered(ctx context.Context, schema, table, pkColumn, orderColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetFirstNRowsOrdered(ctx, c.pool, schema, table, pkColumn, orderColumn, limit)
}

// GetLastNRowsOrdered returns the last N primary key values ordered by a specified column descending.
// Used by .order/<column>/.last/N/ paths for custom ordering.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pkColumn: Primary key column name (returned in results)
//   - orderColumn: Column to order by (descending)
//   - limit: Maximum number of rows to return
//
// Returns primary key values as strings, ordered by orderColumn DESC.
func GetLastNRowsOrdered(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn, orderColumn string, limit int) ([]string, error) {
	logging.Debug("Getting last N rows ordered by column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.String("order_column", orderColumn),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" ORDER BY "%s" DESC NULLS LAST, "%s" DESC LIMIT $1`,
		pkColumn, schema, table, orderColumn, pkColumn,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query last N rows ordered: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk interface{}
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pkStr, err := format.ConvertValueToText(pk)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary key value: %w", err)
		}
		pks = append(pks, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Got last N rows ordered",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("order_column", orderColumn),
		zap.Int("count", len(pks)))

	return pks, nil
}

// GetLastNRowsOrdered is a convenience wrapper for Client
func (c *Client) GetLastNRowsOrdered(ctx context.Context, schema, table, pkColumn, orderColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetLastNRowsOrdered(ctx, c.pool, schema, table, pkColumn, orderColumn, limit)
}

// RenameByPrefix atomically renames all rows where the given column value starts
// with oldPrefix to use newPrefix instead. Used for directory renames in synth views.
// Returns the number of rows affected.
func (c *Client) RenameByPrefix(ctx context.Context, schema, table, column, oldPrefix, newPrefix string) (int64, error) {
	if c.pool == nil {
		return 0, fmt.Errorf("database connection not initialized")
	}

	// UPDATE "schema"."table" SET "column" = $1 || substr("column", length($2) + 1)
	// WHERE "column" = $2 OR "column" LIKE $2 || '/%'
	query := fmt.Sprintf(
		`UPDATE "%s"."%s" SET "%s" = $1 || substr("%s", length($2) + 1) WHERE "%s" = $2 OR "%s" LIKE $2 || '/%%'`,
		schema, table, column, column, column, column,
	)

	cmdTag, err := c.pool.Exec(ctx, query, newPrefix, oldPrefix)
	if err != nil {
		return 0, fmt.Errorf("failed to rename by prefix: %w", err)
	}

	return cmdTag.RowsAffected(), nil
}

// HasChildrenWithPrefix checks if any rows exist where the given column value
// starts with prefix + "/". Used to check if a directory has children before rmdir.
func (c *Client) HasChildrenWithPrefix(ctx context.Context, schema, table, column, prefix string) (bool, error) {
	if c.pool == nil {
		return false, fmt.Errorf("database connection not initialized")
	}

	query := fmt.Sprintf(
		`SELECT EXISTS(SELECT 1 FROM "%s"."%s" WHERE "%s" LIKE $1 || '/%%')`,
		schema, table, column,
	)

	var exists bool
	err := c.pool.QueryRow(ctx, query, prefix).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check children: %w", err)
	}

	return exists, nil
}

// InsertIfNotExists inserts a row only if it doesn't already exist (ON CONFLICT DO NOTHING).
// Used for auto-creating parent directory rows in synth views.
func (c *Client) InsertIfNotExists(ctx context.Context, schema, table string, columns []string, values []interface{}) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}

	if len(columns) == 0 || len(columns) != len(values) {
		return fmt.Errorf("column/value count mismatch: %d columns, %d values", len(columns), len(values))
	}

	// Build column list and parameter placeholders
	quotedCols := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = fmt.Sprintf(`"%s"`, col)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s"."%s" (%s) VALUES (%s) ON CONFLICT DO NOTHING`,
		schema, table, strings.Join(quotedCols, ", "), strings.Join(placeholders, ", "),
	)

	_, err := c.pool.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert if not exists: %w", err)
	}

	return nil
}
