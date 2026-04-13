package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// isUniqueViolation returns true if the error is a PostgreSQL unique violation (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Row represents a database row with column names and values
type Row struct {
	Columns []string
	Values  []interface{}
}

// pkSelectList returns a comma-separated list of quoted PK column names for SELECT.
// Example: `"customer_id", "product_id"`
func pkSelectList(pkColumns []string) string {
	quoted := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		quoted[i] = qi(col)
	}
	return strings.Join(quoted, ", ")
}

// pkOrderByList returns a comma-separated ORDER BY clause fragment for PK columns.
// Example with ASC: `"customer_id" ASC, "product_id" ASC`
func pkOrderByList(pkColumns []string, direction string) string {
	parts := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		parts[i] = qi(col) + " " + direction
	}
	return strings.Join(parts, ", ")
}

// scanAndEncodePK scans multiple PK column values from a row and encodes them
// into a single string using the PrimaryKey's encoding rules.
//
// For single-column UUIDv7 primary keys, this produces a human-readable
// timestamp+base36 display name instead of the standard hex UUID format.
// Non-v7 UUIDs continue to display as hex. See ADR-016 Section 11.
func scanAndEncodePK(values []interface{}, pkColumns []string) (string, error) {
	if len(pkColumns) == 1 {
		// Single-column: check for UUIDv7 before generic conversion
		if bytes, ok := values[0].([16]byte); ok && format.IsUUIDv7(bytes) {
			return format.UUIDv7ToDisplayName(bytes), nil
		}
		return format.ConvertValueToText(values[0])
	}
	// Multi-column: convert each value, then encode
	pk := &PrimaryKey{Columns: pkColumns}
	strs := make([]string, len(values))
	for i, v := range values {
		s, err := format.ConvertValueToText(v)
		if err != nil {
			return "", fmt.Errorf("failed to convert PK column %q: %w", pkColumns[i], err)
		}
		strs[i] = s
	}
	return pk.Encode(strs), nil
}

// GetRow fetches a single row by primary key (single or composite).
func GetRow(ctx context.Context, pool *pgxpool.Pool, schema, table string, pk *PKMatch) (*Row, error) {
	logging.Debug("Querying row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pk.Columns),
		zap.Strings("pk_values", pk.Values))

	// SELECT * FROM "schema"."table" WHERE "col1" = $1 AND "col2" = $2
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE %s`,
		qt(schema, table), pk.WhereClause(1),
	)

	rows, err := pool.Query(ctx, query, pk.WhereArgs()...)
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
			zap.Strings("pk_columns", pk.Columns),
			zap.Strings("pk_values", pk.Values))
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
func (c *Client) GetRow(ctx context.Context, schema, table string, pk *PKMatch) (*Row, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetRow(ctx, c.pool, schema, table, pk)
}

// GetColumn fetches a single column value from a row by primary key (single or composite).
func GetColumn(ctx context.Context, pool *pgxpool.Pool, schema, table string, pk *PKMatch, columnName string) (interface{}, error) {
	logging.Debug("Querying column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pk.Columns),
		zap.String("column", columnName))

	// SELECT "column" FROM "schema"."table" WHERE "col1" = $1 AND "col2" = $2
	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE %s`,
		qi(columnName), qt(schema, table), pk.WhereClause(1),
	)

	var value interface{}
	err := pool.QueryRow(ctx, query, pk.WhereArgs()...).Scan(&value)
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
func (c *Client) GetColumn(ctx context.Context, schema, table string, pk *PKMatch, columnName string) (interface{}, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetColumn(ctx, c.pool, schema, table, pk, columnName)
}

// UpdateColumn updates a single column value for a row by primary key (single or composite).
// Empty string is treated as NULL.
func UpdateColumn(ctx context.Context, pool *pgxpool.Pool, schema, table string, pk *PKMatch, columnName, newValue string) error {
	logging.Debug("Updating column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pk.Columns),
		zap.String("column", columnName),
		zap.Bool("is_null", newValue == ""))

	// UPDATE "schema"."table" SET "column" = $1 WHERE "col1" = $2 AND "col2" = $3
	query := fmt.Sprintf(
		`UPDATE %s SET %s = $1 WHERE %s`,
		qt(schema, table), qi(columnName), pk.WhereClause(2),
	)

	// Convert empty string to NULL
	var value interface{}
	if newValue == "" {
		value = nil
	} else {
		value = newValue
	}

	// Build args: [value, pkVal1, pkVal2, ...]
	args := append([]interface{}{value}, pk.WhereArgs()...)

	// Execute update
	cmdTag, err := pool.Exec(ctx, query, args...)
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
func (c *Client) UpdateColumn(ctx context.Context, schema, table string, pk *PKMatch, columnName, newValue string) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}
	return UpdateColumn(ctx, c.pool, schema, table, pk, columnName, newValue)
}

// UpdateColumnCAS performs a compare-and-swap update on a column.
// Only updates the row if whereColumn still has whereValue (atomic check).
// Returns "row not found" if no row matches, enabling safe concurrent renames.
func (c *Client) UpdateColumnCAS(ctx context.Context, schema, table string, pk *PKMatch, setColumn, newValue, whereColumn, whereValue string) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}

	logging.Debug("CAS updating column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pk.Columns),
		zap.String("set_column", setColumn),
		zap.String("where_column", whereColumn))

	// UPDATE "schema"."table" SET "setColumn" = $1 WHERE "col1" = $2 AND ... AND "whereColumn" = $N
	whereStart := 2
	pkWhere := pk.WhereClause(whereStart)
	casParam := whereStart + pk.ParamCount()
	query := fmt.Sprintf(
		`UPDATE %s SET %s = $1 WHERE %s AND %s = $%d`,
		qt(schema, table), qi(setColumn), pkWhere, qi(whereColumn), casParam,
	)

	// Build args: [newValue, pkVal1, pkVal2, ..., whereValue]
	args := append([]interface{}{newValue}, pk.WhereArgs()...)
	args = append(args, whereValue)

	cmdTag, err := c.pool.Exec(ctx, query, args...)
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
		quotedColumns[i] = qi(col)
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
		`INSERT INTO %s (%s) VALUES (%s) RETURNING *`,
		qt(schema, table), columnList, placeholderList,
	)

	// Execute insert
	rows, err := pool.Query(ctx, query, values...)
	if err != nil {
		return "", fmt.Errorf("failed to insert row: %w", err)
	}
	defer rows.Close()

	// Read returned row to get PK
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("failed to insert row: %w", err)
		}
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

// UpdateRow updates an existing row with the given column values.
func UpdateRow(ctx context.Context, pool *pgxpool.Pool, schema, table string, pk *PKMatch, columns []string, values []interface{}) error {
	logging.Debug("Updating row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pk.Columns),
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
		setClauses[i] = fmt.Sprintf(`%s = $%d`, qi(col), i+1)
	}
	setClause := strings.Join(setClauses, ", ")

	// Build UPDATE query
	// UPDATE "schema"."table" SET "col1" = $1, "col2" = $2 WHERE "pk1" = $N AND "pk2" = $N+1
	whereStart := len(values) + 1
	query := fmt.Sprintf(
		`UPDATE %s SET %s WHERE %s`,
		qt(schema, table), setClause, pk.WhereClause(whereStart),
	)

	// Append PK values to values list
	allValues := append(values, pk.WhereArgs()...)

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
func (c *Client) UpdateRow(ctx context.Context, schema, table string, pk *PKMatch, columns []string, values []interface{}) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}
	return UpdateRow(ctx, c.pool, schema, table, pk, columns, values)
}

// DeleteRow deletes a row by primary key (single or composite).
func DeleteRow(ctx context.Context, pool *pgxpool.Pool, schema, table string, pk *PKMatch) error {
	logging.Debug("Deleting row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pk.Columns),
		zap.Strings("pk_values", pk.Values))

	// DELETE FROM "schema"."table" WHERE "col1" = $1 AND "col2" = $2
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE %s`,
		qt(schema, table), pk.WhereClause(1),
	)

	// Execute delete
	cmdTag, err := pool.Exec(ctx, query, pk.WhereArgs()...)
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
func (c *Client) DeleteRow(ctx context.Context, schema, table string, pk *PKMatch) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}
	return DeleteRow(ctx, c.pool, schema, table, pk)
}

// GetFirstNRows returns the first N primary key values ordered by PK ascending.
// Returns encoded PK strings (comma-delimited for composite PKs).
func GetFirstNRows(ctx context.Context, pool *pgxpool.Pool, schema, table string, pkColumns []string, limit int) ([]string, error) {
	logging.Debug("Getting first N rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pkColumns),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT %s FROM %s ORDER BY %s LIMIT $1`,
		pkSelectList(pkColumns), qt(schema, table), pkOrderByList(pkColumns, "ASC"),
	)

	return scanPKRows(ctx, pool, query, pkColumns, limit)
}

// GetFirstNRows is a convenience wrapper for Client
func (c *Client) GetFirstNRows(ctx context.Context, schema, table string, pkColumns []string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetFirstNRows(ctx, c.pool, schema, table, pkColumns, limit)
}

// GetLastNRows returns the last N primary key values ordered by PK descending.
// Returns encoded PK strings (comma-delimited for composite PKs).
func GetLastNRows(ctx context.Context, pool *pgxpool.Pool, schema, table string, pkColumns []string, limit int) ([]string, error) {
	logging.Debug("Getting last N rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pkColumns),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT %s FROM %s ORDER BY %s LIMIT $1`,
		pkSelectList(pkColumns), qt(schema, table), pkOrderByList(pkColumns, "DESC"),
	)

	return scanPKRows(ctx, pool, query, pkColumns, limit)
}

// GetLastNRows is a convenience wrapper for Client
func (c *Client) GetLastNRows(ctx context.Context, schema, table string, pkColumns []string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetLastNRows(ctx, c.pool, schema, table, pkColumns, limit)
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
func GetRandomSampleRows(ctx context.Context, pool *pgxpool.Pool, schema, table string, pkColumns []string, limit int, estimatedRows int64) ([]string, error) {
	logging.Debug("Getting random sample rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pkColumns),
		zap.Int("limit", limit),
		zap.Int64("estimated_rows", estimatedRows))

	selectCols := pkSelectList(pkColumns)
	var query string

	// Use TABLESAMPLE for large tables (more efficient than ORDER BY RANDOM())
	if estimatedRows > 1000 {
		percentage := float64(limit) * 1.2 / float64(estimatedRows) * 100.0
		if percentage > 100 {
			percentage = 100
		}
		if percentage < 0.001 {
			percentage = 0.001
		}

		query = fmt.Sprintf(
			`SELECT %s FROM %s TABLESAMPLE BERNOULLI(%f) LIMIT $1`,
			selectCols, qt(schema, table), percentage,
		)
	} else {
		query = fmt.Sprintf(
			`SELECT %s FROM %s ORDER BY RANDOM() LIMIT $1`,
			selectCols, qt(schema, table),
		)
	}

	return scanPKRows(ctx, pool, query, pkColumns, limit)
}

// GetRandomSampleRows is a convenience wrapper for Client
func (c *Client) GetRandomSampleRows(ctx context.Context, schema, table string, pkColumns []string, limit int, estimatedRows int64) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetRandomSampleRows(ctx, c.pool, schema, table, pkColumns, limit, estimatedRows)
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
func GetFirstNRowsOrdered(ctx context.Context, pool *pgxpool.Pool, schema, table string, pkColumns []string, orderColumn string, limit int) ([]string, error) {
	logging.Debug("Getting first N rows ordered by column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pkColumns),
		zap.String("order_column", orderColumn),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT %s FROM %s ORDER BY %s ASC NULLS LAST, %s LIMIT $1`,
		pkSelectList(pkColumns), qt(schema, table), qi(orderColumn), pkOrderByList(pkColumns, "ASC"),
	)

	return scanPKRows(ctx, pool, query, pkColumns, limit)
}

// GetFirstNRowsOrdered is a convenience wrapper for Client
func (c *Client) GetFirstNRowsOrdered(ctx context.Context, schema, table string, pkColumns []string, orderColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetFirstNRowsOrdered(ctx, c.pool, schema, table, pkColumns, orderColumn, limit)
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
func GetLastNRowsOrdered(ctx context.Context, pool *pgxpool.Pool, schema, table string, pkColumns []string, orderColumn string, limit int) ([]string, error) {
	logging.Debug("Getting last N rows ordered by column",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pkColumns),
		zap.String("order_column", orderColumn),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT %s FROM %s ORDER BY %s DESC NULLS LAST, %s LIMIT $1`,
		pkSelectList(pkColumns), qt(schema, table), qi(orderColumn), pkOrderByList(pkColumns, "DESC"),
	)

	return scanPKRows(ctx, pool, query, pkColumns, limit)
}

// GetLastNRowsOrdered is a convenience wrapper for Client
func (c *Client) GetLastNRowsOrdered(ctx context.Context, schema, table string, pkColumns []string, orderColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetLastNRowsOrdered(ctx, c.pool, schema, table, pkColumns, orderColumn, limit)
}

// scanPKRows executes a query with a LIMIT $1 parameter, scans PK columns, and
// returns encoded PK strings. Shared by all pagination functions.
func scanPKRows(ctx context.Context, pool *pgxpool.Pool, query string, pkColumns []string, limit int) ([]string, error) {
	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows: %w", err)
	}
	defer rows.Close()

	var pks []string
	scanDests := make([]interface{}, len(pkColumns))
	for rows.Next() {
		// Create fresh scan destinations for each row
		vals := make([]interface{}, len(pkColumns))
		for i := range vals {
			scanDests[i] = &vals[i]
		}
		if err := rows.Scan(scanDests...); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pkStr, err := scanAndEncodePK(vals, pkColumns)
		if err != nil {
			return nil, err
		}
		pks = append(pks, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return pks, nil
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
		`UPDATE %s SET %s = $1 || substr(%s, length($2) + 1) WHERE %s = $2 OR %s LIKE $2 || '/%%'`,
		qt(schema, table), qi(column), qi(column), qi(column), qi(column),
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
		`SELECT EXISTS(SELECT 1 FROM %s WHERE %s LIKE $1 || '/%%')`,
		qt(schema, table), qi(column),
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
		quotedCols[i] = qi(col)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	// Plain INSERT without ON CONFLICT -- deferrable unique constraints (required
	// by ADR-017 for undo transactions) don't support ON CONFLICT as arbiter.
	// Instead, catch the unique violation error (23505) and treat it as a no-op.
	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		qt(schema, table), strings.Join(quotedCols, ", "), strings.Join(placeholders, ", "),
	)

	_, err := c.pool.Exec(ctx, query, values...)
	if err != nil {
		if isUniqueViolation(err) {
			return nil // Row already exists, no-op
		}
		return fmt.Errorf("failed to insert if not exists: %w", err)
	}

	return nil
}

// GetRowsByParent returns rows with a specific parent_id, up to limit.
// Empty parentID means root level (WHERE parent_id IS NULL).
// Used by the parent-pointer directory model (ADR-017) for ReadDir.
func (c *Client) GetRowsByParent(ctx context.Context, schema, table, parentID string, limit int) ([]string, [][]interface{}, error) {
	if c.pool == nil {
		return nil, nil, fmt.Errorf("database connection not initialized")
	}

	var query string
	var args []interface{}
	if parentID == "" {
		query = fmt.Sprintf(
			`SELECT * FROM %s WHERE "parent_id" IS NULL LIMIT $1`,
			qt(schema, table),
		)
		args = []interface{}{limit}
	} else {
		query = fmt.Sprintf(
			`SELECT * FROM %s WHERE "parent_id" = $1 LIMIT $2`,
			qt(schema, table),
		)
		args = []interface{}{parentID, limit}
	}

	return c.queryRows(ctx, query, args...)
}

// GetRowByParentAndName returns a single row matching parent_id + filename.
// Empty parentID means root level (WHERE parent_id IS NULL).
// Returns (columns, row, error); row is nil if not found.
// Used by ReadFile to combine path resolution with row fetch in one query (ADR-017).
func (c *Client) GetRowByParentAndName(ctx context.Context, schema, table, parentID, filename string) ([]string, []interface{}, error) {
	if c.pool == nil {
		return nil, nil, fmt.Errorf("database connection not initialized")
	}

	var query string
	var args []interface{}
	if parentID == "" {
		query = fmt.Sprintf(
			`SELECT * FROM %s WHERE "parent_id" IS NULL AND "filename" = $1 LIMIT 1`,
			qt(schema, table),
		)
		args = []interface{}{filename}
	} else {
		query = fmt.Sprintf(
			`SELECT * FROM %s WHERE "parent_id" = $1 AND "filename" = $2 LIMIT 1`,
			qt(schema, table),
		)
		args = []interface{}{parentID, filename}
	}

	columns, rows, err := c.queryRows(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return columns, nil, nil
	}
	return columns, rows[0], nil
}

// QueryNextLogEntry finds the next log entry for a file after a given log_id.
// Returns the version_id and filename of the next entry, or empty strings if none.
// Used by diff symlink resolution to determine the "after" state.
func (c *Client) QueryNextLogEntry(ctx context.Context, schema, logTable, fileID, afterLogID string) (string, string, error) {
	if c.pool == nil {
		return "", "", fmt.Errorf("database connection not initialized")
	}

	query := fmt.Sprintf(
		`SELECT COALESCE("version_id"::text, ''), "filename" FROM %s WHERE "file_id" = $1 AND "log_id" > $2 ORDER BY "log_id" ASC LIMIT 1`,
		qt(schema, logTable),
	)

	var versionID, filename string
	err := c.pool.QueryRow(ctx, query, fileID, afterLogID).Scan(&versionID, &filename)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return "", "", nil // No next entry
		}
		return "", "", fmt.Errorf("failed to query next log entry: %w", err)
	}
	return versionID, filename, nil
}

// QueryFileExists checks if a row with the given id exists in the source table.
// Used by diff symlink resolution to determine if a file still exists.
func (c *Client) QueryFileExists(ctx context.Context, schema, table, fileID string) (bool, error) {
	if c.pool == nil {
		return false, fmt.Errorf("database connection not initialized")
	}

	query := fmt.Sprintf(
		`SELECT EXISTS(SELECT 1 FROM %s WHERE "id" = $1)`,
		qt(schema, table),
	)

	var exists bool
	err := c.pool.QueryRow(ctx, query, fileID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check file existence: %w", err)
	}
	return exists, nil
}

// QueryUndoAffectedFiles returns the first log entry per file after a target point.
// Uses DISTINCT ON to find one entry per file_id, ordered by log_id ASC (oldest first).
// TimescaleDB's SkipScan optimizes this on the (file_id, log_id ASC) index.
func (c *Client) QueryUndoAffectedFiles(ctx context.Context, schema, logTable, afterID, userID string, filters []UndoFilter) ([]UndoAffectedFile, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	// Build WHERE clause
	conditions := []string{fmt.Sprintf("%s > $1", qi("log_id"))}
	args := []interface{}{afterID}
	argIdx := 2

	if userID != "" {
		conditions = append(conditions, fmt.Sprintf("%s = $%d", qi("user_id"), argIdx))
		args = append(args, userID)
		argIdx++
	}

	for _, f := range filters {
		conditions = append(conditions, fmt.Sprintf("%s = $%d", qi(f.Column), argIdx))
		args = append(args, f.Value)
		argIdx++
	}

	where := strings.Join(conditions, " AND ")

	query := fmt.Sprintf(
		`SELECT DISTINCT ON (%s) %s, %s, %s, %s FROM %s WHERE %s ORDER BY %s, %s ASC`,
		qi("file_id"),
		qi("file_id"), qi("type"), qi("version_id"), qi("filename"),
		qt(schema, logTable),
		where,
		qi("file_id"), qi("log_id"),
	)

	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query undo affected files: %w", err)
	}
	defer rows.Close()

	var result []UndoAffectedFile
	for rows.Next() {
		var f UndoAffectedFile
		var versionID *string
		if err := rows.Scan(&f.FileID, &f.Type, &versionID, &f.Filename); err != nil {
			return nil, fmt.Errorf("failed to scan undo affected file: %w", err)
		}
		if versionID != nil {
			f.VersionID = *versionID
		}
		result = append(result, f)
	}
	return result, nil
}

// QueryLogEntry fetches a single log entry by log_id.
func (c *Client) QueryLogEntry(ctx context.Context, schema, logTable, logID string) (*UndoAffectedFile, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	query := fmt.Sprintf(
		`SELECT %s, %s, %s, %s FROM %s WHERE %s = $1`,
		qi("file_id"), qi("type"), qi("version_id"), qi("filename"),
		qt(schema, logTable),
		qi("log_id"),
	)

	var f UndoAffectedFile
	var versionID *string
	err := c.pool.QueryRow(ctx, query, logID).Scan(&f.FileID, &f.Type, &versionID, &f.Filename)
	if err != nil {
		return nil, fmt.Errorf("log entry not found: %s", logID)
	}
	if versionID != nil {
		f.VersionID = *versionID
	}
	return &f, nil
}

// ExecuteUndoTransaction executes a batch of undo operations atomically.
// Within a single transaction: deletes created rows, upserts from history,
// and inserts undo log entries. BEFORE triggers fire on each restore,
// creating history entries for the undo itself.
func (c *Client) ExecuteUndoTransaction(ctx context.Context, params *UndoTransactionParams) error {
	if c.pool == nil {
		return fmt.Errorf("database connection not initialized")
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin undo transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	sourceTable := qt("public", params.SourceTable)
	historyTable := qt(params.Schema, params.HistoryTable)
	logTable := qt(params.Schema, params.LogTable)

	// Step 1: DELETE rows that were created after the target point.
	for _, fileID := range params.DeleteFileIDs {
		_, err := tx.Exec(ctx,
			fmt.Sprintf(`DELETE FROM %s WHERE %s = $1`, sourceTable, qi("id")),
			fileID,
		)
		if err != nil {
			return fmt.Errorf("failed to delete created row %s: %w", fileID, err)
		}
	}

	// Step 2: UPSERT rows from history for edits/renames/deletes.
	// Fetch each history row by version_id and restore it.
	for i, versionID := range params.RestoreVersionIDs {
		fileID := params.RestoreFileIDs[i]

		// Fetch history row -- get all columns dynamically
		historyRow, err := tx.Query(ctx,
			fmt.Sprintf(`SELECT * FROM %s WHERE %s = $1`, historyTable, qi("version_id")),
			versionID,
		)
		if err != nil {
			return fmt.Errorf("failed to fetch history for version %s: %w", versionID, err)
		}

		if !historyRow.Next() {
			historyRow.Close()
			return fmt.Errorf("history entry not found for version_id %s", versionID)
		}

		// Get column descriptions from the result set
		fieldDescs := historyRow.FieldDescriptions()
		values, err := historyRow.Values()
		historyRow.Close()
		if err != nil {
			return fmt.Errorf("failed to read history row for version %s: %w", versionID, err)
		}

		// Build column/value maps, excluding history-only columns
		var sourceCols []string
		var sourceVals []interface{}
		for j, fd := range fieldDescs {
			colName := string(fd.Name)
			// Skip history-only columns (version_id, operation are not in the source table)
			if colName == "version_id" || colName == "operation" {
				continue
			}
			sourceCols = append(sourceCols, colName)
			sourceVals = append(sourceVals, values[j])
		}

		// Build UPSERT: INSERT ... ON CONFLICT (id) DO UPDATE SET ...
		placeholders := make([]string, len(sourceCols))
		quotedCols := make([]string, len(sourceCols))
		updateSet := make([]string, 0, len(sourceCols))
		for j, col := range sourceCols {
			placeholders[j] = fmt.Sprintf("$%d", j+1)
			quotedCols[j] = qi(col)
			if col != "id" { // Don't update the PK
				updateSet = append(updateSet, fmt.Sprintf("%s = EXCLUDED.%s", qi(col), qi(col)))
			}
		}

		upsertSQL := fmt.Sprintf(
			`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s`,
			sourceTable,
			strings.Join(quotedCols, ", "),
			strings.Join(placeholders, ", "),
			qi("id"),
			strings.Join(updateSet, ", "),
		)

		_, err = tx.Exec(ctx, upsertSQL, sourceVals...)
		if err != nil {
			return fmt.Errorf("failed to restore file %s from history: %w", fileID, err)
		}
	}

	// Step 3: Insert undo log entries for each affected file.
	// DELETE targets get a log entry too (we're undoing the creation).
	for _, fileID := range params.DeleteFileIDs {
		_, err := tx.Exec(ctx,
			fmt.Sprintf(
				`INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (uuidv7(), $1, 'undo', $2, $3)`,
				logTable, qi("log_id"), qi("file_id"), qi("type"), qi("user_id"), qi("description"),
			),
			fileID, params.UserID, params.Description,
		)
		if err != nil {
			return fmt.Errorf("failed to insert undo log entry for delete: %w", err)
		}
	}
	for i, fileID := range params.RestoreFileIDs {
		// Capture version_id of the state just before our restore (the BEFORE trigger has fired)
		var latestVersionID *string
		err := tx.QueryRow(ctx,
			fmt.Sprintf(
				`SELECT %s FROM %s WHERE %s = $1 ORDER BY %s DESC LIMIT 1`,
				qi("version_id"), qt(params.Schema, params.HistoryTable),
				qi("file_id"), qi("version_id"),
			),
			fileID,
		).Scan(&latestVersionID)
		if err != nil {
			latestVersionID = nil
		}

		versionIDVal := ""
		if latestVersionID != nil {
			versionIDVal = *latestVersionID
		}

		_, err = tx.Exec(ctx,
			fmt.Sprintf(
				`INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (uuidv7(), $1, 'undo', $2, $3, $4, $5)`,
				logTable,
				qi("log_id"), qi("file_id"), qi("type"), qi("user_id"),
				qi("filename"), qi("version_id"), qi("description"),
			),
			fileID, params.UserID, params.RestoreFilenames[i], versionIDVal, params.Description,
		)
		if err != nil {
			return fmt.Errorf("failed to insert undo log entry for restore: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit undo transaction: %w", err)
	}
	return nil
}

// HasExtension checks if a PostgreSQL extension is installed in the database.
func (c *Client) HasExtension(ctx context.Context, extName string) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = $1)`,
		extName,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check extension %q: %w", extName, err)
	}
	return exists, nil
}

// TableExists checks if a table exists in the given schema.
func (c *Client) TableExists(ctx context.Context, schema, table string) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`,
		schema, table,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table %q.%q: %w", schema, table, err)
	}
	return exists, nil
}

// QueryHistoryByFilename queries the history table for versions of a file by filename.
// Returns columns and rows ordered by version_id DESC (most recent first).
func (c *Client) QueryHistoryByFilename(ctx context.Context, schema, historyTable, filename string, limit int) ([]string, [][]interface{}, error) {
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE "filename" = $1 ORDER BY "version_id" DESC LIMIT %d`,
		qt(schema, historyTable), limit,
	)
	return c.queryRows(ctx, query, filename)
}

// QueryHistoryByID queries the history table for versions of a row by its file_id UUID.
// Returns columns and rows ordered by version_id DESC (most recent first).
func (c *Client) QueryHistoryByID(ctx context.Context, schema, historyTable, rowID string, limit int) ([]string, [][]interface{}, error) {
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE "file_id" = $1 ORDER BY "version_id" DESC LIMIT %d`,
		qt(schema, historyTable), limit,
	)
	return c.queryRows(ctx, query, rowID)
}

// QueryHistoryDistinctFilenames returns distinct filenames from the history table.
func (c *Client) QueryHistoryDistinctFilenames(ctx context.Context, schema, historyTable string, limit int) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT DISTINCT "filename" FROM %s ORDER BY "filename" LIMIT %d`,
		qt(schema, historyTable), limit,
	)
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query distinct filenames: %w", err)
	}
	defer rows.Close()

	var filenames []string
	for rows.Next() {
		var fn string
		if err := rows.Scan(&fn); err != nil {
			return nil, fmt.Errorf("failed to scan filename: %w", err)
		}
		filenames = append(filenames, fn)
	}
	return filenames, nil
}

// QueryHistoryDistinctFilenamesByParent returns distinct filenames from the history table
// filtered by parent_id. Empty parentID means root level (WHERE parent_id IS NULL).
// Used by the parent-pointer model (ADR-017) for per-directory .history/ listing.
func (c *Client) QueryHistoryDistinctFilenamesByParent(ctx context.Context, schema, historyTable, parentID string, limit int) ([]string, error) {
	var query string
	var args []interface{}
	if parentID == "" {
		query = fmt.Sprintf(
			`SELECT DISTINCT "filename" FROM %s WHERE "parent_id" IS NULL ORDER BY "filename" LIMIT %d`,
			qt(schema, historyTable), limit,
		)
	} else {
		query = fmt.Sprintf(
			`SELECT DISTINCT "filename" FROM %s WHERE "parent_id" = $1 ORDER BY "filename" LIMIT %d`,
			qt(schema, historyTable), limit,
		)
		args = []interface{}{parentID}
	}

	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query distinct filenames by parent: %w", err)
	}
	defer rows.Close()

	var filenames []string
	for rows.Next() {
		var fn string
		if err := rows.Scan(&fn); err != nil {
			return nil, fmt.Errorf("failed to scan filename: %w", err)
		}
		filenames = append(filenames, fn)
	}
	return filenames, nil
}

// QueryHistoryDistinctIDs returns distinct row UUIDs (file_id) from the history table.
func (c *Client) QueryHistoryDistinctIDs(ctx context.Context, schema, historyTable string, limit int) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT DISTINCT "file_id"::text FROM %s ORDER BY "file_id" LIMIT %d`,
		qt(schema, historyTable), limit,
	)
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query distinct IDs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// QueryHistoryVersionByTime finds a history row matching a version ID timestamp.
// Version IDs have second precision; UUIDv7 has millisecond precision. This queries
// with a 1-second window around the target timestamp plus filename or file_id filter.
func (c *Client) QueryHistoryVersionByTime(ctx context.Context, schema, historyTable, filterColumn, filterValue string, targetTime interface{}, limit int) ([]string, [][]interface{}, error) {
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE %s = $1 ORDER BY "version_id" DESC LIMIT %d`,
		qt(schema, historyTable), qi(filterColumn), limit,
	)
	return c.queryRows(ctx, query, filterValue)
}

// InsertLogEntry inserts an operation log entry into the log hypertable.
func (c *Client) InsertLogEntry(ctx context.Context, schema, logTable, userID, opType, fileID, filename, versionID, description string) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (user_id, type, file_id, filename, version_id, description) VALUES ($1, $2, $3, $4, $5, $6)`,
		qt(schema, logTable),
	)

	// Convert empty strings to nil for nullable columns
	var userIDVal, versionIDVal, descVal interface{}
	if userID != "" {
		userIDVal = userID
	}
	if versionID != "" {
		versionIDVal = versionID
	}
	if description != "" {
		descVal = description
	}

	_, err := c.pool.Exec(ctx, query, userIDVal, opType, fileID, filename, versionIDVal, descVal)
	if err != nil {
		return fmt.Errorf("failed to insert log entry: %w", err)
	}
	return nil
}

// QueryLatestVersionID returns the most recent version_id for a given
// file_id from the history table. Returns empty string if no history entry found.
func (c *Client) QueryLatestVersionID(ctx context.Context, schema, historyTable, fileID string) (string, error) {
	query := fmt.Sprintf(
		`SELECT "version_id" FROM %s WHERE "file_id" = $1 ORDER BY "version_id" DESC LIMIT 1`,
		qt(schema, historyTable),
	)
	var versionID string
	err := c.pool.QueryRow(ctx, query, fileID).Scan(&versionID)
	if err != nil {
		return "", fmt.Errorf("failed to query latest version ID: %w", err)
	}
	return versionID, nil
}

// PathSegment represents one resolved segment from the resolve_path function.
type PathSegment struct {
	Depth int    // 1-based depth in the resolved path
	ID    string // UUID of the resolved row
	Name  string // filename segment that was resolved
}

// ResolvePath calls the tigerfs.resolve_path PL/pgSQL function to resolve
// a sequence of path segments to row IDs using the parent-pointer model.
// startParentID is the UUID of the starting parent (empty string for root/NULL).
// Returns one PathSegment per resolved segment. If a segment doesn't resolve,
// fewer segments are returned than requested (no error).
func (c *Client) ResolvePath(ctx context.Context, schema, table, startParentID string, segments []string) ([]PathSegment, error) {
	if len(segments) == 0 {
		return nil, nil
	}

	qualifiedTable := fmt.Sprintf("%s.%s", qi(schema), qi(table))

	// Convert empty startParentID to SQL NULL
	var parentArg interface{}
	if startParentID != "" {
		parentArg = startParentID
	}

	query := fmt.Sprintf(
		`SELECT depth, resolved_id, resolved_name FROM %s.resolve_path($1::regclass, $2::uuid, $3::text[])`,
		qi("tigerfs"),
	)

	rows, err := c.pool.Query(ctx, query, qualifiedTable, parentArg, segments)
	if err != nil {
		return nil, fmt.Errorf("resolve_path failed: %w", err)
	}
	defer rows.Close()

	var result []PathSegment
	for rows.Next() {
		var seg PathSegment
		var id [16]byte
		if err := rows.Scan(&seg.Depth, &id, &seg.Name); err != nil {
			return nil, fmt.Errorf("failed to scan resolve_path result: %w", err)
		}
		// Convert UUID bytes to hex string
		s, _ := format.ConvertValueToText(id)
		seg.ID = s
		result = append(result, seg)
	}

	return result, nil
}

// queryRows executes a query and returns columns and row data.
func (c *Client) queryRows(ctx context.Context, query string, args ...interface{}) ([]string, [][]interface{}, error) {
	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	var result [][]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, values)
	}

	return columns, result, nil
}
