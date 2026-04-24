package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

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
func scanAndEncodePK(values []interface{}, pkColumns []string) (string, error) {
	if len(pkColumns) == 1 {
		// Single-column: convert directly
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
func GetRow(ctx context.Context, dbtx DBTX, schema, table string, pk *PKMatch) (*Row, error) {
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

	rows, err := dbtx.Query(ctx, query, pk.WhereArgs()...)
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
func (c *Client) GetRow(ctx context.Context, schema, table string, pk *PKMatch) (result *Row, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetRow(ctx, q, schema, table, pk)
}

// GetColumn fetches a single column value from a row by primary key (single or composite).
func GetColumn(ctx context.Context, dbtx DBTX, schema, table string, pk *PKMatch, columnName string) (interface{}, error) {
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
	err := dbtx.QueryRow(ctx, query, pk.WhereArgs()...).Scan(&value)
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
func (c *Client) GetColumn(ctx context.Context, schema, table string, pk *PKMatch, columnName string) (result interface{}, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetColumn(ctx, q, schema, table, pk, columnName)
}

// UpdateColumn updates a single column value for a row by primary key (single or composite).
// Empty string is treated as NULL.
func UpdateColumn(ctx context.Context, dbtx DBTX, schema, table string, pk *PKMatch, columnName, newValue string) error {
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
	cmdTag, err := dbtx.Exec(ctx, query, args...)
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
func (c *Client) UpdateColumn(ctx context.Context, schema, table string, pk *PKMatch, columnName, newValue string) (retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return err
	}
	defer func() { done(retErr) }()
	return UpdateColumn(ctx, q, schema, table, pk, columnName, newValue)
}

// UpdateColumnCAS performs a compare-and-swap update on a column.
// Only updates the row if whereColumn still has whereValue (atomic check).
// Returns "row not found" if no row matches, enabling safe concurrent renames.
func (c *Client) UpdateColumnCAS(ctx context.Context, schema, table string, pk *PKMatch, setColumn, newValue, whereColumn, whereValue string) (retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return err
	}
	defer func() { done(retErr) }()

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

	cmdTag, err := q.Exec(ctx, query, args...)
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
func InsertRow(ctx context.Context, dbtx DBTX, schema, table string, columns []string, values []interface{}) (string, error) {
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
	rows, err := dbtx.Query(ctx, query, values...)
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
func (c *Client) InsertRow(ctx context.Context, schema, table string, columns []string, values []interface{}) (result string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return "", err
	}
	defer func() { done(retErr) }()
	return InsertRow(ctx, q, schema, table, columns, values)
}

// UpdateRow updates an existing row with the given column values.
func UpdateRow(ctx context.Context, dbtx DBTX, schema, table string, pk *PKMatch, columns []string, values []interface{}) error {
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
	cmdTag, err := dbtx.Exec(ctx, query, allValues...)
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
func (c *Client) UpdateRow(ctx context.Context, schema, table string, pk *PKMatch, columns []string, values []interface{}) (retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return err
	}
	defer func() { done(retErr) }()
	return UpdateRow(ctx, q, schema, table, pk, columns, values)
}

// DeleteRow deletes a row by primary key (single or composite).
func DeleteRow(ctx context.Context, dbtx DBTX, schema, table string, pk *PKMatch) error {
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
	cmdTag, err := dbtx.Exec(ctx, query, pk.WhereArgs()...)
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
func (c *Client) DeleteRow(ctx context.Context, schema, table string, pk *PKMatch) (retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return err
	}
	defer func() { done(retErr) }()
	return DeleteRow(ctx, q, schema, table, pk)
}

// GetFirstNRows returns the first N primary key values ordered by PK ascending.
// Returns encoded PK strings (comma-delimited for composite PKs).
func GetFirstNRows(ctx context.Context, dbtx DBTX, schema, table string, pkColumns []string, limit int) ([]string, error) {
	logging.Debug("Getting first N rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pkColumns),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT %s FROM %s ORDER BY %s LIMIT $1`,
		pkSelectList(pkColumns), qt(schema, table), pkOrderByList(pkColumns, "ASC"),
	)

	return scanPKRows(ctx, dbtx, query, pkColumns, limit)
}

// GetFirstNRows is a convenience wrapper for Client
func (c *Client) GetFirstNRows(ctx context.Context, schema, table string, pkColumns []string, limit int) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetFirstNRows(ctx, q, schema, table, pkColumns, limit)
}

// GetLastNRows returns the last N primary key values ordered by PK descending.
// Returns encoded PK strings (comma-delimited for composite PKs).
func GetLastNRows(ctx context.Context, dbtx DBTX, schema, table string, pkColumns []string, limit int) ([]string, error) {
	logging.Debug("Getting last N rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("pk_columns", pkColumns),
		zap.Int("limit", limit))

	query := fmt.Sprintf(
		`SELECT %s FROM %s ORDER BY %s LIMIT $1`,
		pkSelectList(pkColumns), qt(schema, table), pkOrderByList(pkColumns, "DESC"),
	)

	return scanPKRows(ctx, dbtx, query, pkColumns, limit)
}

// GetLastNRows is a convenience wrapper for Client
func (c *Client) GetLastNRows(ctx context.Context, schema, table string, pkColumns []string, limit int) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetLastNRows(ctx, q, schema, table, pkColumns, limit)
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
func GetRandomSampleRows(ctx context.Context, dbtx DBTX, schema, table string, pkColumns []string, limit int, estimatedRows int64) ([]string, error) {
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

	return scanPKRows(ctx, dbtx, query, pkColumns, limit)
}

// GetRandomSampleRows is a convenience wrapper for Client
func (c *Client) GetRandomSampleRows(ctx context.Context, schema, table string, pkColumns []string, limit int, estimatedRows int64) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetRandomSampleRows(ctx, q, schema, table, pkColumns, limit, estimatedRows)
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
func GetFirstNRowsOrdered(ctx context.Context, dbtx DBTX, schema, table string, pkColumns []string, orderColumn string, limit int) ([]string, error) {
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

	return scanPKRows(ctx, dbtx, query, pkColumns, limit)
}

// GetFirstNRowsOrdered is a convenience wrapper for Client
func (c *Client) GetFirstNRowsOrdered(ctx context.Context, schema, table string, pkColumns []string, orderColumn string, limit int) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetFirstNRowsOrdered(ctx, q, schema, table, pkColumns, orderColumn, limit)
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
func GetLastNRowsOrdered(ctx context.Context, dbtx DBTX, schema, table string, pkColumns []string, orderColumn string, limit int) ([]string, error) {
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

	return scanPKRows(ctx, dbtx, query, pkColumns, limit)
}

// GetLastNRowsOrdered is a convenience wrapper for Client
func (c *Client) GetLastNRowsOrdered(ctx context.Context, schema, table string, pkColumns []string, orderColumn string, limit int) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()
	return GetLastNRowsOrdered(ctx, q, schema, table, pkColumns, orderColumn, limit)
}

// scanPKRows executes a query with a LIMIT $1 parameter, scans PK columns, and
// returns encoded PK strings. Shared by all pagination functions.
func scanPKRows(ctx context.Context, dbtx DBTX, query string, pkColumns []string, limit int) ([]string, error) {
	rows, err := dbtx.Query(ctx, query, limit)
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
func (c *Client) RenameByPrefix(ctx context.Context, schema, table, column, oldPrefix, newPrefix string) (result int64, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { done(retErr) }()

	// UPDATE "schema"."table" SET "column" = $1 || substr("column", length($2) + 1)
	// WHERE "column" = $2 OR "column" LIKE $2 || '/%'
	query := fmt.Sprintf(
		`UPDATE %s SET %s = $1 || substr(%s, length($2) + 1) WHERE %s = $2 OR %s LIKE $2 || '/%%'`,
		qt(schema, table), qi(column), qi(column), qi(column), qi(column),
	)

	cmdTag, err := q.Exec(ctx, query, newPrefix, oldPrefix)
	if err != nil {
		return 0, fmt.Errorf("failed to rename by prefix: %w", err)
	}

	return cmdTag.RowsAffected(), nil
}

// HasChildrenWithPrefix checks if any rows exist where the given column value
// starts with prefix + "/". Used to check if a directory has children before rmdir.
func (c *Client) HasChildrenWithPrefix(ctx context.Context, schema, table, column, prefix string) (result bool, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return false, err
	}
	defer func() { done(retErr) }()

	query := fmt.Sprintf(
		`SELECT EXISTS(SELECT 1 FROM %s WHERE %s LIKE $1 || '/%%')`,
		qt(schema, table), qi(column),
	)

	var exists bool
	err = q.QueryRow(ctx, query, prefix).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check children: %w", err)
	}

	return exists, nil
}

// InsertIfNotExists inserts a row only if it doesn't already exist (ON CONFLICT DO NOTHING).
// Used for auto-creating parent directory rows in synth views.
func (c *Client) InsertIfNotExists(ctx context.Context, schema, table string, columns []string, values []interface{}) (retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return err
	}
	defer func() { done(retErr) }()

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

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING`,
		qt(schema, table), strings.Join(quotedCols, ", "), strings.Join(placeholders, ", "),
	)

	_, err = q.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert if not exists: %w", err)
	}

	return nil
}

// HasExtension checks if a PostgreSQL extension is installed in the database.
func (c *Client) HasExtension(ctx context.Context, extName string) (result bool, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return false, err
	}
	defer func() { done(retErr) }()

	var exists bool
	err = q.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = $1)`,
		extName,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check extension %q: %w", extName, err)
	}
	return exists, nil
}

// TableExists checks if a table exists in the given schema.
func (c *Client) TableExists(ctx context.Context, schema, table string) (result bool, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return false, err
	}
	defer func() { done(retErr) }()

	var exists bool
	err = q.QueryRow(ctx,
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
// Returns columns and rows ordered by _history_id DESC (most recent first).
func (c *Client) QueryHistoryByFilename(ctx context.Context, schema, historyTable, filename string, limit int) (cols []string, rows [][]interface{}, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { done(retErr) }()

	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE "filename" = $1 ORDER BY "_history_id" DESC LIMIT %d`,
		qt(schema, historyTable), limit,
	)
	return c.queryRows(ctx, q, query, filename)
}

// QueryHistoryByID queries the history table for versions of a row by its UUID.
// Returns columns and rows ordered by _history_id DESC (most recent first).
func (c *Client) QueryHistoryByID(ctx context.Context, schema, historyTable, rowID string, limit int) (cols []string, rows [][]interface{}, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { done(retErr) }()

	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE "id" = $1 ORDER BY "_history_id" DESC LIMIT %d`,
		qt(schema, historyTable), limit,
	)
	return c.queryRows(ctx, q, query, rowID)
}

// QueryHistoryDistinctFilenames returns distinct filenames from the history table.
func (c *Client) QueryHistoryDistinctFilenames(ctx context.Context, schema, historyTable string, limit int) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()

	query := fmt.Sprintf(
		`SELECT DISTINCT "filename" FROM %s ORDER BY "filename" LIMIT %d`,
		qt(schema, historyTable), limit,
	)
	rows, err := q.Query(ctx, query)
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

// QueryHistoryDistinctIDs returns distinct row UUIDs from the history table.
func (c *Client) QueryHistoryDistinctIDs(ctx context.Context, schema, historyTable string, limit int) (result []string, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { done(retErr) }()

	query := fmt.Sprintf(
		`SELECT DISTINCT "id"::text FROM %s ORDER BY "id" LIMIT %d`,
		qt(schema, historyTable), limit,
	)
	rows, err := q.Query(ctx, query)
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
// with a 1-second window around the target timestamp plus filename or id filter.
func (c *Client) QueryHistoryVersionByTime(ctx context.Context, schema, historyTable, filterColumn, filterValue string, targetTime interface{}, limit int) (cols []string, rows [][]interface{}, retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { done(retErr) }()

	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE %s = $1 ORDER BY "_history_id" DESC LIMIT %d`,
		qt(schema, historyTable), qi(filterColumn), limit,
	)
	return c.queryRows(ctx, q, query, filterValue)
}

// queryRows executes a query and returns columns and row data.
func (c *Client) queryRows(ctx context.Context, dbtx DBTX, query string, args ...interface{}) ([]string, [][]interface{}, error) {
	rows, err := dbtx.Query(ctx, query, args...)
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
