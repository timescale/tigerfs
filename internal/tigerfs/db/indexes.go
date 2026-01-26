package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// Index represents metadata about a database index.
// Used by the FUSE layer to expose index-based navigation paths
// (e.g., .email/ for single-column, .last_name.first_name/ for composite).
type Index struct {
	// Name is the PostgreSQL index name (e.g., "users_email_idx")
	Name string

	// Columns lists the indexed column names in index order.
	// For composite indexes, the order determines navigation hierarchy.
	Columns []string

	// IsUnique indicates whether this is a unique constraint index.
	// Unique indexes guarantee at most one row per lookup value.
	IsUnique bool

	// IsPrimary indicates whether this is the primary key index.
	// Primary key indexes are excluded from .column/ navigation
	// since rows are already accessible by PK directly.
	IsPrimary bool

	// Definition contains the full CREATE INDEX statement.
	// Useful for .indexes metadata file output.
	Definition string
}

// IsComposite returns true if the index spans multiple columns.
// Composite indexes use nested directory navigation (e.g., .last_name/Smith/first_name/John/).
func (i *Index) IsComposite() bool {
	return len(i.Columns) > 1
}

// GetIndexes retrieves all indexes for a table from PostgreSQL catalog.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: Database connection pool
//   - schema: Schema name (e.g., "public")
//   - table: Table name
//
// Returns all indexes including primary key, unique constraints, and regular indexes.
// Returns empty slice (not error) if table has no indexes.
//
// Uses pg_catalog instead of parsing indexdef strings for reliability.
func GetIndexes(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]Index, error) {
	logging.Debug("Querying indexes",
		zap.String("schema", schema),
		zap.String("table", table))

	// Query pg_catalog tables to get structured index metadata.
	// Uses unnest with ordinality to preserve column order in composite indexes.
	query := `
		SELECT
			i.relname AS index_name,
			ix.indisunique AS is_unique,
			ix.indisprimary AS is_primary,
			pg_get_indexdef(i.oid) AS index_definition,
			array_agg(a.attname ORDER BY x.ordinality) AS columns
		FROM
			pg_class t
			JOIN pg_index ix ON t.oid = ix.indrelid
			JOIN pg_class i ON i.oid = ix.indexrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ordinality)
			JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
		WHERE
			n.nspname = $1
			AND t.relname = $2
		GROUP BY
			i.relname, ix.indisunique, ix.indisprimary, i.oid
		ORDER BY
			i.relname
	`

	rows, err := pool.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	var indexes []Index
	for rows.Next() {
		var idx Index
		var columns []string

		if err := rows.Scan(&idx.Name, &idx.IsUnique, &idx.IsPrimary, &idx.Definition, &columns); err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}
		idx.Columns = columns
		indexes = append(indexes, idx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating indexes: %w", err)
	}

	logging.Debug("Found indexes",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("count", len(indexes)))

	return indexes, nil
}

// GetIndexes is a convenience wrapper for Client.
func (c *Client) GetIndexes(ctx context.Context, schema, table string) ([]Index, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetIndexes(ctx, c.pool, schema, table)
}

// GetIndexByColumn finds the first index whose leading column matches.
// Returns nil (not error) if no matching index exists.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: Database connection pool
//   - schema: Schema name
//   - table: Table name
//   - column: Column name to match against first index column
//
// Only matches indexes where the specified column is the leading (first) column,
// since PostgreSQL can only use an index efficiently when querying by leading columns.
func GetIndexByColumn(ctx context.Context, pool *pgxpool.Pool, schema, table, column string) (*Index, error) {
	indexes, err := GetIndexes(ctx, pool, schema, table)
	if err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		if len(idx.Columns) > 0 && idx.Columns[0] == column {
			return &idx, nil
		}
	}

	return nil, nil
}

// GetIndexByColumn is a convenience wrapper for Client.
func (c *Client) GetIndexByColumn(ctx context.Context, schema, table, column string) (*Index, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetIndexByColumn(ctx, c.pool, schema, table, column)
}

// GetSingleColumnIndexes returns indexes with exactly one column.
// These are suitable for simple .column/ navigation paths.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: Database connection pool
//   - schema: Schema name
//   - table: Table name
//
// Excludes composite indexes. Returns empty slice if none found.
func GetSingleColumnIndexes(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]Index, error) {
	indexes, err := GetIndexes(ctx, pool, schema, table)
	if err != nil {
		return nil, err
	}

	var result []Index
	for _, idx := range indexes {
		if !idx.IsComposite() {
			result = append(result, idx)
		}
	}

	return result, nil
}

// GetSingleColumnIndexes is a convenience wrapper for Client.
func (c *Client) GetSingleColumnIndexes(ctx context.Context, schema, table string) ([]Index, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetSingleColumnIndexes(ctx, c.pool, schema, table)
}

// GetCompositeIndexes returns indexes with multiple columns.
// These are suitable for nested .col1.col2/ navigation paths.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: Database connection pool
//   - schema: Schema name
//   - table: Table name
//
// Excludes single-column indexes. Returns empty slice if none found.
func GetCompositeIndexes(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]Index, error) {
	indexes, err := GetIndexes(ctx, pool, schema, table)
	if err != nil {
		return nil, err
	}

	var result []Index
	for _, idx := range indexes {
		if idx.IsComposite() {
			result = append(result, idx)
		}
	}

	return result, nil
}

// GetCompositeIndexes is a convenience wrapper for Client.
func (c *Client) GetCompositeIndexes(ctx context.Context, schema, table string) ([]Index, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetCompositeIndexes(ctx, c.pool, schema, table)
}

// GetDistinctValues retrieves distinct values for an indexed column.
// Used by the FUSE layer to list contents of index directories (e.g., ls .email/).
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: Database connection pool
//   - schema: Schema name
//   - table: Table name
//   - column: Column name to get distinct values for
//   - limit: Maximum number of values to return (prevents huge listings)
//
// Returns values as strings. NULL values are excluded.
// Values are ordered for consistent directory listings.
func GetDistinctValues(ctx context.Context, pool *pgxpool.Pool, schema, table, column string, limit int) ([]string, error) {
	logging.Debug("Querying distinct values",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("column", column),
		zap.Int("limit", limit))

	// Query distinct non-null values, ordered for consistent listings
	query := fmt.Sprintf(
		`SELECT DISTINCT "%s" FROM "%s"."%s" WHERE "%s" IS NOT NULL ORDER BY "%s" LIMIT $1`,
		column, schema, table, column, column,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query distinct values: %w", err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var value interface{}
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("failed to scan distinct value: %w", err)
		}
		// Convert to string representation
		values = append(values, fmt.Sprintf("%v", value))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating distinct values: %w", err)
	}

	logging.Debug("Found distinct values",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("column", column),
		zap.Int("count", len(values)))

	return values, nil
}

// GetDistinctValues is a convenience wrapper for Client.
func (c *Client) GetDistinctValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetDistinctValues(ctx, c.pool, schema, table, column, limit)
}

// GetRowsByIndexValue retrieves primary keys of rows matching an indexed column value.
// Used by the FUSE layer to resolve index paths like .email/foo@x.com/.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: Database connection pool
//   - schema: Schema name
//   - table: Table name
//   - column: Indexed column to query
//   - value: Value to match
//   - pkColumn: Primary key column to return
//   - limit: Maximum number of rows to return
//
// Returns primary key values as strings. If no rows match, returns empty slice (not error).
func GetRowsByIndexValue(ctx context.Context, pool *pgxpool.Pool, schema, table, column, value, pkColumn string, limit int) ([]string, error) {
	logging.Debug("Querying rows by index value",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("column", column),
		zap.String("value", value),
		zap.String("pk_column", pkColumn),
		zap.Int("limit", limit))

	// Query rows matching the index value, returning PKs
	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" WHERE "%s" = $1 ORDER BY "%s" LIMIT $2`,
		pkColumn, schema, table, column, pkColumn,
	)

	rows, err := pool.Query(ctx, query, value, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query by index value: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk interface{}
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pks = append(pks, fmt.Sprintf("%v", pk))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Found rows by index value",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("column", column),
		zap.String("value", value),
		zap.Int("count", len(pks)))

	return pks, nil
}

// GetRowsByIndexValue is a convenience wrapper for Client.
func (c *Client) GetRowsByIndexValue(ctx context.Context, schema, table, column, value, pkColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetRowsByIndexValue(ctx, c.pool, schema, table, column, value, pkColumn, limit)
}

// GetDistinctValuesFiltered retrieves distinct values for a column filtered by conditions on other columns.
// Used by the FUSE layer to navigate composite indexes (e.g., ls .last_name.first_name/Smith/).
//
// For example, when navigating .last_name.first_name/Smith/, this function retrieves distinct
// first_name values where last_name='Smith'. DISTINCT is used because the directory listing
// should show each possible "next value" once, even if multiple rows share that value.
// With 50 users named "Smith, John" and 30 named "Smith, Alice", the directory shows
// just ["Alice", "John"], not 80 duplicate entries.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: Database connection pool
//   - schema: Schema name
//   - table: Table name
//   - targetColumn: Column to get distinct values for (the "next" column in navigation)
//   - filterColumns: Column names for WHERE conditions (previous columns in navigation)
//   - filterValues: Values to match for each filter column (must match filterColumns length)
//   - limit: Maximum number of values to return
//
// Returns values as strings. NULL values are excluded.
// filterColumns and filterValues must have the same length.
func GetDistinctValuesFiltered(ctx context.Context, pool *pgxpool.Pool, schema, table, targetColumn string, filterColumns, filterValues []string, limit int) ([]string, error) {
	if len(filterColumns) != len(filterValues) {
		return nil, fmt.Errorf("filterColumns and filterValues length mismatch: %d vs %d", len(filterColumns), len(filterValues))
	}

	logging.Debug("Querying filtered distinct values",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("target_column", targetColumn),
		zap.Strings("filter_columns", filterColumns),
		zap.Int("limit", limit))

	// Build WHERE clause with conditions for each filter column
	// Start with the target column IS NOT NULL condition
	whereParts := []string{fmt.Sprintf(`"%s" IS NOT NULL`, targetColumn)}
	args := make([]interface{}, 0, len(filterValues)+1)

	for i, col := range filterColumns {
		whereParts = append(whereParts, fmt.Sprintf(`"%s" = $%d`, col, i+1))
		args = append(args, filterValues[i])
	}
	args = append(args, limit)

	query := fmt.Sprintf(
		`SELECT DISTINCT "%s" FROM "%s"."%s" WHERE %s ORDER BY "%s" LIMIT $%d`,
		targetColumn, schema, table,
		joinStrings(whereParts, " AND "),
		targetColumn, len(args),
	)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query filtered distinct values: %w", err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var value interface{}
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("failed to scan filtered distinct value: %w", err)
		}
		values = append(values, fmt.Sprintf("%v", value))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating filtered distinct values: %w", err)
	}

	logging.Debug("Found filtered distinct values",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("target_column", targetColumn),
		zap.Int("count", len(values)))

	return values, nil
}

// GetDistinctValuesFiltered is a convenience wrapper for Client.
func (c *Client) GetDistinctValuesFiltered(ctx context.Context, schema, table, targetColumn string, filterColumns, filterValues []string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetDistinctValuesFiltered(ctx, c.pool, schema, table, targetColumn, filterColumns, filterValues, limit)
}

// GetRowsByCompositeIndex retrieves primary keys of rows matching multiple column conditions.
// Used by the FUSE layer to resolve composite index paths like .last_name.first_name/Smith/John/.
//
// Unlike GetDistinctValuesFiltered, this returns actual row PKs (no DISTINCT) because at the
// final navigation level we want to list all matching rows, not collapse them.
//
// Generates a query with multiple WHERE conditions:
//
//	SELECT pk FROM table WHERE col1=$1 AND col2=$2 ... ORDER BY pk LIMIT $n
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: Database connection pool
//   - schema: Schema name
//   - table: Table name
//   - columns: Indexed columns to query (in index order)
//   - values: Values to match for each column (must match columns length)
//   - pkColumn: Primary key column to return
//   - limit: Maximum number of rows to return
//
// Returns primary key values as strings. If no rows match, returns empty slice (not error).
func GetRowsByCompositeIndex(ctx context.Context, pool *pgxpool.Pool, schema, table string, columns, values []string, pkColumn string, limit int) ([]string, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("columns and values length mismatch: %d vs %d", len(columns), len(values))
	}

	logging.Debug("Querying rows by composite index",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("columns", columns),
		zap.String("pk_column", pkColumn),
		zap.Int("limit", limit))

	// Build WHERE clause with conditions for each column
	whereParts := make([]string, len(columns))
	args := make([]interface{}, 0, len(values)+1)

	for i, col := range columns {
		whereParts[i] = fmt.Sprintf(`"%s" = $%d`, col, i+1)
		args = append(args, values[i])
	}
	args = append(args, limit)

	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" WHERE %s ORDER BY "%s" LIMIT $%d`,
		pkColumn, schema, table,
		joinStrings(whereParts, " AND "),
		pkColumn, len(args),
	)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query by composite index: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk interface{}
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pks = append(pks, fmt.Sprintf("%v", pk))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Found rows by composite index",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("columns", columns),
		zap.Int("count", len(pks)))

	return pks, nil
}

// GetRowsByCompositeIndex is a convenience wrapper for Client.
func (c *Client) GetRowsByCompositeIndex(ctx context.Context, schema, table string, columns, values []string, pkColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetRowsByCompositeIndex(ctx, c.pool, schema, table, columns, values, pkColumn, limit)
}

// joinStrings joins strings with a separator (helper to avoid importing strings package).
func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}
