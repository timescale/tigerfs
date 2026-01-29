package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// PrimaryKey represents a table's primary key information
type PrimaryKey struct {
	Columns []string // Column names in order
}

// GetPrimaryKey discovers the primary key for a table
// Returns error if no primary key exists or if composite key (not supported in MVP)
func GetPrimaryKey(ctx context.Context, pool *pgxpool.Pool, schema, table string) (*PrimaryKey, error) {
	logging.Debug("Querying primary key",
		zap.String("schema", schema),
		zap.String("table", table))

	query := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = $1
		  AND tc.table_name = $2
		  AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position
	`

	rows, err := pool.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		columns = append(columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating primary key columns: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s.%s has no primary key", schema, table)
	}

	// MVP limitation: only support single-column primary keys
	if len(columns) > 1 {
		return nil, fmt.Errorf("table %s.%s has composite primary key (not supported): %v", schema, table, columns)
	}

	logging.Debug("Found primary key",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Strings("columns", columns))

	return &PrimaryKey{
		Columns: columns,
	}, nil
}

// ListRows returns a list of primary key values from a table
// Limited to max_rows to prevent excessive directory listings
func ListRows(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn string, limit int) ([]string, error) {
	logging.Debug("Listing rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn),
		zap.Int("limit", limit))

	// Build query with proper quoting for identifiers
	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" ORDER BY "%s" LIMIT $1`,
		pkColumn, schema, table, pkColumn,
	)

	rows, err := pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list rows: %w", err)
	}
	defer rows.Close()

	var pkValues []string
	for rows.Next() {
		var pkValue interface{}
		if err := rows.Scan(&pkValue); err != nil {
			return nil, fmt.Errorf("failed to scan primary key value: %w", err)
		}

		// Convert PK value to string using format helper
		// This properly handles UUID, numeric, and other PostgreSQL types
		pkStr, err := format.ConvertValueToText(pkValue)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary key value: %w", err)
		}
		pkValues = append(pkValues, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Found rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("count", len(pkValues)))

	return pkValues, nil
}

// GetPrimaryKey is a convenience wrapper for Client
func (c *Client) GetPrimaryKey(ctx context.Context, schema, table string) (*PrimaryKey, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetPrimaryKey(ctx, c.pool, schema, table)
}

// ListRows is a convenience wrapper for Client
func (c *Client) ListRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return ListRows(ctx, c.pool, schema, table, pkColumn, limit)
}

// ListAllRows returns all primary key values from a table without any limit.
// Used by .all/ paths to explicitly bypass dir_listing_limit restriction.
func ListAllRows(ctx context.Context, pool *pgxpool.Pool, schema, table, pkColumn string) ([]string, error) {
	logging.Debug("Listing all rows (no limit)",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk_column", pkColumn))

	// Build query without LIMIT
	query := fmt.Sprintf(
		`SELECT "%s" FROM "%s"."%s" ORDER BY "%s"`,
		pkColumn, schema, table, pkColumn,
	)

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list all rows: %w", err)
	}
	defer rows.Close()

	var pkValues []string
	for rows.Next() {
		var pkValue interface{}
		if err := rows.Scan(&pkValue); err != nil {
			return nil, fmt.Errorf("failed to scan primary key value: %w", err)
		}

		// Convert PK value to string using format helper
		// This properly handles UUID, numeric, and other PostgreSQL types
		pkStr, err := format.ConvertValueToText(pkValue)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary key value: %w", err)
		}
		pkValues = append(pkValues, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	logging.Debug("Listed all rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("count", len(pkValues)))

	return pkValues, nil
}

// ListAllRows is a convenience wrapper for Client
func (c *Client) ListAllRows(ctx context.Context, schema, table, pkColumn string) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return ListAllRows(ctx, c.pool, schema, table, pkColumn)
}
