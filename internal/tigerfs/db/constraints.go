package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// Constraint represents a database constraint
type Constraint struct {
	Name       string
	Type       string // NOT NULL, UNIQUE, CHECK, PRIMARY KEY, FOREIGN KEY
	Columns    []string
	Definition string // For CHECK constraints
}

// ValidateConstraints validates column values against table constraints
// Returns an error if any constraints would be violated
func ValidateConstraints(ctx context.Context, pool *pgxpool.Pool, schema, table string, values map[string]interface{}) error {
	logging.Debug("Validating constraints",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("column_count", len(values)))

	// Get all columns for the table to check NOT NULL constraints
	columns, err := getColumnsForConstraintCheck(ctx, pool, schema, table)
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Check NOT NULL constraints
	for _, col := range columns {
		if !col.IsNullable && col.Default == "" {
			// Column is NOT NULL without default
			value, ok := values[col.Name]
			if !ok || value == nil || value == "" {
				logging.Debug("NOT NULL constraint violation",
					zap.String("schema", schema),
					zap.String("table", table),
					zap.String("column", col.Name))
				return fmt.Errorf("NOT NULL constraint violation on column %s", col.Name)
			}
		}
	}

	// Check UNIQUE constraints
	// For each column being updated, check if it has a unique constraint
	uniqueConstraints, err := getUniqueConstraints(ctx, pool, schema, table)
	if err != nil {
		return fmt.Errorf("failed to get unique constraints: %w", err)
	}

	for _, constraint := range uniqueConstraints {
		// Check if any of the columns in this constraint are being updated
		for _, colName := range constraint.Columns {
			if value, ok := values[colName]; ok {
				// This column is being updated, check for duplicates
				if err := checkUniqueConstraint(ctx, pool, schema, table, colName, value); err != nil {
					logging.Debug("UNIQUE constraint violation",
						zap.String("schema", schema),
						zap.String("table", table),
						zap.String("column", colName),
						zap.String("constraint", constraint.Name))
					return fmt.Errorf("UNIQUE constraint violation on column %s", colName)
				}
			}
		}
	}

	// CHECK constraints are handled by PostgreSQL during INSERT/UPDATE
	// We don't pre-validate them here

	logging.Debug("Constraint validation passed",
		zap.String("schema", schema),
		zap.String("table", table))

	return nil
}

// getColumnsForConstraintCheck queries column metadata for constraint checking
func getColumnsForConstraintCheck(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]Column, error) {
	query := `
		SELECT
			column_name,
			data_type,
			is_nullable = 'YES' as is_nullable,
			column_default
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
		var nullable bool
		var defaultVal *string

		err := rows.Scan(&col.Name, &col.DataType, &nullable, &defaultVal)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		col.IsNullable = nullable
		if defaultVal != nil {
			col.Default = *defaultVal
		}

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return columns, nil
}

// getUniqueConstraints retrieves UNIQUE constraints for a table
func getUniqueConstraints(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]Constraint, error) {
	query := `
		SELECT
			tc.constraint_name,
			array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
			AND tc.table_name = kcu.table_name
		WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
			AND tc.table_schema = $1
			AND tc.table_name = $2
		GROUP BY tc.constraint_name
	`

	rows, err := pool.Query(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query unique constraints: %w", err)
	}
	defer rows.Close()

	var constraints []Constraint
	for rows.Next() {
		var constraint Constraint
		constraint.Type = "UNIQUE"

		err := rows.Scan(&constraint.Name, &constraint.Columns)
		if err != nil {
			return nil, fmt.Errorf("failed to scan constraint: %w", err)
		}

		constraints = append(constraints, constraint)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating constraints: %w", err)
	}

	return constraints, nil
}

// checkUniqueConstraint checks if a value violates a unique constraint
func checkUniqueConstraint(ctx context.Context, pool *pgxpool.Pool, schema, table, column string, value interface{}) error {
	// Check if this value already exists in the table
	query := fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1 FROM %s WHERE %s = $1
		)
	`, qt(schema, table), qi(column))

	var exists bool
	err := pool.QueryRow(ctx, query, value).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check unique constraint: %w", err)
	}

	if exists {
		return fmt.Errorf("value already exists")
	}

	return nil
}
