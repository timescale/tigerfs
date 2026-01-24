package fuse

import (
	"context"
	"fmt"
	"sync"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// PartialRow tracks columns for a row being created incrementally
type PartialRow struct {
	Schema    string
	Table     string
	PkColumn  string
	PkValue   string
	Columns   map[string]interface{} // column name -> value
	Committed bool                   // true if row has been INSERTed
}

// PartialRowTracker manages in-memory state for rows being created incrementally
type PartialRowTracker struct {
	mu   sync.RWMutex
	rows map[string]*PartialRow // key: "schema.table.pkvalue"
	db   *db.Client
}

// NewPartialRowTracker creates a new partial row tracker
func NewPartialRowTracker(dbClient *db.Client) *PartialRowTracker {
	return &PartialRowTracker{
		rows: make(map[string]*PartialRow),
		db:   dbClient,
	}
}

// GetOrCreate gets or creates a partial row
func (t *PartialRowTracker) GetOrCreate(schema, table, pkColumn, pkValue string) *PartialRow {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	if row, exists := t.rows[key]; exists {
		return row
	}

	// Create new partial row
	row := &PartialRow{
		Schema:    schema,
		Table:     table,
		PkColumn:  pkColumn,
		PkValue:   pkValue,
		Columns:   make(map[string]interface{}),
		Committed: false,
	}
	t.rows[key] = row

	logging.Debug("Created partial row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk", pkValue))

	return row
}

// Get retrieves a partial row if it exists
func (t *PartialRowTracker) Get(schema, table, pkValue string) *PartialRow {
	t.mu.RLock()
	defer t.mu.RUnlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	return t.rows[key]
}

// SetColumn sets a column value for a partial row
func (t *PartialRowTracker) SetColumn(schema, table, pkColumn, pkValue, columnName string, value interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	row, exists := t.rows[key]
	if !exists {
		row = &PartialRow{
			Schema:    schema,
			Table:     table,
			PkColumn:  pkColumn,
			PkValue:   pkValue,
			Columns:   make(map[string]interface{}),
			Committed: false,
		}
		t.rows[key] = row
	}

	row.Columns[columnName] = value

	logging.Debug("Set column in partial row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk", pkValue),
		zap.String("column", columnName),
		zap.Int("total_columns", len(row.Columns)))
}

// TryCommit attempts to commit a partial row to the database
// Returns true if committed, false if more columns needed
func (t *PartialRowTracker) TryCommit(ctx context.Context, schema, table, pkColumn, pkValue string) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	row, exists := t.rows[key]
	if !exists || row.Committed {
		return false, nil
	}

	// Check if we have enough columns to satisfy NOT NULL constraints
	canCommit, err := t.canCommitRow(ctx, row)
	if err != nil {
		return false, err
	}

	if !canCommit {
		logging.Debug("Not enough columns to commit row",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.String("pk", pkValue),
			zap.Int("column_count", len(row.Columns)))
		return false, nil
	}

	// Build column and value lists for INSERT
	columns := make([]string, 0, len(row.Columns))
	values := make([]interface{}, 0, len(row.Columns))

	// Add PK column if provided
	if pkVal, hasPk := row.Columns[row.PkColumn]; hasPk {
		columns = append(columns, row.PkColumn)
		values = append(values, pkVal)
	}

	// Add other columns
	for col, val := range row.Columns {
		if col != row.PkColumn {
			columns = append(columns, col)
			values = append(values, val)
		}
	}

	// If PK not explicitly set, add it with the pkValue from directory name
	if _, hasPk := row.Columns[row.PkColumn]; !hasPk {
		columns = append([]string{row.PkColumn}, columns...)
		values = append([]interface{}{row.PkValue}, values...)
	}

	// Execute INSERT
	_, err = t.db.InsertRow(ctx, row.Schema, row.Table, columns, values)
	if err != nil {
		logging.Error("Failed to commit partial row",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.String("pk", pkValue),
			zap.Error(err))
		return false, fmt.Errorf("failed to insert row: %w", err)
	}

	// Mark as committed
	row.Committed = true

	logging.Info("Committed partial row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk", pkValue),
		zap.Int("column_count", len(row.Columns)))

	return true, nil
}

// canCommitRow checks if a partial row has enough columns to satisfy constraints
func (t *PartialRowTracker) canCommitRow(ctx context.Context, row *PartialRow) (bool, error) {
	// Get table columns to check NOT NULL constraints
	tableColumns, err := t.db.GetColumns(ctx, row.Schema, row.Table)
	if err != nil {
		return false, fmt.Errorf("failed to get table columns: %w", err)
	}

	// Check if all NOT NULL columns (without defaults) are provided
	for _, col := range tableColumns {
		if !col.IsNullable {
			// Column is NOT NULL
			if _, provided := row.Columns[col.Name]; !provided {
				// Column not provided - check if it has a default or is the PK
				if col.Name == row.PkColumn {
					// PK will be provided from directory name
					continue
				}
				// NOT NULL column without default not provided
				logging.Debug("Missing NOT NULL column",
					zap.String("table", row.Table),
					zap.String("column", col.Name))
				return false, nil
			}
		}
	}

	return true, nil
}

// Remove removes a partial row from tracking
func (t *PartialRowTracker) Remove(schema, table, pkValue string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	delete(t.rows, key)

	logging.Debug("Removed partial row",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("pk", pkValue))
}
