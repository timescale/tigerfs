package fs

import (
	"context"
	"fmt"
	"sync"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// StagingManager manages in-memory state for rows being created incrementally.
//
// When a user creates a row directory (mkdir /table/pk), the staging manager
// tracks column values as they are written. When all required (NOT NULL) columns
// are present, the row can be committed to the database.
//
// Thread-safety: All methods are safe for concurrent use.
//
// Typical workflow:
//  1. User runs: mkdir /table/newpk
//  2. GetOrCreate is called to start tracking the new row
//  3. User runs: echo "value" > /table/newpk/column
//  4. SetColumn is called to record the column value
//  5. TryCommit is called to check if row has all required columns
//  6. If TryCommit returns true, the row is INSERTed and marked committed
type StagingManager struct {
	mu   sync.RWMutex
	rows map[string]*PartialRow // key: "schema.table.pkvalue"
}

// PartialRow tracks column values for a row being created incrementally.
// The row remains in memory until TryCommit succeeds, at which point
// the data is INSERTed to the database.
type PartialRow struct {
	// Schema is the database schema name (e.g., "public").
	Schema string
	// Table is the table name.
	Table string
	// PKColumn is the primary key column name (e.g., "id").
	PKColumn string
	// PKValue is the primary key value from the directory name.
	PKValue string
	// Columns maps column names to their values. Values are stored as
	// interface{} and converted to appropriate types during INSERT.
	Columns map[string]interface{}
	// Committed is true once the row has been INSERTed to the database.
	Committed bool
}

// NewStagingManager creates a new staging manager.
func NewStagingManager() *StagingManager {
	return &StagingManager{
		rows: make(map[string]*PartialRow),
	}
}

// GetOrCreate retrieves an existing partial row or creates a new one.
//
// Parameters:
//   - schema: database schema name (e.g., "public")
//   - table: table name
//   - pkColumn: primary key column name (e.g., "id")
//   - pkValue: primary key value (from directory name)
//
// Returns the partial row (never nil). If the row doesn't exist,
// a new empty PartialRow is created with Committed=false.
func (s *StagingManager) GetOrCreate(schema, table, pkColumn, pkValue string) *PartialRow {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	if row, exists := s.rows[key]; exists {
		return row
	}

	row := &PartialRow{
		Schema:    schema,
		Table:     table,
		PKColumn:  pkColumn,
		PKValue:   pkValue,
		Columns:   make(map[string]interface{}),
		Committed: false,
	}
	s.rows[key] = row

	return row
}

// Get retrieves a partial row by its key.
//
// Parameters:
//   - schema: database schema name
//   - table: table name
//   - pkValue: primary key value
//
// Returns the partial row if it exists, or nil if not found.
func (s *StagingManager) Get(schema, table, pkValue string) *PartialRow {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	return s.rows[key]
}

// SetColumn sets a column value for a partial row.
// If the partial row doesn't exist, it is created automatically.
//
// Parameters:
//   - schema: database schema name
//   - table: table name
//   - pkColumn: primary key column name
//   - pkValue: primary key value
//   - columnName: name of the column to set
//   - value: the value to store (typically a string from file content)
func (s *StagingManager) SetColumn(schema, table, pkColumn, pkValue, columnName string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	row, exists := s.rows[key]
	if !exists {
		row = &PartialRow{
			Schema:    schema,
			Table:     table,
			PKColumn:  pkColumn,
			PKValue:   pkValue,
			Columns:   make(map[string]interface{}),
			Committed: false,
		}
		s.rows[key] = row
	}

	row.Columns[columnName] = value
}

// TryCommit attempts to commit a partial row to the database.
//
// The commit succeeds only if all NOT NULL columns (without defaults)
// have values. The primary key is always included using pkValue.
//
// Parameters:
//   - ctx: context for database operations
//   - schema: database schema name
//   - table: table name
//   - pkColumn: primary key column name
//   - pkValue: primary key value
//   - dbClient: database client for schema inspection and INSERT
//
// Returns:
//   - (true, nil): row was committed to the database
//   - (false, nil): row not ready (missing required columns)
//   - (false, err): database error occurred
func (s *StagingManager) TryCommit(ctx context.Context, schema, table, pkColumn, pkValue string, dbClient db.DBClient) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	row, exists := s.rows[key]
	if !exists || row.Committed {
		return false, nil
	}

	// Check if we have enough columns to satisfy NOT NULL constraints
	canCommit, err := s.canCommitRow(ctx, row, dbClient)
	if err != nil {
		return false, err
	}

	if !canCommit {
		return false, nil
	}

	// Build column and value lists for INSERT
	columns := make([]string, 0, len(row.Columns))
	values := make([]interface{}, 0, len(row.Columns))

	// Add PK column if provided
	if pkVal, hasPk := row.Columns[row.PKColumn]; hasPk {
		columns = append(columns, row.PKColumn)
		values = append(values, pkVal)
	}

	// Add other columns
	for col, val := range row.Columns {
		if col != row.PKColumn {
			columns = append(columns, col)
			values = append(values, val)
		}
	}

	// If PK not explicitly set, add it with the pkValue from directory name
	if _, hasPk := row.Columns[row.PKColumn]; !hasPk {
		columns = append([]string{row.PKColumn}, columns...)
		values = append([]interface{}{row.PKValue}, values...)
	}

	// Execute INSERT
	_, err = dbClient.InsertRow(ctx, row.Schema, row.Table, columns, values)
	if err != nil {
		return false, fmt.Errorf("failed to insert row: %w", err)
	}

	// Mark as committed
	row.Committed = true

	return true, nil
}

// canCommitRow checks if a partial row has enough columns to satisfy constraints.
func (s *StagingManager) canCommitRow(ctx context.Context, row *PartialRow, dbClient db.DBClient) (bool, error) {
	// Get table columns to check NOT NULL constraints
	tableColumns, err := dbClient.GetColumns(ctx, row.Schema, row.Table)
	if err != nil {
		return false, fmt.Errorf("failed to get table columns: %w", err)
	}

	// Check if all NOT NULL columns (without defaults) are provided
	for _, col := range tableColumns {
		if !col.IsNullable {
			// Column is NOT NULL
			if _, provided := row.Columns[col.Name]; !provided {
				// Column not provided - check if it has a default or is the PK
				if col.Name == row.PKColumn {
					// PK will be provided from directory name
					continue
				}
				// NOT NULL column without default not provided
				return false, nil
			}
		}
	}

	return true, nil
}

// Remove removes a partial row from tracking.
// Use this after a committed row is no longer needed, or to cancel
// an incomplete row creation.
//
// Parameters:
//   - schema: database schema name
//   - table: table name
//   - pkValue: primary key value
func (s *StagingManager) Remove(schema, table, pkValue string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	delete(s.rows, key)
}

// GetColumnValue retrieves a column value from a partial row.
//
// Parameters:
//   - schema: database schema name
//   - table: table name
//   - pkValue: primary key value
//   - columnName: name of the column to retrieve
//
// Returns the column value, or nil if the row or column doesn't exist.
func (s *StagingManager) GetColumnValue(schema, table, pkValue, columnName string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	row, exists := s.rows[key]
	if !exists {
		return nil
	}

	return row.Columns[columnName]
}

// IsCommitted checks if a partial row has been committed to the database.
//
// Parameters:
//   - schema: database schema name
//   - table: table name
//   - pkValue: primary key value
//
// Returns true if the row exists and was committed, false otherwise.
func (s *StagingManager) IsCommitted(schema, table, pkValue string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := fmt.Sprintf("%s.%s.%s", schema, table, pkValue)
	row, exists := s.rows[key]
	if !exists {
		return false
	}

	return row.Committed
}
