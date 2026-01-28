package db

import (
	"context"
)

// MockDDLExecutor is a mock implementation of DDLExecutor for testing.
// Use function fields to customize behavior per test.
type MockDDLExecutor struct {
	ExecFunc              func(ctx context.Context, sql string, args ...interface{}) error
	ExecInTransactionFunc func(ctx context.Context, sql string, args ...interface{}) error
}

var _ DDLExecutor = (*MockDDLExecutor)(nil)

func (m *MockDDLExecutor) Exec(ctx context.Context, sql string, args ...interface{}) error {
	if m.ExecFunc != nil {
		return m.ExecFunc(ctx, sql, args...)
	}
	return nil
}

func (m *MockDDLExecutor) ExecInTransaction(ctx context.Context, sql string, args ...interface{}) error {
	if m.ExecInTransactionFunc != nil {
		return m.ExecInTransactionFunc(ctx, sql, args...)
	}
	return nil
}

// MockSchemaReader is a mock implementation of SchemaReader for testing.
type MockSchemaReader struct {
	GetCurrentSchemaFunc    func(ctx context.Context) (string, error)
	GetSchemasFunc          func(ctx context.Context) ([]string, error)
	GetTablesFunc           func(ctx context.Context, schema string) ([]string, error)
	GetColumnsFunc          func(ctx context.Context, schema, table string) ([]Column, error)
	GetPrimaryKeyFunc       func(ctx context.Context, schema, table string) (*PrimaryKey, error)
	GetTablePermissionsFunc func(ctx context.Context, schema, table string) (*TablePermissions, error)
}

var _ SchemaReader = (*MockSchemaReader)(nil)

func (m *MockSchemaReader) GetCurrentSchema(ctx context.Context) (string, error) {
	if m.GetCurrentSchemaFunc != nil {
		return m.GetCurrentSchemaFunc(ctx)
	}
	return "public", nil
}

func (m *MockSchemaReader) GetSchemas(ctx context.Context) ([]string, error) {
	if m.GetSchemasFunc != nil {
		return m.GetSchemasFunc(ctx)
	}
	return []string{"public"}, nil
}

func (m *MockSchemaReader) GetTables(ctx context.Context, schema string) ([]string, error) {
	if m.GetTablesFunc != nil {
		return m.GetTablesFunc(ctx, schema)
	}
	return []string{}, nil
}

func (m *MockSchemaReader) GetColumns(ctx context.Context, schema, table string) ([]Column, error) {
	if m.GetColumnsFunc != nil {
		return m.GetColumnsFunc(ctx, schema, table)
	}
	return []Column{}, nil
}

func (m *MockSchemaReader) GetPrimaryKey(ctx context.Context, schema, table string) (*PrimaryKey, error) {
	if m.GetPrimaryKeyFunc != nil {
		return m.GetPrimaryKeyFunc(ctx, schema, table)
	}
	return &PrimaryKey{Columns: []string{"id"}}, nil
}

func (m *MockSchemaReader) GetTablePermissions(ctx context.Context, schema, table string) (*TablePermissions, error) {
	if m.GetTablePermissionsFunc != nil {
		return m.GetTablePermissionsFunc(ctx, schema, table)
	}
	return &TablePermissions{CanSelect: true, CanInsert: true, CanUpdate: true, CanDelete: true}, nil
}

// MockRowReader is a mock implementation of RowReader for testing.
type MockRowReader struct {
	GetRowFunc      func(ctx context.Context, schema, table, pkColumn, pkValue string) (*Row, error)
	GetColumnFunc   func(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error)
	ListRowsFunc    func(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)
	ListAllRowsFunc func(ctx context.Context, schema, table, pkColumn string) ([]string, error)
}

var _ RowReader = (*MockRowReader)(nil)

func (m *MockRowReader) GetRow(ctx context.Context, schema, table, pkColumn, pkValue string) (*Row, error) {
	if m.GetRowFunc != nil {
		return m.GetRowFunc(ctx, schema, table, pkColumn, pkValue)
	}
	return &Row{Columns: []string{"id"}, Values: []interface{}{pkValue}}, nil
}

func (m *MockRowReader) GetColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName string) (interface{}, error) {
	if m.GetColumnFunc != nil {
		return m.GetColumnFunc(ctx, schema, table, pkColumn, pkValue, columnName)
	}
	return nil, nil
}

func (m *MockRowReader) ListRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	if m.ListRowsFunc != nil {
		return m.ListRowsFunc(ctx, schema, table, pkColumn, limit)
	}
	return []string{}, nil
}

func (m *MockRowReader) ListAllRows(ctx context.Context, schema, table, pkColumn string) ([]string, error) {
	if m.ListAllRowsFunc != nil {
		return m.ListAllRowsFunc(ctx, schema, table, pkColumn)
	}
	return []string{}, nil
}

// MockRowWriter is a mock implementation of RowWriter for testing.
type MockRowWriter struct {
	InsertRowFunc    func(ctx context.Context, schema, table string, columns []string, values []interface{}) (string, error)
	UpdateRowFunc    func(ctx context.Context, schema, table, pkColumn, pkValue string, columns []string, values []interface{}) error
	UpdateColumnFunc func(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error
	DeleteRowFunc    func(ctx context.Context, schema, table, pkColumn, pkValue string) error
}

var _ RowWriter = (*MockRowWriter)(nil)

func (m *MockRowWriter) InsertRow(ctx context.Context, schema, table string, columns []string, values []interface{}) (string, error) {
	if m.InsertRowFunc != nil {
		return m.InsertRowFunc(ctx, schema, table, columns, values)
	}
	return "1", nil
}

func (m *MockRowWriter) UpdateRow(ctx context.Context, schema, table, pkColumn, pkValue string, columns []string, values []interface{}) error {
	if m.UpdateRowFunc != nil {
		return m.UpdateRowFunc(ctx, schema, table, pkColumn, pkValue, columns, values)
	}
	return nil
}

func (m *MockRowWriter) UpdateColumn(ctx context.Context, schema, table, pkColumn, pkValue, columnName, newValue string) error {
	if m.UpdateColumnFunc != nil {
		return m.UpdateColumnFunc(ctx, schema, table, pkColumn, pkValue, columnName, newValue)
	}
	return nil
}

func (m *MockRowWriter) DeleteRow(ctx context.Context, schema, table, pkColumn, pkValue string) error {
	if m.DeleteRowFunc != nil {
		return m.DeleteRowFunc(ctx, schema, table, pkColumn, pkValue)
	}
	return nil
}

// MockIndexReader is a mock implementation of IndexReader for testing.
type MockIndexReader struct {
	GetIndexesFunc                 func(ctx context.Context, schema, table string) ([]Index, error)
	GetIndexByColumnFunc           func(ctx context.Context, schema, table, column string) (*Index, error)
	GetSingleColumnIndexesFunc     func(ctx context.Context, schema, table string) ([]Index, error)
	GetCompositeIndexesFunc        func(ctx context.Context, schema, table string) ([]Index, error)
	GetDistinctValuesFunc          func(ctx context.Context, schema, table, column string, limit int) ([]string, error)
	GetDistinctValuesOrderedFunc   func(ctx context.Context, schema, table, column string, limit int, ascending bool) ([]string, error)
	GetDistinctValuesFilteredFunc  func(ctx context.Context, schema, table, targetColumn string, filterColumns, filterValues []string, limit int) ([]string, error)
	GetRowsByIndexValueFunc        func(ctx context.Context, schema, table, column, value, pkColumn string, limit int) ([]string, error)
	GetRowsByIndexValueOrderedFunc func(ctx context.Context, schema, table, column, value, pkColumn string, limit int, ascending bool) ([]string, error)
	GetRowsByCompositeIndexFunc    func(ctx context.Context, schema, table string, columns, values []string, pkColumn string, limit int) ([]string, error)
}

var _ IndexReader = (*MockIndexReader)(nil)

func (m *MockIndexReader) GetIndexes(ctx context.Context, schema, table string) ([]Index, error) {
	if m.GetIndexesFunc != nil {
		return m.GetIndexesFunc(ctx, schema, table)
	}
	return []Index{}, nil
}

func (m *MockIndexReader) GetIndexByColumn(ctx context.Context, schema, table, column string) (*Index, error) {
	if m.GetIndexByColumnFunc != nil {
		return m.GetIndexByColumnFunc(ctx, schema, table, column)
	}
	return nil, nil
}

func (m *MockIndexReader) GetSingleColumnIndexes(ctx context.Context, schema, table string) ([]Index, error) {
	if m.GetSingleColumnIndexesFunc != nil {
		return m.GetSingleColumnIndexesFunc(ctx, schema, table)
	}
	return []Index{}, nil
}

func (m *MockIndexReader) GetCompositeIndexes(ctx context.Context, schema, table string) ([]Index, error) {
	if m.GetCompositeIndexesFunc != nil {
		return m.GetCompositeIndexesFunc(ctx, schema, table)
	}
	return []Index{}, nil
}

func (m *MockIndexReader) GetDistinctValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	if m.GetDistinctValuesFunc != nil {
		return m.GetDistinctValuesFunc(ctx, schema, table, column, limit)
	}
	return []string{}, nil
}

func (m *MockIndexReader) GetDistinctValuesOrdered(ctx context.Context, schema, table, column string, limit int, ascending bool) ([]string, error) {
	if m.GetDistinctValuesOrderedFunc != nil {
		return m.GetDistinctValuesOrderedFunc(ctx, schema, table, column, limit, ascending)
	}
	return []string{}, nil
}

func (m *MockIndexReader) GetDistinctValuesFiltered(ctx context.Context, schema, table, targetColumn string, filterColumns, filterValues []string, limit int) ([]string, error) {
	if m.GetDistinctValuesFilteredFunc != nil {
		return m.GetDistinctValuesFilteredFunc(ctx, schema, table, targetColumn, filterColumns, filterValues, limit)
	}
	return []string{}, nil
}

func (m *MockIndexReader) GetRowsByIndexValue(ctx context.Context, schema, table, column, value, pkColumn string, limit int) ([]string, error) {
	if m.GetRowsByIndexValueFunc != nil {
		return m.GetRowsByIndexValueFunc(ctx, schema, table, column, value, pkColumn, limit)
	}
	return []string{}, nil
}

func (m *MockIndexReader) GetRowsByIndexValueOrdered(ctx context.Context, schema, table, column, value, pkColumn string, limit int, ascending bool) ([]string, error) {
	if m.GetRowsByIndexValueOrderedFunc != nil {
		return m.GetRowsByIndexValueOrderedFunc(ctx, schema, table, column, value, pkColumn, limit, ascending)
	}
	return []string{}, nil
}

func (m *MockIndexReader) GetRowsByCompositeIndex(ctx context.Context, schema, table string, columns, values []string, pkColumn string, limit int) ([]string, error) {
	if m.GetRowsByCompositeIndexFunc != nil {
		return m.GetRowsByCompositeIndexFunc(ctx, schema, table, columns, values, pkColumn, limit)
	}
	return []string{}, nil
}

// MockCountReader is a mock implementation of CountReader for testing.
type MockCountReader struct {
	GetRowCountFunc          func(ctx context.Context, schema, table string) (int64, error)
	GetRowCountSmartFunc     func(ctx context.Context, schema, table string) (int64, error)
	GetRowCountEstimatesFunc func(ctx context.Context, schema string, tables []string) (map[string]int64, error)
}

var _ CountReader = (*MockCountReader)(nil)

func (m *MockCountReader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	if m.GetRowCountFunc != nil {
		return m.GetRowCountFunc(ctx, schema, table)
	}
	return 0, nil
}

func (m *MockCountReader) GetRowCountSmart(ctx context.Context, schema, table string) (int64, error) {
	if m.GetRowCountSmartFunc != nil {
		return m.GetRowCountSmartFunc(ctx, schema, table)
	}
	return 0, nil
}

func (m *MockCountReader) GetRowCountEstimates(ctx context.Context, schema string, tables []string) (map[string]int64, error) {
	if m.GetRowCountEstimatesFunc != nil {
		return m.GetRowCountEstimatesFunc(ctx, schema, tables)
	}
	return make(map[string]int64), nil
}

// MockDDLReader is a mock implementation of DDLReader for testing.
type MockDDLReader struct {
	GetTableDDLFunc               func(ctx context.Context, schema, table string) (string, error)
	GetFullDDLFunc                func(ctx context.Context, schema, table string) (string, error)
	GetIndexDDLFunc               func(ctx context.Context, schema, table string) (string, error)
	GetForeignKeyDDLFunc          func(ctx context.Context, schema, table string) (string, error)
	GetCheckConstraintDDLFunc     func(ctx context.Context, schema, table string) (string, error)
	GetTriggerDDLFunc             func(ctx context.Context, schema, table string) (string, error)
	GetTableCommentsFunc          func(ctx context.Context, schema, table string) (string, error)
	GetReferencingForeignKeysFunc func(ctx context.Context, schema, table string) ([]ForeignKeyRef, error)
	GetSchemaTableCountFunc       func(ctx context.Context, schema string) (int, error)
	GetViewDefinitionFunc         func(ctx context.Context, schema, view string) (string, error)
	GetDependentViewsFunc         func(ctx context.Context, schema, name string) ([]string, error)
}

var _ DDLReader = (*MockDDLReader)(nil)

func (m *MockDDLReader) GetTableDDL(ctx context.Context, schema, table string) (string, error) {
	if m.GetTableDDLFunc != nil {
		return m.GetTableDDLFunc(ctx, schema, table)
	}
	return "", nil
}

func (m *MockDDLReader) GetFullDDL(ctx context.Context, schema, table string) (string, error) {
	if m.GetFullDDLFunc != nil {
		return m.GetFullDDLFunc(ctx, schema, table)
	}
	return "", nil
}

func (m *MockDDLReader) GetIndexDDL(ctx context.Context, schema, table string) (string, error) {
	if m.GetIndexDDLFunc != nil {
		return m.GetIndexDDLFunc(ctx, schema, table)
	}
	return "", nil
}

func (m *MockDDLReader) GetForeignKeyDDL(ctx context.Context, schema, table string) (string, error) {
	if m.GetForeignKeyDDLFunc != nil {
		return m.GetForeignKeyDDLFunc(ctx, schema, table)
	}
	return "", nil
}

func (m *MockDDLReader) GetCheckConstraintDDL(ctx context.Context, schema, table string) (string, error) {
	if m.GetCheckConstraintDDLFunc != nil {
		return m.GetCheckConstraintDDLFunc(ctx, schema, table)
	}
	return "", nil
}

func (m *MockDDLReader) GetTriggerDDL(ctx context.Context, schema, table string) (string, error) {
	if m.GetTriggerDDLFunc != nil {
		return m.GetTriggerDDLFunc(ctx, schema, table)
	}
	return "", nil
}

func (m *MockDDLReader) GetTableComments(ctx context.Context, schema, table string) (string, error) {
	if m.GetTableCommentsFunc != nil {
		return m.GetTableCommentsFunc(ctx, schema, table)
	}
	return "", nil
}

func (m *MockDDLReader) GetReferencingForeignKeys(ctx context.Context, schema, table string) ([]ForeignKeyRef, error) {
	if m.GetReferencingForeignKeysFunc != nil {
		return m.GetReferencingForeignKeysFunc(ctx, schema, table)
	}
	return nil, nil
}

func (m *MockDDLReader) GetSchemaTableCount(ctx context.Context, schema string) (int, error) {
	if m.GetSchemaTableCountFunc != nil {
		return m.GetSchemaTableCountFunc(ctx, schema)
	}
	return 0, nil
}

func (m *MockDDLReader) GetViewDefinition(ctx context.Context, schema, view string) (string, error) {
	if m.GetViewDefinitionFunc != nil {
		return m.GetViewDefinitionFunc(ctx, schema, view)
	}
	return "", nil
}

func (m *MockDDLReader) GetDependentViews(ctx context.Context, schema, name string) ([]string, error) {
	if m.GetDependentViewsFunc != nil {
		return m.GetDependentViewsFunc(ctx, schema, name)
	}
	return nil, nil
}

// MockPaginationReader is a mock implementation of PaginationReader for testing.
type MockPaginationReader struct {
	GetFirstNRowsFunc       func(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)
	GetLastNRowsFunc        func(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error)
	GetRandomSampleRowsFunc func(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error)
}

var _ PaginationReader = (*MockPaginationReader)(nil)

func (m *MockPaginationReader) GetFirstNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	if m.GetFirstNRowsFunc != nil {
		return m.GetFirstNRowsFunc(ctx, schema, table, pkColumn, limit)
	}
	return []string{}, nil
}

func (m *MockPaginationReader) GetLastNRows(ctx context.Context, schema, table, pkColumn string, limit int) ([]string, error) {
	if m.GetLastNRowsFunc != nil {
		return m.GetLastNRowsFunc(ctx, schema, table, pkColumn, limit)
	}
	return []string{}, nil
}

func (m *MockPaginationReader) GetRandomSampleRows(ctx context.Context, schema, table, pkColumn string, limit int, estimatedRows int64) ([]string, error) {
	if m.GetRandomSampleRowsFunc != nil {
		return m.GetRandomSampleRowsFunc(ctx, schema, table, pkColumn, limit, estimatedRows)
	}
	return []string{}, nil
}

// MockDBClient is a composite mock implementing DBClient for full FUSE node testing.
// Embeds individual mocks for each interface.
type MockDBClient struct {
	*MockDDLExecutor
	*MockSchemaReader
	*MockRowReader
	*MockRowWriter
	*MockIndexReader
	*MockCountReader
	*MockDDLReader
	*MockPaginationReader
}

var _ DBClient = (*MockDBClient)(nil)

// NewMockDBClient creates a new MockDBClient with all embedded mocks initialized.
func NewMockDBClient() *MockDBClient {
	return &MockDBClient{
		MockDDLExecutor:      &MockDDLExecutor{},
		MockSchemaReader:     &MockSchemaReader{},
		MockRowReader:        &MockRowReader{},
		MockRowWriter:        &MockRowWriter{},
		MockIndexReader:      &MockIndexReader{},
		MockCountReader:      &MockCountReader{},
		MockDDLReader:        &MockDDLReader{},
		MockPaginationReader: &MockPaginationReader{},
	}
}
