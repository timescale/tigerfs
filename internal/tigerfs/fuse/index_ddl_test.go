package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TestNewIndexesNode tests IndexesNode creation
func TestNewIndexesNode(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	node := NewIndexesNode(cfg, mock, "public", "users", staging)

	if node.cfg != cfg {
		t.Error("Expected config to be set")
	}
	if node.schema != "public" {
		t.Errorf("Expected schema='public', got %q", node.schema)
	}
	if node.tableName != "users" {
		t.Errorf("Expected tableName='users', got %q", node.tableName)
	}
	if node.staging != staging {
		t.Error("Expected staging to be set")
	}
}

// TestIndexesNode_Readdir tests listing indexes
func TestIndexesNode_Readdir(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	mock.MockIndexReader.GetIndexesFunc = func(ctx context.Context, schema, table string) ([]db.Index, error) {
		return []db.Index{
			{Name: "users_email_idx", Columns: []string{"email"}, IsUnique: true},
			{Name: "users_name_idx", Columns: []string{"name"}, IsUnique: false},
		}, nil
	}

	node := NewIndexesNode(cfg, mock, "public", "users", staging)
	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}
	if stream == nil {
		t.Fatal("Expected non-nil stream")
	}

	// Collect entries
	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should have .create plus 2 indexes
	expected := map[string]bool{DirCreate: true, "users_email_idx": true, "users_name_idx": true}
	for _, e := range entries {
		if !expected[e] {
			t.Errorf("Unexpected entry: %q", e)
		}
		delete(expected, e)
	}
	if len(expected) > 0 {
		t.Errorf("Missing entries: %v", expected)
	}
}

// TestIndexesNode_Readdir_Empty tests listing when no indexes exist
func TestIndexesNode_Readdir_Empty(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	mock.MockIndexReader.GetIndexesFunc = func(ctx context.Context, schema, table string) ([]db.Index, error) {
		return []db.Index{}, nil
	}

	node := NewIndexesNode(cfg, mock, "public", "users", staging)
	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should still have .create even with no indexes
	if len(entries) != 1 || entries[0] != DirCreate {
		t.Errorf("Expected [%s], got %v", DirCreate, entries)
	}
}

// TestIndexCreateDirNode_Mkdir tests creating a new index staging entry
func TestIndexCreateDirNode_Mkdir(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	node := NewIndexCreateDirNode(cfg, mock, "public", "users", staging)

	// Simulate what Mkdir does - it should create a staging entry
	// We test the staging logic directly since Mkdir requires FUSE bridge for NewPersistentInode
	path := "users/.indexes/.create/email_idx"
	template := node.generateIndexTemplate(ctx, "email_idx")
	staging.Set(path, template)

	// Verify staging entry was created
	entry := staging.Get(path)
	if entry == nil {
		t.Fatal("Expected staging entry to be created")
	}
	// The staging entry stores content, not the StagingContext
	// Context is tracked by the FUSE node
	if entry.Content != template {
		t.Error("Expected staging content to match template")
	}
	// Verify the path appears in ListPending
	prefix := "users/.indexes/.create"
	pending := staging.ListPending(prefix)
	found := false
	for _, name := range pending {
		if name == "email_idx" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected 'email_idx' in ListPending, got %v", pending)
	}
}

// TestIndexCreateDirNode_GenerateTemplate tests template generation
func TestIndexCreateDirNode_GenerateTemplate(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	node := NewIndexCreateDirNode(cfg, mock, "public", "users", staging)
	template := node.generateIndexTemplate(ctx, "email_idx")

	// Template should contain index name and table name
	if template == "" {
		t.Error("Expected non-empty template")
	}
	if !contains(template, "email_idx") {
		t.Error("Template should contain index name")
	}
	if !contains(template, "users") {
		t.Error("Template should contain table name")
	}
	if !contains(template, "CREATE INDEX") {
		t.Error("Template should contain CREATE INDEX")
	}
}

// TestIndexCreateDirNode_GenerateTemplate_NonDefaultSchema tests template with schema prefix
func TestIndexCreateDirNode_GenerateTemplate_NonDefaultSchema(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	node := NewIndexCreateDirNode(cfg, mock, "analytics", "events", staging)
	template := node.generateIndexTemplate(ctx, "event_time_idx")

	// Template should contain qualified table name
	if !contains(template, "analytics.events") {
		t.Error("Template should contain schema-qualified table name")
	}
}

// TestIndexCreateDirNode_GenerateTemplate_ColumnInference tests column inference from index name
func TestIndexCreateDirNode_GenerateTemplate_ColumnInference(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	staging := NewStagingTracker()

	// Create mock with columns
	mock := db.NewMockDBClient()
	mock.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "email", DataType: "text"},
			{Name: "name", DataType: "text"},
			{Name: "created_at", DataType: "timestamp"},
		}, nil
	}

	node := NewIndexCreateDirNode(cfg, mock, "public", "users", staging)

	tests := []struct {
		indexName      string
		expectColumn   string
		expectInferred bool
	}{
		{"idx_email", "email", true},
		{"email_idx", "email", true},
		{"email_index", "email", true},
		{"idx_name", "name", true},
		{"name", "name", true},             // just the column name
		{"users_email_idx", "email", true}, // table prefix stripped (no 'users' column)
		{"users_email", "email", true},     // table prefix stripped without suffix
		{"idx_foo", "", false},             // no matching column
		{"idx_", "", false},                // empty after prefix
		{"random_name", "", false},         // no pattern match
		{"created_at_idx", "", false},      // ambiguous (contains underscore after strip)
		{"idx_created_at", "", false},      // ambiguous (contains underscore after strip)
	}

	for _, tt := range tests {
		t.Run(tt.indexName, func(t *testing.T) {
			template := node.generateIndexTemplate(ctx, tt.indexName)

			if tt.expectInferred {
				// Should have "Inferred column" comment and uncommented CREATE INDEX
				if !contains(template, "Inferred column: "+tt.expectColumn) {
					t.Errorf("Expected 'Inferred column: %s' in template", tt.expectColumn)
				}
				// Should have uncommented CREATE INDEX with the column
				expected := "CREATE INDEX " + tt.indexName + " ON users (" + tt.expectColumn + ");"
				if !contains(template, expected) {
					t.Errorf("Expected uncommented '%s' in template, got:\n%s", expected, template)
				}
			} else {
				// Should have commented CREATE INDEX
				if contains(template, "Inferred column") {
					t.Error("Should not have inferred column")
				}
				if !contains(template, "-- CREATE INDEX") {
					t.Error("CREATE INDEX should be commented out when no column inferred")
				}
				if !contains(template, "Uncomment and replace column_name") {
					t.Error("Should have instruction to uncomment")
				}
			}
		})
	}
}

// TestIndexCreateDirNode_GenerateTemplate_ColumnInference_WithTableNameColumn tests
// that table prefix stripping is disabled when there's a column named the same as the table.
func TestIndexCreateDirNode_GenerateTemplate_ColumnInference_WithTableNameColumn(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	staging := NewStagingTracker()

	// Create mock with a column named 'users' (same as table name)
	mock := db.NewMockDBClient()
	mock.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "users", DataType: "integer"}, // column same as table name!
			{Name: "email", DataType: "text"},
		}, nil
	}

	node := NewIndexCreateDirNode(cfg, mock, "public", "users", staging)

	// users_email_idx should NOT infer email because 'users' is a column
	// (could be composite index on users, email)
	template := node.generateIndexTemplate(ctx, "users_email_idx")

	if contains(template, "Inferred column") {
		t.Error("Should not infer column when table name is also a column name")
	}
	if !contains(template, "-- CREATE INDEX") {
		t.Error("CREATE INDEX should be commented out")
	}
}

// TestIndexDDLNode_Readdir tests listing .delete and .schema
func TestIndexDDLNode_Readdir(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	index := &db.Index{
		Name:      "users_email_idx",
		Columns:   []string{"email"},
		IsUnique:  true,
		IsPrimary: false,
	}

	node := NewIndexDDLNode(cfg, mock, "public", "users", index, staging)
	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should have .delete and .schema
	expected := map[string]bool{DirDelete: true, FileSchema: true}
	for _, e := range entries {
		if !expected[e] {
			t.Errorf("Unexpected entry: %q", e)
		}
		delete(expected, e)
	}
	if len(expected) > 0 {
		t.Errorf("Missing entries: %v", expected)
	}
}

// TestIndexDDLNode_GetSchemaContent tests .schema content generation
func TestIndexDDLNode_GetSchemaContent(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	index := &db.Index{
		Name:       "users_email_idx",
		Columns:    []string{"email"},
		IsUnique:   true,
		IsPrimary:  false,
		Definition: "CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email)",
	}

	node := NewIndexDDLNode(cfg, mock, "public", "users", index, staging)
	content := node.getSchemaContent()

	if !contains(content, "CREATE UNIQUE INDEX") {
		t.Error("Schema should contain CREATE INDEX statement")
	}
	if !contains(content, "users_email_idx") {
		t.Error("Schema should contain index name")
	}
	if !contains(content, "email") {
		t.Error("Schema should contain column name")
	}
}

// TestIndexDDLNode_GetSchemaContent_Fallback tests .schema fallback when Definition is empty
func TestIndexDDLNode_GetSchemaContent_Fallback(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	index := &db.Index{
		Name:       "users_name_idx",
		Columns:    []string{"last_name", "first_name"},
		IsUnique:   false,
		IsPrimary:  false,
		Definition: "", // Empty definition
	}

	node := NewIndexDDLNode(cfg, mock, "public", "users", index, staging)
	content := node.getSchemaContent()

	if !contains(content, "users_name_idx") {
		t.Errorf("Fallback should contain index name, got: %s", content)
	}
	if !contains(content, "definition not available") {
		t.Errorf("Fallback should indicate definition not available, got: %s", content)
	}
}

// TestIndexDeleteWorkflow_WithMocks tests the full delete workflow using mocks
func TestIndexDeleteWorkflow_WithMocks(t *testing.T) {
	staging := NewStagingTracker()

	var executedSQL string
	mockDB := &db.MockDDLExecutor{
		ExecFunc: func(ctx context.Context, sql string, args ...interface{}) error {
			executedSQL = sql
			return nil
		},
	}

	// Simulate creating a delete staging entry
	path := "users/.indexes/email_idx/.delete"
	stagingCtx := StagingContext{
		ObjectType:  "index",
		ObjectName:  "email_idx",
		Schema:      "public",
		TableName:   "users",
		Operation:   DDLDelete,
		StagingPath: path,
	}
	ddl := "DROP INDEX email_idx;"
	staging.Set(path, ddl)

	// Commit the delete using the test helper
	err := runCommitWithExecutor(context.Background(), mockDB, staging, nil, stagingCtx)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify SQL was executed
	if executedSQL != "DROP INDEX email_idx;" {
		t.Errorf("Expected DROP INDEX, got: %s", executedSQL)
	}
}

// TestIndexCreateWorkflow_WithMocks tests the full create workflow using mocks
func TestIndexCreateWorkflow_WithMocks(t *testing.T) {
	staging := NewStagingTracker()

	var executedSQL string
	mockDB := &db.MockDDLExecutor{
		ExecFunc: func(ctx context.Context, sql string, args ...interface{}) error {
			executedSQL = sql
			return nil
		},
	}

	// Simulate creating a create staging entry
	path := "users/.indexes/.create/email_idx"
	stagingCtx := StagingContext{
		ObjectType:  "index",
		ObjectName:  "email_idx",
		Schema:      "public",
		TableName:   "users",
		Operation:   DDLCreate,
		StagingPath: path,
	}
	ddl := "CREATE INDEX email_idx ON users(email);"
	staging.Set(path, ddl)

	// Commit the create using the test helper
	err := runCommitWithExecutor(context.Background(), mockDB, staging, nil, stagingCtx)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify SQL was executed
	if executedSQL != "CREATE INDEX email_idx ON users(email);" {
		t.Errorf("Expected CREATE INDEX, got: %s", executedSQL)
	}
}

// TestJoinStrings tests the joinStrings helper
func TestJoinStrings(t *testing.T) {
	tests := []struct {
		strs     []string
		sep      string
		expected string
	}{
		{[]string{}, ", ", ""},
		{[]string{"a"}, ", ", "a"},
		{[]string{"a", "b"}, ", ", "a, b"},
		{[]string{"a", "b", "c"}, "-", "a-b-c"},
	}

	for _, tt := range tests {
		result := joinStrings(tt.strs, tt.sep)
		if result != tt.expected {
			t.Errorf("joinStrings(%v, %q) = %q, want %q", tt.strs, tt.sep, result, tt.expected)
		}
	}
}

// TestIndexesNode_Lookup_Create tests looking up .create directory
func TestIndexesNode_Lookup_Create(t *testing.T) {
	t.Skip("Lookup tests require FUSE bridge infrastructure - see test/integration/")
}

// TestIndexesNode_Lookup_Index tests looking up existing index
func TestIndexesNode_Lookup_Index(t *testing.T) {
	t.Skip("Lookup tests require FUSE bridge infrastructure - see test/integration/")
}

// TestIndexDDLNode_Lookup_Delete tests looking up .delete directory
func TestIndexDDLNode_Lookup_Delete(t *testing.T) {
	t.Skip("Lookup tests require FUSE bridge infrastructure - see test/integration/")
}

// TestIndexCreateDirNode_Readdir tests listing staged index creations
func TestIndexCreateDirNode_Readdir(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	// Add some staged index creations for "users" table
	staging.Set("users/.indexes/.create/email_idx", "CREATE INDEX email_idx ON users(email);")
	staging.Set("users/.indexes/.create/name_idx", "CREATE INDEX name_idx ON users(name);")

	// Different table - should not appear in users' listing
	staging.Set("orders/.indexes/.create/order_idx", "CREATE INDEX order_idx ON orders(order_id);")

	node := NewIndexCreateDirNode(cfg, mock, "public", "users", staging)
	stream, errno := node.Readdir(context.Background())

	if errno != 0 {
		t.Errorf("Expected errno=0, got %d", errno)
	}

	var entries []string
	for stream.HasNext() {
		entry, _ := stream.Next()
		entries = append(entries, entry.Name)
	}

	// Should only have the 2 indexes for "users" table
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d: %v", len(entries), entries)
	}

	expected := map[string]bool{"email_idx": true, "name_idx": true}
	for _, e := range entries {
		if !expected[e] {
			t.Errorf("Unexpected entry: %q", e)
		}
	}
}

// TestIndexCreateDirNode_Lookup tests looking up staged index
func TestIndexCreateDirNode_Lookup(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	// No staging entry - should return ENOENT
	node := NewIndexCreateDirNode(cfg, mock, "public", "users", staging)
	_, errno := node.Lookup(context.Background(), "email_idx", nil)
	if errno != syscall.ENOENT {
		t.Errorf("Expected ENOENT for non-staged index, got %d", errno)
	}

	// Add staging entry - Lookup requires FUSE bridge for inode creation
	t.Skip("Full Lookup tests require FUSE bridge infrastructure - see test/integration/")
}

// Note: contains helper is defined in control_files_test.go

// TestExtractColumnCandidates tests the extractColumnCandidates helper function
func TestExtractColumnCandidates(t *testing.T) {
	tests := []struct {
		name               string
		indexName          string
		tableName          string
		hasTableNameColumn bool
		expected           []string
	}{
		// Prefix patterns: idx_<column>, ix_<column>, index_<column>
		{
			name:      "idx_prefix",
			indexName: "idx_email",
			tableName: "users",
			expected:  []string{"email"},
		},
		{
			name:      "ix_prefix",
			indexName: "ix_name",
			tableName: "users",
			expected:  []string{"name"},
		},
		{
			name:      "index_prefix",
			indexName: "index_status",
			tableName: "orders",
			expected:  []string{"status"},
		},

		// Suffix patterns: <column>_idx, <column>_ix, <column>_index
		{
			name:      "idx_suffix",
			indexName: "email_idx",
			tableName: "users",
			expected:  []string{"email"},
		},
		{
			name:      "ix_suffix",
			indexName: "name_ix",
			tableName: "users",
			expected:  []string{"name"},
		},
		{
			name:      "index_suffix",
			indexName: "status_index",
			tableName: "orders",
			expected:  []string{"status"},
		},

		// Table prefix patterns: <table>_<column>_idx
		{
			name:      "table_column_idx",
			indexName: "users_email_idx",
			tableName: "users",
			expected:  []string{"email"},
		},
		{
			name:      "table_column_no_suffix",
			indexName: "users_email",
			tableName: "users",
			expected:  []string{"email"},
		},

		// Table prefix disabled when column has same name as table
		{
			name:               "table_column_idx_with_table_name_column",
			indexName:          "users_email_idx",
			tableName:          "users",
			hasTableNameColumn: true,
			expected:           []string{}, // No inference when ambiguous
		},

		// Just the column name
		{
			name:      "bare_column_name",
			indexName: "email",
			tableName: "users",
			expected:  []string{"email"},
		},

		// Edge cases - no match
		{
			name:      "underscore_in_column_prefix",
			indexName: "idx_created_at",
			tableName: "users",
			expected:  []string{}, // Has underscore after prefix
		},
		{
			name:      "underscore_in_column_suffix",
			indexName: "created_at_idx",
			tableName: "users",
			expected:  []string{}, // Has underscore before suffix
		},
		{
			name:      "empty_after_prefix",
			indexName: "idx_",
			tableName: "users",
			expected:  []string{},
		},
		{
			name:      "random_underscored_name",
			indexName: "some_random_name",
			tableName: "users",
			expected:  []string{},
		},

		// Case insensitivity
		{
			name:      "uppercase_prefix",
			indexName: "IDX_EMAIL",
			tableName: "users",
			expected:  []string{"email"},
		},
		{
			name:      "mixed_case_table",
			indexName: "Users_Status_idx",
			tableName: "Users",
			expected:  []string{"status"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractColumnCandidates(tt.indexName, tt.tableName, tt.hasTableNameColumn)

			// Compare results
			if len(result) != len(tt.expected) {
				t.Errorf("extractColumnCandidates(%q, %q, %v) = %v, want %v",
					tt.indexName, tt.tableName, tt.hasTableNameColumn, result, tt.expected)
				return
			}

			// Check each expected value is present
			for _, exp := range tt.expected {
				found := false
				for _, got := range result {
					if got == exp {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("extractColumnCandidates(%q, %q, %v) = %v, missing %q",
						tt.indexName, tt.tableName, tt.hasTableNameColumn, result, exp)
				}
			}
		})
	}
}
