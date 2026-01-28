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
	expected := map[string]bool{".create": true, "users_email_idx": true, "users_name_idx": true}
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
	if len(entries) != 1 || entries[0] != ".create" {
		t.Errorf("Expected [.create], got %v", entries)
	}
}

// TestIndexCreateDirNode_Mkdir tests creating a new index staging entry
func TestIndexCreateDirNode_Mkdir(t *testing.T) {
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	node := NewIndexCreateDirNode(cfg, mock, "public", "users", staging)

	// Simulate what Mkdir does - it should create a staging entry
	// We test the staging logic directly since Mkdir requires FUSE bridge for NewPersistentInode
	path := "users/.indexes/.create/email_idx"
	template := node.generateIndexTemplate("email_idx")
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
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	node := NewIndexCreateDirNode(cfg, mock, "public", "users", staging)
	template := node.generateIndexTemplate("email_idx")

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
	cfg := &config.Config{}
	mock := db.NewMockDBClient()
	staging := NewStagingTracker()

	node := NewIndexCreateDirNode(cfg, mock, "analytics", "events", staging)
	template := node.generateIndexTemplate("event_time_idx")

	// Template should contain qualified table name
	if !contains(template, "analytics.events") {
		t.Error("Template should contain schema-qualified table name")
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
	expected := map[string]bool{".delete": true, ".schema": true}
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
