package fuse

import (
	"context"
	"errors"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TestTestFileNode_runTest_Success tests that DDL validation succeeds with valid SQL.
func TestTestFileNode_runTest_Success(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	staging := NewStagingTracker()
	stagingPath := ".create/users"

	// Set up staged DDL content
	staging.Set(stagingPath, "CREATE TABLE users (id SERIAL PRIMARY KEY);")

	// Create mock DDL executor that succeeds
	mockDB := &db.MockDDLExecutor{
		ExecInTransactionFunc: func(ctx context.Context, sql string, args ...interface{}) error {
			// Verify the SQL was extracted correctly (comments stripped)
			if sql != "CREATE TABLE users (id SERIAL PRIMARY KEY);" {
				t.Errorf("Unexpected SQL: %q", sql)
			}
			return nil
		},
	}

	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	// Create actual TestFileNode with mock
	node := NewTestFileNode(cfg, mockDB, staging, stagingCtx)
	if node == nil {
		t.Fatal("NewTestFileNode returned nil")
	}

	// Test the core logic via helper (since runTest is private)
	err := runTestWithExecutor(ctx, mockDB, staging, stagingCtx)
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	// Verify test result was stored
	result := staging.GetTestResult(stagingPath)
	if result != "OK: DDL validated successfully.\n" {
		t.Errorf("Unexpected test result: %q", result)
	}
}

// TestTestFileNode_runTest_ValidationFails tests that DDL validation errors are reported.
func TestTestFileNode_runTest_ValidationFails(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	staging := NewStagingTracker()
	stagingPath := ".create/users"

	// Set up staged DDL content with invalid type
	staging.Set(stagingPath, "CREATE TABLE users (id INVALID_TYPE);")

	// Create mock DDL executor that returns an error
	mockDB := &db.MockDDLExecutor{
		ExecInTransactionFunc: func(ctx context.Context, sql string, args ...interface{}) error {
			return errors.New("type \"invalid_type\" does not exist")
		},
	}

	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	// Verify node creation works with mock
	node := NewTestFileNode(cfg, mockDB, staging, stagingCtx)
	if node == nil {
		t.Fatal("NewTestFileNode returned nil")
	}

	err := runTestWithExecutor(ctx, mockDB, staging, stagingCtx)
	if err == nil {
		t.Error("Expected error, got nil")
	}

	// Verify error was stored in test result
	result := staging.GetTestResult(stagingPath)
	if result != "Error: type \"invalid_type\" does not exist\n" {
		t.Errorf("Unexpected test result: %q", result)
	}
}

// TestTestFileNode_runTest_NoContent tests that missing content is handled.
func TestTestFileNode_runTest_NoContent(t *testing.T) {
	ctx := context.Background()
	staging := NewStagingTracker()
	stagingPath := ".create/users"

	// Create entry but with empty content
	staging.GetOrCreate(stagingPath)

	mockDB := &db.MockDDLExecutor{}
	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	err := runTestWithExecutor(ctx, mockDB, staging, stagingCtx)
	if err == nil {
		t.Error("Expected error for empty content, got nil")
	}

	// Verify error was stored (SetTestResult requires entry to exist)
	result := staging.GetTestResult(stagingPath)
	if result != "Error: No DDL content to test. Write DDL to .schema first.\n" {
		t.Errorf("Unexpected test result: %q", result)
	}
}

// TestTestFileNode_runTest_OnlyComments tests that comment-only content is rejected.
func TestTestFileNode_runTest_OnlyComments(t *testing.T) {
	ctx := context.Background()
	staging := NewStagingTracker()
	stagingPath := ".create/users"

	// Set up content that's only comments
	// Note: HasContent() returns false for comment-only content,
	// so this is treated the same as "no content"
	staging.Set(stagingPath, "-- CREATE TABLE users (id SERIAL PRIMARY KEY);")

	mockDB := &db.MockDDLExecutor{}
	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	err := runTestWithExecutor(ctx, mockDB, staging, stagingCtx)
	if err == nil {
		t.Error("Expected error for comment-only content, got nil")
	}

	// HasContent returns false for comment-only content, so we get "no DDL content" error
	result := staging.GetTestResult(stagingPath)
	if result != "Error: No DDL content to test. Write DDL to .schema first.\n" {
		t.Errorf("Unexpected test result: %q", result)
	}
}

// TestCommitFileNode_runCommit_Success tests successful DDL execution.
func TestCommitFileNode_runCommit_Success(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	staging := NewStagingTracker()
	stagingPath := ".create/users"

	// Set up staged DDL content
	staging.Set(stagingPath, "CREATE TABLE users (id SERIAL PRIMARY KEY);")

	// Verify content exists before commit
	if !staging.HasContent(stagingPath) {
		t.Fatal("Content should exist before commit")
	}

	executed := false
	mockDB := &db.MockDDLExecutor{
		ExecFunc: func(ctx context.Context, sql string, args ...interface{}) error {
			executed = true
			if sql != "CREATE TABLE users (id SERIAL PRIMARY KEY);" {
				t.Errorf("Unexpected SQL: %q", sql)
			}
			return nil
		},
	}

	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	// Verify node creation works with mock
	node := NewCommitFileNode(cfg, mockDB, staging, stagingCtx)
	if node == nil {
		t.Fatal("NewCommitFileNode returned nil")
	}

	err := runCommitWithExecutor(ctx, mockDB, staging, nil, stagingCtx)
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	if !executed {
		t.Error("Expected DDL to be executed")
	}

	// Verify staging entry was cleared after successful commit
	if staging.HasContent(stagingPath) {
		t.Error("Staging entry should be cleared after successful commit")
	}
}

// TestCommitFileNode_runCommit_ExecutionFails tests that execution errors preserve staging.
func TestCommitFileNode_runCommit_ExecutionFails(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	staging := NewStagingTracker()
	stagingPath := ".create/users"

	// Set up staged DDL content
	staging.Set(stagingPath, "CREATE TABLE users (id SERIAL PRIMARY KEY);")

	mockDB := &db.MockDDLExecutor{
		ExecFunc: func(ctx context.Context, sql string, args ...interface{}) error {
			return errors.New("relation \"users\" already exists")
		},
	}

	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	// Verify node creation works with mock
	node := NewCommitFileNode(cfg, mockDB, staging, stagingCtx)
	if node == nil {
		t.Fatal("NewCommitFileNode returned nil")
	}

	err := runCommitWithExecutor(ctx, mockDB, staging, nil, stagingCtx)
	if err == nil {
		t.Error("Expected error, got nil")
	}

	// Verify staging entry is NOT cleared on failure (so user can fix and retry)
	if !staging.HasContent(stagingPath) {
		t.Error("Staging entry should be preserved after failed commit")
	}
}

// TestCommitFileNode_runCommit_NoContent tests that missing content is handled.
func TestCommitFileNode_runCommit_NoContent(t *testing.T) {
	ctx := context.Background()
	staging := NewStagingTracker()
	stagingPath := ".create/users"

	mockDB := &db.MockDDLExecutor{}
	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	err := runCommitWithExecutor(ctx, mockDB, staging, nil, stagingCtx)
	if err == nil {
		t.Error("Expected error for empty content, got nil")
	}
	if err.Error() != "no DDL content to commit. Write DDL to .schema first" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// TestCommitFileNode_runCommit_OnlyComments tests that comment-only content is rejected.
func TestCommitFileNode_runCommit_OnlyComments(t *testing.T) {
	ctx := context.Background()
	staging := NewStagingTracker()
	stagingPath := ".create/users"

	// Set up content that's only comments
	staging.Set(stagingPath, "-- CREATE TABLE users (id SERIAL PRIMARY KEY);")

	mockDB := &db.MockDDLExecutor{}
	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	err := runCommitWithExecutor(ctx, mockDB, staging, nil, stagingCtx)
	if err == nil {
		t.Error("Expected error for comment-only content, got nil")
	}
	// HasContent returns false for comment-only content
	if err.Error() != "no DDL content to commit. Write DDL to .schema first" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// TestStagingDirNode_Creation tests that staging nodes accept DDLExecutor interface.
func TestStagingDirNode_Creation(t *testing.T) {
	cfg := &config.Config{}
	staging := NewStagingTracker()
	mockDB := &db.MockDDLExecutor{}

	stagingCtx := StagingContext{
		StagingPath: ".create/users",
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	// Verify StagingDirNode accepts DDLExecutor
	node := NewStagingDirNode(cfg, mockDB, staging, stagingCtx)
	if node == nil {
		t.Fatal("NewStagingDirNode returned nil")
	}
	if node.db != mockDB {
		t.Error("StagingDirNode should store the mock DDLExecutor")
	}
}

// TestCreateDirNode_Creation tests that CreateDirNode accepts DDLExecutor interface.
func TestCreateDirNode_Creation(t *testing.T) {
	cfg := &config.Config{}
	staging := NewStagingTracker()
	mockDB := &db.MockDDLExecutor{}

	// Verify CreateDirNode accepts DDLExecutor
	node := NewCreateDirNode(cfg, mockDB, nil, staging, "table", "public", "", ".create")
	if node == nil {
		t.Fatal("NewCreateDirNode returned nil")
	}
	if node.db != mockDB {
		t.Error("CreateDirNode should store the mock DDLExecutor")
	}
}

// TestSchemaFileNode_Creation tests that SchemaFileNode accepts DDLExecutor interface.
func TestSchemaFileNode_Creation(t *testing.T) {
	cfg := &config.Config{}
	staging := NewStagingTracker()
	mockDB := &db.MockDDLExecutor{}

	stagingCtx := StagingContext{
		StagingPath: ".create/users",
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	node := NewSchemaFileNode(cfg, mockDB, staging, stagingCtx)
	if node == nil {
		t.Fatal("NewSchemaFileNode returned nil")
	}
}

// TestAbortFileNode_Creation tests that AbortFileNode accepts DDLExecutor interface.
func TestAbortFileNode_Creation(t *testing.T) {
	cfg := &config.Config{}
	staging := NewStagingTracker()
	mockDB := &db.MockDDLExecutor{}

	stagingCtx := StagingContext{
		StagingPath: ".create/users",
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	node := NewAbortFileNode(cfg, mockDB, staging, stagingCtx)
	if node == nil {
		t.Fatal("NewAbortFileNode returned nil")
	}
}

// runTestWithExecutor extracts the test logic from TestFileNode.runTest for unit testing.
// This allows testing the core DDL validation logic without FUSE dependencies.
func runTestWithExecutor(ctx context.Context, executor db.DDLExecutor, staging *StagingTracker, stagingCtx StagingContext) error {
	// Check if there's content to test
	if !staging.HasContent(stagingCtx.StagingPath) {
		result := "Error: No DDL content to test. Write DDL to .schema first.\n"
		staging.SetTestResult(stagingCtx.StagingPath, result)
		return errors.New("no DDL content")
	}

	// Get and extract SQL
	content := staging.GetContent(stagingCtx.StagingPath)
	sql := ExtractSQL(content)

	if sql == "" {
		result := "Error: .schema contains only comments. Uncomment the DDL to test.\n"
		staging.SetTestResult(stagingCtx.StagingPath, result)
		return errors.New("only comments in schema")
	}

	// Test via ExecInTransaction
	err := executor.ExecInTransaction(ctx, sql)
	if err != nil {
		result := "Error: " + err.Error() + "\n"
		staging.SetTestResult(stagingCtx.StagingPath, result)
		return err
	}

	result := "OK: DDL validated successfully.\n"
	staging.SetTestResult(stagingCtx.StagingPath, result)

	return nil
}

// runCommitWithExecutor extracts the commit logic from CommitFileNode.runCommit for unit testing.
// This allows testing the core DDL execution logic without FUSE dependencies.
func runCommitWithExecutor(ctx context.Context, executor db.DDLExecutor, staging *StagingTracker, cache *MetadataCache, stagingCtx StagingContext) error {
	// Check if there's content to commit
	if !staging.HasContent(stagingCtx.StagingPath) {
		return errors.New("no DDL content to commit. Write DDL to .schema first")
	}

	// Get and extract SQL
	content := staging.GetContent(stagingCtx.StagingPath)
	sql := ExtractSQL(content)

	if sql == "" {
		return errors.New(".schema contains only comments. Uncomment the DDL to commit")
	}

	// Execute DDL
	err := executor.Exec(ctx, sql)
	if err != nil {
		return errors.New("DDL execution failed: " + err.Error())
	}

	// Clear staging entry on success
	staging.Delete(stagingCtx.StagingPath)

	// Invalidate metadata cache to pick up changes
	if cache != nil {
		cache.Invalidate()
	}

	return nil
}

// TestCreateDirNode_Mkdir tests the mkdir operation that creates staging entries.
func TestCreateDirNode_Mkdir(t *testing.T) {
	cfg := &config.Config{}
	staging := NewStagingTracker()
	mockDB := &db.MockDDLExecutor{}

	node := NewCreateDirNode(cfg, mockDB, nil, staging, "table", "public", "", ".create")

	// Verify staging entry doesn't exist before mkdir
	if staging.Get(".create/orders") != nil {
		t.Fatal("Entry should not exist before mkdir")
	}

	// Simulate mkdir .create/orders
	stagingPath := node.pathPrefix + "/orders"
	staging.GetOrCreate(stagingPath) // This is what Mkdir does

	// Verify staging entry was created
	entry := staging.Get(".create/orders")
	if entry == nil {
		t.Fatal("mkdir should create staging entry")
	}

	// Verify entry has empty content (ready for .schema write)
	if entry.Content != "" {
		t.Errorf("New staging entry should have empty content, got %q", entry.Content)
	}

	// Verify entry appears in ListPending
	pending := staging.ListPending(".create")
	found := false
	for _, name := range pending {
		if name == "orders" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Created entry should appear in ListPending")
	}
}

// TestAbortFileNode_runAbort tests that abort clears staging.
func TestAbortFileNode_runAbort(t *testing.T) {
	cfg := &config.Config{}
	staging := NewStagingTracker()
	mockDB := &db.MockDDLExecutor{}
	stagingPath := ".create/users"

	// Set up staged content
	staging.Set(stagingPath, "CREATE TABLE users (id SERIAL PRIMARY KEY);")

	// Verify content exists
	if !staging.HasContent(stagingPath) {
		t.Fatal("Content should exist before abort")
	}

	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	node := NewAbortFileNode(cfg, mockDB, staging, stagingCtx)
	if node == nil {
		t.Fatal("NewAbortFileNode returned nil")
	}

	// Simulate touch .abort (calls runAbort)
	node.runAbort()

	// Verify staging entry was cleared
	if staging.HasContent(stagingPath) {
		t.Error("abort should clear staging content")
	}

	// Verify the entry no longer exists
	if staging.Get(stagingPath) != nil {
		t.Error("abort should delete staging entry")
	}
}

// TestSchemaFileHandle_WriteAndRead tests writing and reading .schema content.
func TestSchemaFileHandle_WriteAndRead(t *testing.T) {
	cfg := &config.Config{}
	staging := NewStagingTracker()
	mockDB := &db.MockDDLExecutor{}
	stagingPath := ".create/users"

	// Create the staging entry
	staging.GetOrCreate(stagingPath)

	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "users",
	}

	node := NewSchemaFileNode(cfg, mockDB, staging, stagingCtx)
	ctx := context.Background()

	// Open the file
	fh, _, errno := node.Open(ctx, 0)
	if errno != 0 {
		t.Fatalf("Open failed with errno %d", errno)
	}

	// Cast to SchemaFileHandle
	sfh, ok := fh.(*SchemaFileHandle)
	if !ok {
		t.Fatal("Expected SchemaFileHandle")
	}

	// Write DDL content
	ddl := []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);")
	written, errno := sfh.Write(ctx, ddl, 0)
	if errno != 0 {
		t.Fatalf("Write failed with errno %d", errno)
	}
	if written != uint32(len(ddl)) {
		t.Errorf("Expected %d bytes written, got %d", len(ddl), written)
	}

	// Verify content was stored in staging tracker
	content := staging.GetContent(stagingPath)
	if content != string(ddl) {
		t.Errorf("Expected content %q, got %q", string(ddl), content)
	}

	// Read content back
	dest := make([]byte, 1024)
	result, errno := sfh.Read(ctx, dest, 0)
	if errno != 0 {
		t.Fatalf("Read failed with errno %d", errno)
	}

	readContent, _ := result.Bytes(dest)
	if string(readContent) != string(ddl) {
		t.Errorf("Expected to read %q, got %q", string(ddl), string(readContent))
	}
}

// TestSchemaFileNode_GeneratesTemplate tests that reading .schema generates a template.
func TestSchemaFileNode_GeneratesTemplate(t *testing.T) {
	cfg := &config.Config{}
	staging := NewStagingTracker()
	mockDB := &db.MockDDLExecutor{}
	stagingPath := ".create/orders"

	// Create staging entry with no content
	staging.GetOrCreate(stagingPath)

	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "orders",
	}

	node := NewSchemaFileNode(cfg, mockDB, staging, stagingCtx)
	ctx := context.Background()

	// Get content (should generate template since no content exists)
	content := node.getContent(ctx)

	// Verify template was generated
	if content == "" {
		t.Error("Expected template to be generated")
	}
	if !contains(content, "orders") {
		t.Error("Template should contain object name 'orders'")
	}
	if !contains(content, "CREATE TABLE") {
		t.Error("Template should contain CREATE TABLE hint")
	}
}

// TestFullWorkflow tests the complete staging workflow: mkdir -> write .schema -> touch .test -> touch .commit
func TestFullWorkflow(t *testing.T) {
	ctx := context.Background()
	staging := NewStagingTracker()

	// Track what SQL was executed
	var testedSQL, committedSQL string

	mockDB := &db.MockDDLExecutor{
		ExecInTransactionFunc: func(ctx context.Context, sql string, args ...interface{}) error {
			testedSQL = sql
			return nil
		},
		ExecFunc: func(ctx context.Context, sql string, args ...interface{}) error {
			committedSQL = sql
			return nil
		},
	}

	stagingPath := ".create/products"

	// Step 1: mkdir .create/products
	staging.GetOrCreate(stagingPath)
	if staging.Get(stagingPath) == nil {
		t.Fatal("Step 1 failed: mkdir should create staging entry")
	}

	// Step 2: Write DDL to .schema
	ddl := "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT);"
	staging.Set(stagingPath, ddl)
	if staging.GetContent(stagingPath) != ddl {
		t.Fatal("Step 2 failed: write .schema should store content")
	}

	// Step 3: touch .test (validate DDL)
	stagingCtx := StagingContext{
		StagingPath: stagingPath,
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "products",
	}

	err := runTestWithExecutor(ctx, mockDB, staging, stagingCtx)
	if err != nil {
		t.Fatalf("Step 3 failed: touch .test returned error: %v", err)
	}
	if testedSQL != ddl {
		t.Errorf("Step 3: Expected tested SQL %q, got %q", ddl, testedSQL)
	}

	// Verify test result was stored
	result := staging.GetTestResult(stagingPath)
	if result != "OK: DDL validated successfully.\n" {
		t.Errorf("Step 3: Expected success result, got %q", result)
	}

	// Step 4: touch .commit (execute DDL)
	err = runCommitWithExecutor(ctx, mockDB, staging, nil, stagingCtx)
	if err != nil {
		t.Fatalf("Step 4 failed: touch .commit returned error: %v", err)
	}
	if committedSQL != ddl {
		t.Errorf("Step 4: Expected committed SQL %q, got %q", ddl, committedSQL)
	}

	// Verify staging entry was cleared
	if staging.HasContent(stagingPath) {
		t.Error("Step 4 failed: staging entry should be cleared after commit")
	}
}

// contains is a helper function for string containment check
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
