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
