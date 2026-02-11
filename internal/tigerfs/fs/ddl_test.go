package fs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewDDLManager tests the DDLManager constructor.
func TestNewDDLManager(t *testing.T) {
	dm := NewDDLManager(nil, 0)
	require.NotNil(t, dm)
	assert.NotNil(t, dm.sessions)
}

// TestDDLManager_CreateSession tests creating DDL staging sessions.
func TestDDLManager_CreateSession(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	// Create a session
	id, err := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	// Session should exist
	session := dm.GetSession(id)
	require.NotNil(t, session)
	assert.Equal(t, DDLCreate, session.Operation)
	assert.Equal(t, "table", session.ObjectType)
	assert.Equal(t, "orders", session.ObjectName)
	assert.Equal(t, "public", session.Schema)
	assert.False(t, session.Validated)
	assert.NotZero(t, session.CreatedAt)
}

// TestDDLManager_CreateSession_Index tests creating an index DDL session.
func TestDDLManager_CreateSession_Index(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, err := dm.CreateSession(DDLCreate, "index", "public", "users_email_idx", "users")
	require.NoError(t, err)

	session := dm.GetSession(id)
	require.NotNil(t, session)
	assert.Equal(t, "index", session.ObjectType)
	assert.Equal(t, "users_email_idx", session.ObjectName)
	assert.Equal(t, "users", session.ParentTable)
}

// TestDDLManager_WriteSQL tests writing SQL content to a session.
func TestDDLManager_WriteSQL(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")

	// Write SQL
	sql := "CREATE TABLE orders (id SERIAL PRIMARY KEY);"
	err := dm.WriteSQL(id, sql)
	require.NoError(t, err)

	// Verify content
	session := dm.GetSession(id)
	assert.Equal(t, sql, session.SQL)
}

// TestDDLManager_WriteSQL_NonExistent tests writing to non-existent session.
func TestDDLManager_WriteSQL_NonExistent(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	err := dm.WriteSQL("nonexistent", "CREATE TABLE foo;")
	assert.Error(t, err)
}

// TestDDLManager_GetSQL tests reading SQL content.
func TestDDLManager_GetSQL(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABLE orders;")

	sql := dm.GetSQL(id)
	assert.Equal(t, "CREATE TABLE orders;", sql)
}

// TestDDLManager_GetSQL_Template tests getting template when no SQL written.
func TestDDLManager_GetSQL_Template(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")

	sql := dm.GetSQL(id)
	assert.Contains(t, sql, "CREATE TABLE")
	assert.Contains(t, sql, "orders")
}

// TestDDLManager_Test tests DDL validation.
func TestDDLManager_Test(t *testing.T) {
	mockDB := &mockDBClient{
		execInTxSuccess: true,
	}
	dm := NewDDLManager(mockDB, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABLE orders (id SERIAL PRIMARY KEY);")

	// Run test
	result, err := dm.Test(context.Background(), id)
	require.NoError(t, err)
	assert.Contains(t, result, "OK")

	// Session should be marked validated
	session := dm.GetSession(id)
	assert.True(t, session.Validated)
	assert.Equal(t, result, session.TestLog)
}

// TestDDLManager_Test_Failure tests DDL validation failure.
func TestDDLManager_Test_Failure(t *testing.T) {
	mockDB := &mockDBClient{
		execInTxError: assert.AnError,
	}
	dm := NewDDLManager(mockDB, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABL orders;") // intentional typo

	// Run test - should return error in result
	result, err := dm.Test(context.Background(), id)
	require.NoError(t, err) // Test method doesn't error, it records the error
	assert.Contains(t, result, "Error")

	// Session should not be marked validated
	session := dm.GetSession(id)
	assert.False(t, session.Validated)
}

// TestDDLManager_Test_NoContent tests validation with no content.
func TestDDLManager_Test_NoContent(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")

	// Run test without writing SQL
	result, err := dm.Test(context.Background(), id)
	require.NoError(t, err)
	assert.Contains(t, result, "Error")
	assert.Contains(t, result, "No DDL content")
}

// TestDDLManager_Test_OnlyComments tests validation with only comments.
func TestDDLManager_Test_OnlyComments(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "-- This is just a comment\n/* Another comment */")

	result, err := dm.Test(context.Background(), id)
	require.NoError(t, err)
	assert.Contains(t, result, "Error")
	assert.Contains(t, result, "only comments")
}

// TestDDLManager_Commit tests DDL execution.
func TestDDLManager_Commit(t *testing.T) {
	mockDB := &mockDBClient{
		execSuccess: true,
	}
	dm := NewDDLManager(mockDB, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABLE orders (id SERIAL PRIMARY KEY);")

	// Commit
	err := dm.Commit(context.Background(), id)
	require.NoError(t, err)

	// Session should be marked completed (not removed)
	session := dm.GetSession(id)
	require.NotNil(t, session)
	assert.True(t, session.Completed)
	assert.False(t, session.CompletedAt.IsZero())

	// DB should have been called
	assert.True(t, mockDB.execCalled)
}

// TestDDLManager_Commit_Failure tests DDL execution failure.
func TestDDLManager_Commit_Failure(t *testing.T) {
	mockDB := &mockDBClient{
		execError: assert.AnError,
	}
	dm := NewDDLManager(mockDB, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABLE orders;")

	// Commit should fail
	err := dm.Commit(context.Background(), id)
	assert.Error(t, err)

	// Session should still exist (not removed on failure)
	session := dm.GetSession(id)
	assert.NotNil(t, session)
}

// TestDDLManager_Commit_NoContent tests commit with no content.
func TestDDLManager_Commit_NoContent(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")

	err := dm.Commit(context.Background(), id)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no DDL content")
}

// TestDDLManager_Abort tests aborting a DDL session.
func TestDDLManager_Abort(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABLE orders;")

	// Abort
	err := dm.Abort(id)
	require.NoError(t, err)

	// Session should be marked completed (not removed)
	session := dm.GetSession(id)
	require.NotNil(t, session)
	assert.True(t, session.Completed)
	assert.False(t, session.CompletedAt.IsZero())
}

// TestDDLManager_Abort_Idempotent tests that abort is idempotent.
func TestDDLManager_Abort_Idempotent(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")

	// Abort twice - should not error
	err := dm.Abort(id)
	require.NoError(t, err)

	err = dm.Abort(id)
	require.NoError(t, err)
}

// TestDDLManager_ListSessions tests listing sessions.
func TestDDLManager_ListSessions(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	// Create sessions
	dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.CreateSession(DDLCreate, "table", "public", "users", "")
	dm.CreateSession(DDLModify, "table", "public", "products", "")

	// List all create sessions
	sessions := dm.ListSessions(DDLCreate)
	assert.Len(t, sessions, 2)

	// List modify sessions
	sessions = dm.ListSessions(DDLModify)
	assert.Len(t, sessions, 1)
}

// TestDDLManager_GetTestLog tests reading test log.
func TestDDLManager_GetTestLog(t *testing.T) {
	mockDB := &mockDBClient{
		execInTxSuccess: true,
	}
	dm := NewDDLManager(mockDB, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABLE orders;")
	dm.Test(context.Background(), id)

	log := dm.GetTestLog(id)
	assert.Contains(t, log, "OK")
}

// TestDDLManager_GenerateTemplate_Table tests table template generation.
func TestDDLManager_GenerateTemplate_Table(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	template := dm.GetSQL(id)

	assert.Contains(t, template, "CREATE TABLE")
	assert.Contains(t, template, "orders")
	assert.Contains(t, template, "PRIMARY KEY")
}

// TestDDLManager_GenerateTemplate_Index tests index template generation.
func TestDDLManager_GenerateTemplate_Index(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "index", "public", "users_email_idx", "users")
	template := dm.GetSQL(id)

	assert.Contains(t, template, "CREATE INDEX")
	assert.Contains(t, template, "users_email_idx")
	assert.Contains(t, template, "users")
}

// TestDDLManager_GenerateTemplate_Schema tests schema template generation.
func TestDDLManager_GenerateTemplate_Schema(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "schema", "", "myschema", "")
	template := dm.GetSQL(id)

	assert.Contains(t, template, "CREATE SCHEMA")
	assert.Contains(t, template, "myschema")
}

// TestDDLManager_GenerateTemplate_View tests view template generation.
func TestDDLManager_GenerateTemplate_View(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "view", "public", "active_users", "")
	template := dm.GetSQL(id)

	assert.Contains(t, template, "CREATE VIEW")
	assert.Contains(t, template, "active_users")
}

// TestDDLManager_GenerateTemplate_Modify tests modify template generation.
func TestDDLManager_GenerateTemplate_Modify(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLModify, "table", "public", "users", "")
	template := dm.GetSQL(id)

	assert.Contains(t, template, "ALTER TABLE")
	assert.Contains(t, template, "users")
}

// TestDDLManager_GenerateTemplate_Delete tests delete template generation.
func TestDDLManager_GenerateTemplate_Delete(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLDelete, "table", "public", "users", "")
	template := dm.GetSQL(id)

	assert.Contains(t, template, "DROP TABLE")
	assert.Contains(t, template, "users")
}

// TestIsEmptyOrCommented tests SQL content detection.
func TestIsEmptyOrCommented(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected bool
	}{
		{"empty", "", true},
		{"whitespace", "   \n\t  ", true},
		{"line comment", "-- just a comment", true},
		{"block comment", "/* block comment */", true},
		{"mixed comments", "-- line\n/* block */\n-- another", true},
		{"sql statement", "CREATE TABLE foo;", false},
		{"sql with comment", "-- comment\nCREATE TABLE foo;", false},
		{"commented sql", "-- CREATE TABLE foo;", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsEmptyOrCommented(tt.content)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractSQL tests SQL extraction from content.
func TestExtractSQL(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected string
	}{
		{"simple", "CREATE TABLE foo;", "CREATE TABLE foo;"},
		{"with line comment", "-- comment\nCREATE TABLE foo;", "CREATE TABLE foo;"},
		{"with block comment", "/* comment */CREATE TABLE foo;", "CREATE TABLE foo;"},
		{"inline comment", "CREATE TABLE foo; -- inline", "CREATE TABLE foo;"},
		{"multi-line", "CREATE TABLE\nfoo;", "CREATE TABLE\nfoo;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractSQL(tt.content)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDDLStagingEntry tests the DDLStagingEntry struct.
func TestDDLStagingEntry(t *testing.T) {
	entry := &DDLStagingEntry{
		ID:          "test-id",
		Operation:   DDLCreate,
		ObjectType:  "table",
		ObjectName:  "orders",
		Schema:      "public",
		ParentTable: "",
		SQL:         "CREATE TABLE orders;",
		TestLog:     "OK",
		Validated:   true,
		CreatedAt:   time.Now(),
	}

	assert.Equal(t, "test-id", entry.ID)
	assert.Equal(t, DDLCreate, entry.Operation)
	assert.True(t, entry.Validated)
}

// TestDDLManager_FindSessionByName tests finding sessions by name.
func TestDDLManager_FindSessionByName(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	// Create multiple sessions
	id1, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.CreateSession(DDLCreate, "table", "public", "users", "")
	dm.CreateSession(DDLModify, "table", "public", "orders", "") // Same name, different op

	// Find by name and operation
	found := dm.FindSessionByName(DDLCreate, "orders")
	assert.Equal(t, id1, found)

	// Find non-existent
	notFound := dm.FindSessionByName(DDLCreate, "products")
	assert.Empty(t, notFound)
}

// TestDDLManager_FindSessionByName_WrongOp tests that wrong operation type doesn't match.
func TestDDLManager_FindSessionByName_WrongOp(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	// Create a DDLCreate session
	dm.CreateSession(DDLCreate, "table", "public", "orders", "")

	// Try to find with wrong operation type
	found := dm.FindSessionByName(DDLDelete, "orders")
	assert.Empty(t, found)
}

// TestParseDDLOpType tests parsing DDL operation strings.
func TestParseDDLOpType(t *testing.T) {
	tests := []struct {
		input    string
		expected DDLOpType
		valid    bool
	}{
		{"create", DDLCreate, true},
		{"modify", DDLModify, true},
		{"delete", DDLDelete, true},
		{"invalid", DDLCreate, false},
		{"CREATE", DDLCreate, false}, // case-sensitive
		{"", DDLCreate, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			op, valid := ParseDDLOpType(tt.input)
			assert.Equal(t, tt.expected, op)
			assert.Equal(t, tt.valid, valid)
		})
	}
}

// TestDDLManager_UpdatedAt tests that UpdatedAt is set and updated correctly.
func TestDDLManager_UpdatedAt(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	// UpdatedAt is set on creation
	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	session := dm.GetSession(id)
	require.NotNil(t, session)
	assert.False(t, session.UpdatedAt.IsZero(), "UpdatedAt should be set on creation")
	assert.Equal(t, session.CreatedAt, session.UpdatedAt, "UpdatedAt should equal CreatedAt initially")

	// UpdatedAt is stable across repeated reads
	time.Sleep(time.Millisecond)
	session2 := dm.GetSession(id)
	assert.Equal(t, session.UpdatedAt, session2.UpdatedAt, "UpdatedAt should be stable without writes")

	// UpdatedAt advances on WriteSQL
	beforeWrite := session.UpdatedAt
	time.Sleep(time.Millisecond)
	dm.WriteSQL(id, "CREATE TABLE orders (id SERIAL PRIMARY KEY);")
	session = dm.GetSession(id)
	assert.True(t, session.UpdatedAt.After(beforeWrite), "UpdatedAt should advance after WriteSQL")
}

// TestDDLManager_Commit_MarksCompleted tests that Commit marks session as completed.
func TestDDLManager_Commit_MarksCompleted(t *testing.T) {
	mockDB := &mockDBClient{execSuccess: true}
	dm := NewDDLManager(mockDB, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABLE orders (id SERIAL PRIMARY KEY);")

	err := dm.Commit(context.Background(), id)
	require.NoError(t, err)

	session := dm.GetSession(id)
	require.NotNil(t, session, "session should still exist after commit")
	assert.True(t, session.Completed)
	assert.False(t, session.CompletedAt.IsZero())
}

// TestDDLManager_Abort_MarksCompleted tests that Abort marks session as completed.
func TestDDLManager_Abort_MarksCompleted(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.WriteSQL(id, "CREATE TABLE orders;")

	err := dm.Abort(id)
	require.NoError(t, err)

	session := dm.GetSession(id)
	require.NotNil(t, session, "session should still exist after abort")
	assert.True(t, session.Completed)
	assert.False(t, session.CompletedAt.IsZero())
}

// TestDDLManager_FindSessionByName_CompletedWithinGrace tests that completed sessions
// are still found within the grace period.
func TestDDLManager_FindSessionByName_CompletedWithinGrace(t *testing.T) {
	dm := NewDDLManager(nil, 5*time.Second) // 5s grace period

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.Abort(id)

	// Should still be found within grace period
	found := dm.FindSessionByName(DDLCreate, "orders")
	assert.Equal(t, id, found, "completed session should be found within grace period")
}

// TestDDLManager_FindSessionByName_ReapsExpired tests that FindSessionByName
// reaps completed sessions past their grace period.
func TestDDLManager_FindSessionByName_ReapsExpired(t *testing.T) {
	dm := NewDDLManager(nil, 1*time.Millisecond) // 1ms grace period

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.Abort(id)

	// Wait for grace period to expire
	time.Sleep(5 * time.Millisecond)

	// Should be reaped
	found := dm.FindSessionByName(DDLCreate, "orders")
	assert.Empty(t, found, "expired completed session should be reaped")

	// Session should be gone
	session := dm.GetSession(id)
	assert.Nil(t, session, "reaped session should not exist")
}

// TestDDLManager_ListSessionEntries_CompletedWithinGrace tests that completed sessions
// are included in listings within the grace period.
func TestDDLManager_ListSessionEntries_CompletedWithinGrace(t *testing.T) {
	dm := NewDDLManager(nil, 5*time.Second)

	dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	id2, _ := dm.CreateSession(DDLCreate, "table", "public", "users", "")
	dm.Abort(id2) // Mark users as completed

	entries := dm.ListSessionEntries(DDLCreate)
	assert.Len(t, entries, 2, "completed session within grace period should be listed")
}

// TestDDLManager_ListSessionEntries_ReapsExpired tests that ListSessionEntries
// excludes completed sessions past their grace period.
func TestDDLManager_ListSessionEntries_ReapsExpired(t *testing.T) {
	dm := NewDDLManager(nil, 1*time.Millisecond)

	dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	id2, _ := dm.CreateSession(DDLCreate, "table", "public", "users", "")
	dm.Abort(id2) // Mark users as completed

	time.Sleep(5 * time.Millisecond)

	entries := dm.ListSessionEntries(DDLCreate)
	assert.Len(t, entries, 1, "expired completed session should be reaped")
	assert.Equal(t, "orders", entries[0].ObjectName)
}

// TestDDLManager_RemoveSession tests permanent session removal.
func TestDDLManager_RemoveSession(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.RemoveSession(id)

	session := dm.GetSession(id)
	assert.Nil(t, session, "removed session should not exist")
}

// TestDDLManager_CreateAfterRemove tests creating a new session with the same name
// after removing the old one.
func TestDDLManager_CreateAfterRemove(t *testing.T) {
	dm := NewDDLManager(nil, 0)

	id1, _ := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	dm.RemoveSession(id1)

	id2, err := dm.CreateSession(DDLCreate, "table", "public", "orders", "")
	require.NoError(t, err)
	assert.NotEqual(t, id1, id2)

	session := dm.GetSession(id2)
	require.NotNil(t, session)
	assert.Equal(t, "orders", session.ObjectName)
	assert.False(t, session.Completed)
}
