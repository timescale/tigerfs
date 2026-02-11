package fs

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// DDLOpType identifies the type of DDL operation.
type DDLOpType int

const (
	// DDLCreate represents a CREATE operation (table, index, schema, view).
	DDLCreate DDLOpType = iota
	// DDLModify represents an ALTER/MODIFY operation.
	DDLModify
	// DDLDelete represents a DROP/DELETE operation.
	DDLDelete
)

// DDLStagingEntry tracks a DDL staging session.
//
// A session is created when a user starts a DDL operation (e.g., mkdir /.create/orders).
// The session tracks:
//   - The SQL content being prepared
//   - Validation results from .test
//   - Whether the SQL has been validated
type DDLStagingEntry struct {
	// ID is the unique session identifier.
	ID string
	// Operation is the type of DDL operation (create, modify, delete).
	Operation DDLOpType
	// ObjectType is "table", "index", "schema", or "view".
	ObjectType string
	// ObjectName is the name of the object being created/modified/deleted.
	ObjectName string
	// Schema is the PostgreSQL schema (for tables, indexes, views).
	Schema string
	// ParentTable is the parent table (for indexes).
	ParentTable string
	// SQL is the DDL statement content.
	SQL string
	// TestLog is the output from the last .test validation.
	TestLog string
	// Validated is true if .test succeeded.
	Validated bool
	// CreatedAt is when the session was created.
	CreatedAt time.Time
	// UpdatedAt is when the session content was last modified.
	// Used to provide stable mtime for DDL files (avoids time.Now() per stat call).
	UpdatedAt time.Time
	// Completed is true after a successful commit or abort.
	// The session is kept in memory briefly so filesystem close operations
	// (stat, readdir) don't fail. Cleaned up by lazy reaper.
	Completed   bool
	CompletedAt time.Time
	// ExtraFiles holds editor temp files (swap files, backups, etc.) stored in-memory.
	// Editors like vim and emacs create temporary files alongside the file being edited;
	// these must be stored so editors don't error out.
	ExtraFiles map[string]*ExtraFile
}

// ExtraFile represents an in-memory editor temp file with metadata.
// Tracks size, mtime, and permissions for NFS compliance.
type ExtraFile struct {
	Data      []byte
	ModTime   time.Time
	CreatedAt time.Time
}

// DDLManager handles DDL staging operations.
//
// Thread-safe management of DDL sessions. Each session represents a pending
// DDL operation that can be tested, committed, or aborted.
//
// Typical workflow:
//  1. CreateSession: Start a new DDL staging session
//  2. WriteSQL: Write/update the DDL content
//  3. Test: Validate the DDL (BEGIN/ROLLBACK)
//  4. Commit: Execute the DDL, or Abort: Cancel the session
type DDLManager struct {
	db          db.DDLExecutor
	sessions    map[string]*DDLStagingEntry
	mu          sync.RWMutex
	gracePeriod time.Duration // How long completed sessions stay visible
}

// NewDDLManager creates a new DDL manager.
//
// Parameters:
//   - dbClient: database client for executing DDL (can be nil for template-only use)
//   - gracePeriod: how long completed sessions stay visible for post-close operations.
//     If 0, defaults to 30 seconds.
//
// Returns a new DDLManager ready for use.
func NewDDLManager(dbClient db.DDLExecutor, gracePeriod time.Duration) *DDLManager {
	if gracePeriod == 0 {
		gracePeriod = 30 * time.Second
	}
	return &DDLManager{
		db:          dbClient,
		sessions:    make(map[string]*DDLStagingEntry),
		gracePeriod: gracePeriod,
	}
}

// CreateSession starts a new DDL staging session.
//
// Parameters:
//   - op: the DDL operation type (DDLCreate, DDLModify, DDLDelete)
//   - objectType: "table", "index", "schema", or "view"
//   - schema: PostgreSQL schema name (empty for schema operations)
//   - objectName: name of the object
//   - parentTable: parent table name (for index operations, empty otherwise)
//
// Returns the session ID and nil error on success.
func (m *DDLManager) CreateSession(op DDLOpType, objectType, schema, objectName, parentTable string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := uuid.New().String()
	now := time.Now()
	entry := &DDLStagingEntry{
		ID:          id,
		Operation:   op,
		ObjectType:  objectType,
		ObjectName:  objectName,
		Schema:      schema,
		ParentTable: parentTable,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	m.sessions[id] = entry

	return id, nil
}

// GetSession retrieves a session by ID.
//
// Returns the session if found, or nil if not found.
func (m *DDLManager) GetSession(id string) *DDLStagingEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sessions[id]
}

// WriteSQL updates the SQL content for a session.
//
// Parameters:
//   - sessionID: the session to update
//   - sql: the DDL statement content
//
// Returns an error if the session doesn't exist.
func (m *DDLManager) WriteSQL(sessionID, sql string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return fmt.Errorf("session not found: %s", sessionID)
	}

	session.SQL = sql
	session.Validated = false // Reset validation when content changes
	session.UpdatedAt = time.Now()

	return nil
}

// GetSQL retrieves the SQL content for a session.
// If no SQL has been written, returns a generated template.
//
// Parameters:
//   - sessionID: the session to query
//
// Returns the SQL content or template, or empty string if session not found.
func (m *DDLManager) GetSQL(sessionID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return ""
	}

	if session.SQL != "" {
		return session.SQL
	}

	// Generate template
	return m.generateTemplate(session)
}

// Test validates the DDL statement via BEGIN/ROLLBACK.
//
// The SQL is executed within a transaction that is always rolled back,
// allowing syntax and semantic validation without making changes.
//
// Parameters:
//   - ctx: context for cancellation
//   - sessionID: the session to test
//
// Returns the test result message. The result is also stored in the session's
// TestLog field. Returns an error only for session lookup failures, not
// for DDL validation failures (which are recorded in the result).
func (m *DDLManager) Test(ctx context.Context, sessionID string) (string, error) {
	m.mu.Lock()
	session, exists := m.sessions[sessionID]
	if !exists {
		m.mu.Unlock()
		return "", fmt.Errorf("session not found: %s", sessionID)
	}

	// Check for content
	if session.SQL == "" || IsEmptyOrCommented(session.SQL) {
		result := "Error: No DDL content to test. Write DDL to sql first.\n"
		if session.SQL != "" && IsEmptyOrCommented(session.SQL) {
			result = "Error: sql contains only comments. Uncomment the DDL to test.\n"
		}
		session.TestLog = result
		session.Validated = false
		session.UpdatedAt = time.Now()
		m.mu.Unlock()
		return result, nil
	}

	// Extract executable SQL
	sql := ExtractSQL(session.SQL)
	m.mu.Unlock()

	// Run test via transaction
	var result string
	var validated bool

	if m.db != nil {
		err := m.db.ExecInTransaction(ctx, sql)
		if err != nil {
			result = fmt.Sprintf("Error: %s\n", err.Error())
			validated = false
		} else {
			result = "OK: DDL validated successfully.\n"
			validated = true
		}
	} else {
		result = "Error: No database connection for testing.\n"
		validated = false
	}

	// Store result
	m.mu.Lock()
	if session, exists := m.sessions[sessionID]; exists {
		session.TestLog = result
		session.Validated = validated
		session.UpdatedAt = time.Now()
	}
	m.mu.Unlock()

	return result, nil
}

// GetTestLog retrieves the test log for a session.
//
// Returns the test log content, or empty string if session not found
// or no test has been run.
func (m *DDLManager) GetTestLog(sessionID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return ""
	}

	return session.TestLog
}

// Commit executes the DDL statement.
//
// On success, the session is removed. On failure, the session remains
// so the user can fix the SQL and retry.
//
// Parameters:
//   - ctx: context for cancellation
//   - sessionID: the session to commit
//
// Returns nil on success, or an error describing the failure.
func (m *DDLManager) Commit(ctx context.Context, sessionID string) error {
	m.mu.Lock()
	session, exists := m.sessions[sessionID]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("session not found: %s", sessionID)
	}

	// Check for content
	if session.SQL == "" || IsEmptyOrCommented(session.SQL) {
		m.mu.Unlock()
		if session.SQL != "" && IsEmptyOrCommented(session.SQL) {
			return fmt.Errorf("sql contains only comments. Uncomment the DDL to commit")
		}
		return fmt.Errorf("no DDL content to commit. Write DDL to sql first")
	}

	// Extract executable SQL
	sql := ExtractSQL(session.SQL)
	m.mu.Unlock()

	// Execute DDL
	if m.db == nil {
		return fmt.Errorf("no database connection for commit")
	}

	err := m.db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("DDL execution failed: %w", err)
	}

	// Mark session as completed (keep for grace period so post-close
	// filesystem operations like stat/readdir don't fail)
	m.mu.Lock()
	if session, exists := m.sessions[sessionID]; exists {
		session.Completed = true
		session.CompletedAt = time.Now()
		session.UpdatedAt = time.Now()
	}
	m.mu.Unlock()

	return nil
}

// Abort cancels a DDL staging session.
//
// This is idempotent - calling Abort on a non-existent or already-aborted
// session does not return an error.
//
// Parameters:
//   - sessionID: the session to abort
//
// Returns nil (always succeeds).
func (m *DDLManager) Abort(sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if session, exists := m.sessions[sessionID]; exists {
		session.Completed = true
		session.CompletedAt = time.Now()
		session.UpdatedAt = time.Now()
	}
	return nil
}

// RemoveSession permanently deletes a session from the manager.
// Used to clear completed sessions when creating a new session with the same name.
func (m *DDLManager) RemoveSession(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
}

// ListSessions returns all sessions of a given operation type.
//
// Parameters:
//   - op: the operation type to filter by
//
// Returns a slice of matching session IDs.
func (m *DDLManager) ListSessions(op DDLOpType) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ids []string
	for id, session := range m.sessions {
		if session.Operation == op {
			ids = append(ids, id)
		}
	}
	return ids
}

// ListSessionEntries returns all staging session entries matching an operation type.
//
// This is used by ReadDir to list staging directories with their metadata.
//
// Parameters:
//   - op: the operation type to filter by
//
// Returns a slice of matching session entries (copies, not pointers).
func (m *DDLManager) ListSessionEntries(op DDLOpType) []DDLStagingEntry {
	m.mu.Lock() // Write lock for lazy cleanup
	defer m.mu.Unlock()

	now := time.Now()
	var entries []DDLStagingEntry
	for id, session := range m.sessions {
		if session.Operation != op {
			continue
		}
		// Reap expired completed sessions
		if session.Completed && now.Sub(session.CompletedAt) > m.gracePeriod {
			delete(m.sessions, id)
			continue
		}
		entries = append(entries, *session)
	}
	return entries
}

// FindSessionByName finds a session by operation type and object name.
//
// This is used by the Operations layer, which receives object names from
// path parsing rather than session UUIDs.
//
// Parameters:
//   - op: the DDL operation type to match
//   - objectName: the name of the object being created/modified/deleted
//
// Returns the session ID if found, or empty string if not found.
func (m *DDLManager) FindSessionByName(op DDLOpType, objectName string) string {
	m.mu.Lock() // Write lock for lazy cleanup
	defer m.mu.Unlock()

	for id, session := range m.sessions {
		if session.Operation == op && session.ObjectName == objectName {
			// Reap expired completed session
			if session.Completed && time.Since(session.CompletedAt) > m.gracePeriod {
				delete(m.sessions, id)
				return ""
			}
			return id
		}
	}
	return ""
}

// SetExtraFile stores an editor temp file in a session.
// Updates the file's ModTime on each write for NFS mtime compliance.
func (m *DDLManager) SetExtraFile(sessionID, filename string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	if !exists {
		return
	}

	if session.ExtraFiles == nil {
		session.ExtraFiles = make(map[string]*ExtraFile)
	}

	now := time.Now()
	if ef, exists := session.ExtraFiles[filename]; exists {
		ef.Data = data
		ef.ModTime = now
	} else {
		session.ExtraFiles[filename] = &ExtraFile{
			Data:      data,
			ModTime:   now,
			CreatedAt: now,
		}
	}
}

// GetExtraFile retrieves an editor temp file's data from a session.
// Returns nil if not found.
func (m *DDLManager) GetExtraFile(sessionID, filename string) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[sessionID]
	if !exists || session.ExtraFiles == nil {
		return nil
	}
	ef := session.ExtraFiles[filename]
	if ef == nil {
		return nil
	}
	return ef.Data
}

// GetExtraFileInfo retrieves an editor temp file's metadata from a session.
// Returns nil if not found.
func (m *DDLManager) GetExtraFileInfo(sessionID, filename string) *ExtraFile {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[sessionID]
	if !exists || session.ExtraFiles == nil {
		return nil
	}
	return session.ExtraFiles[filename]
}

// DeleteExtraFile removes an editor temp file from a session.
func (m *DDLManager) DeleteExtraFile(sessionID, filename string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[sessionID]
	if !exists || session.ExtraFiles == nil {
		return
	}
	delete(session.ExtraFiles, filename)
}

// HasExtraFile checks if an editor temp file exists in a session.
func (m *DDLManager) HasExtraFile(sessionID, filename string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[sessionID]
	if !exists || session.ExtraFiles == nil {
		return false
	}
	_, has := session.ExtraFiles[filename]
	return has
}

// ListExtraFiles returns names of all editor temp files in a session.
func (m *DDLManager) ListExtraFiles(sessionID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[sessionID]
	if !exists || session.ExtraFiles == nil {
		return nil
	}

	names := make([]string, 0, len(session.ExtraFiles))
	for name := range session.ExtraFiles {
		names = append(names, name)
	}
	return names
}

// ParseDDLOpType converts a string operation name to DDLOpType.
//
// Parameters:
//   - opStr: operation string ("create", "modify", or "delete")
//
// Returns the corresponding DDLOpType and true if valid, or DDLCreate
// and false if the string is not recognized.
func ParseDDLOpType(opStr string) (DDLOpType, bool) {
	switch opStr {
	case "create":
		return DDLCreate, true
	case "modify":
		return DDLModify, true
	case "delete":
		return DDLDelete, true
	default:
		return DDLCreate, false
	}
}

// generateTemplate generates a DDL template based on the session context.
func (m *DDLManager) generateTemplate(session *DDLStagingEntry) string {
	switch session.Operation {
	case DDLCreate:
		return m.generateCreateTemplate(session)
	case DDLModify:
		return m.generateModifyTemplate(session)
	case DDLDelete:
		return m.generateDeleteTemplate(session)
	default:
		return "-- Unknown operation\n"
	}
}

func (m *DDLManager) generateCreateTemplate(session *DDLStagingEntry) string {
	switch session.ObjectType {
	case "table":
		return fmt.Sprintf(`-- Create table: %s
-- Uncomment and modify the following template:

-- CREATE TABLE %s (
--     id SERIAL PRIMARY KEY,
--     name TEXT NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT NOW()
-- );
`, session.ObjectName, session.ObjectName)

	case "index":
		return fmt.Sprintf(`-- Create index: %s on table %s
-- Uncomment and modify the following template:

-- CREATE INDEX %s ON %s (
--     column_name
-- );
`, session.ObjectName, session.ParentTable, session.ObjectName, session.ParentTable)

	case "schema":
		return fmt.Sprintf(`-- Create schema: %s
-- Uncomment to execute:

-- CREATE SCHEMA %s;
`, session.ObjectName, session.ObjectName)

	case "view":
		return fmt.Sprintf(`-- Create view: %s
-- Uncomment and modify the following template:

-- CREATE VIEW %s AS
-- SELECT
--     column1,
--     column2
-- FROM
--     table_name
-- WHERE
--     condition;
`, session.ObjectName, session.ObjectName)

	default:
		return fmt.Sprintf("-- Create %s: %s\n", session.ObjectType, session.ObjectName)
	}
}

func (m *DDLManager) generateModifyTemplate(session *DDLStagingEntry) string {
	return fmt.Sprintf(`-- Modify %s: %s
-- Examples:
-- ALTER TABLE %s ADD COLUMN column_name TYPE;
-- ALTER TABLE %s DROP COLUMN column_name;
-- ALTER TABLE %s ALTER COLUMN column_name TYPE new_type;

-- Add your ALTER statement below:

`, session.ObjectType, session.ObjectName, session.ObjectName, session.ObjectName, session.ObjectName)
}

func (m *DDLManager) generateDeleteTemplate(session *DDLStagingEntry) string {
	switch session.ObjectType {
	case "table":
		return fmt.Sprintf(`-- Delete table: %s
-- WARNING: This will permanently delete the table and all its data.

-- Uncomment to delete:
-- DROP TABLE %s;

-- Or with CASCADE to also drop dependent objects:
-- DROP TABLE %s CASCADE;
`, session.ObjectName, session.ObjectName, session.ObjectName)

	case "index":
		return fmt.Sprintf(`-- Delete index: %s

-- Uncomment to delete:
-- DROP INDEX %s;
`, session.ObjectName, session.ObjectName)

	case "schema":
		return fmt.Sprintf(`-- Delete schema: %s
-- WARNING: This will delete the schema and potentially all objects within it.

-- Uncomment to delete (fails if schema contains objects):
-- DROP SCHEMA %s;

-- Or with CASCADE to delete schema and all contained objects:
-- DROP SCHEMA %s CASCADE;
`, session.ObjectName, session.ObjectName, session.ObjectName)

	case "view":
		return fmt.Sprintf(`-- Delete view: %s

-- Uncomment to delete:
-- DROP VIEW %s;

-- Or with CASCADE to also drop dependent views:
-- DROP VIEW %s CASCADE;
`, session.ObjectName, session.ObjectName, session.ObjectName)

	default:
		return fmt.Sprintf("-- Delete %s: %s\n", session.ObjectType, session.ObjectName)
	}
}

// IsEmptyOrCommented checks if content is empty or contains only SQL comments.
//
// Returns true if the content has no executable SQL statements.
// Handles:
//   - Empty strings
//   - Whitespace-only strings
//   - Single-line comments (-- comment)
//   - Block comments (/* comment */)
func IsEmptyOrCommented(content string) bool {
	if content == "" {
		return true
	}

	// Remove block comments /* ... */
	blockCommentRe := regexp.MustCompile(`/\*[\s\S]*?\*/`)
	cleaned := blockCommentRe.ReplaceAllString(content, "")

	// Remove single-line comments -- ...
	lineCommentRe := regexp.MustCompile(`--.*$`)
	lines := strings.Split(cleaned, "\n")
	var nonCommentLines []string
	for _, line := range lines {
		// Remove line comment from this line
		line = lineCommentRe.ReplaceAllString(line, "")
		line = strings.TrimSpace(line)
		if line != "" {
			nonCommentLines = append(nonCommentLines, line)
		}
	}

	return len(nonCommentLines) == 0
}

// ExtractSQL extracts executable SQL from content, removing comments.
//
// Returns the SQL with comments stripped and whitespace trimmed.
func ExtractSQL(content string) string {
	// Remove block comments /* ... */
	blockCommentRe := regexp.MustCompile(`/\*[\s\S]*?\*/`)
	cleaned := blockCommentRe.ReplaceAllString(content, "")

	// Remove single-line comments -- ... but preserve newlines
	lineCommentRe := regexp.MustCompile(`--.*$`)
	lines := strings.Split(cleaned, "\n")
	var resultLines []string
	for _, line := range lines {
		// Remove line comment from this line
		line = lineCommentRe.ReplaceAllString(line, "")
		line = strings.TrimRight(line, " \t")
		resultLines = append(resultLines, line)
	}

	result := strings.Join(resultLines, "\n")
	return strings.TrimSpace(result)
}
