package synth

import (
	"strings"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func TestGenerateMarkdownTableSQL(t *testing.T) {
	sql := GenerateMarkdownTableSQL("public", "posts")

	if !strings.Contains(sql, `"tigerfs"."posts"`) {
		t.Errorf("should reference tigerfs.posts table, got:\n%s", sql)
	}
	if !strings.Contains(sql, "id UUID PRIMARY KEY") {
		t.Errorf("should have UUID primary key, got:\n%s", sql)
	}
	if !strings.Contains(sql, "filename TEXT NOT NULL") {
		t.Errorf("should have filename column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "filetype TEXT NOT NULL DEFAULT 'file'") {
		t.Errorf("should have filetype column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "UNIQUE(filename, filetype)") {
		t.Errorf("should have compound UNIQUE constraint, got:\n%s", sql)
	}
	if !strings.Contains(sql, "title TEXT") {
		t.Errorf("should have title column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "author TEXT") {
		t.Errorf("should have author column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "headers JSONB") {
		t.Errorf("should have headers JSONB column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "body TEXT") {
		t.Errorf("should have body column, got:\n%s", sql)
	}
	if !strings.Contains(sql, `encoding TEXT NOT NULL DEFAULT 'utf8'`) {
		t.Errorf("should contain encoding column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "created_at TIMESTAMPTZ") {
		t.Errorf("should have created_at column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "modified_at TIMESTAMPTZ") {
		t.Errorf("should have modified_at column, got:\n%s", sql)
	}
}

func TestGeneratePlainTextTableSQL(t *testing.T) {
	sql := GeneratePlainTextTableSQL("public", "snippets")

	if !strings.Contains(sql, `"tigerfs"."snippets"`) {
		t.Errorf("should reference tigerfs.snippets table, got:\n%s", sql)
	}
	if !strings.Contains(sql, "filename TEXT NOT NULL") {
		t.Errorf("should have filename column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "filetype TEXT NOT NULL DEFAULT 'file'") {
		t.Errorf("should have filetype column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "UNIQUE(filename, filetype)") {
		t.Errorf("should have compound UNIQUE constraint, got:\n%s", sql)
	}
	if !strings.Contains(sql, "body TEXT") {
		t.Errorf("should have body column, got:\n%s", sql)
	}
	if !strings.Contains(sql, `encoding TEXT NOT NULL DEFAULT 'utf8'`) {
		t.Errorf("should contain encoding column, got:\n%s", sql)
	}
	// Plain text should NOT have title/author
	if strings.Contains(sql, "title TEXT") {
		t.Errorf("plain text should not have title column, got:\n%s", sql)
	}
	if strings.Contains(sql, "author TEXT") {
		t.Errorf("plain text should not have author column, got:\n%s", sql)
	}
}

func TestGenerateViewSQL(t *testing.T) {
	sql := GenerateViewSQL("public", "posts", TigerFSSchema, "posts")

	if !strings.Contains(sql, "CREATE VIEW") {
		t.Errorf("should be CREATE VIEW, got:\n%s", sql)
	}
	if !strings.Contains(sql, `"public"."posts"`) {
		t.Errorf("should reference posts view in user schema, got:\n%s", sql)
	}
	if !strings.Contains(sql, `"tigerfs"."posts"`) {
		t.Errorf("should SELECT FROM tigerfs.posts, got:\n%s", sql)
	}
}

func TestGenerateViewCommentSQL(t *testing.T) {
	sql := GenerateViewCommentSQL("public", "posts", FormatMarkdown)

	if !strings.Contains(sql, "COMMENT ON VIEW") {
		t.Errorf("should be COMMENT ON VIEW, got:\n%s", sql)
	}
	if !strings.Contains(sql, "tigerfs:md") {
		t.Errorf("should contain tigerfs:md marker, got:\n%s", sql)
	}

	sql = GenerateViewCommentSQL("myschema", "notes", FormatPlainText)
	if !strings.Contains(sql, "tigerfs:txt") {
		t.Errorf("should contain tigerfs:txt marker, got:\n%s", sql)
	}
}

func TestGenerateModifiedAtTriggerSQL(t *testing.T) {
	stmts := GenerateModifiedAtTriggerSQL("public", "posts")
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(stmts))
	}

	allSQL := strings.Join(stmts, "\n")

	if !strings.Contains(allSQL, "CREATE OR REPLACE FUNCTION") {
		t.Errorf("should create function, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "CREATE TRIGGER") {
		t.Errorf("should create trigger, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "BEFORE UPDATE") {
		t.Errorf("should trigger BEFORE UPDATE, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "NEW.modified_at = now()") {
		t.Errorf("should set modified_at to now(), got:\n%s", allSQL)
	}
	// Function should be in tigerfs schema with clean name (no underscore)
	if !strings.Contains(allSQL, `"tigerfs"."set_posts_modified_at"`) {
		t.Errorf("function should be in tigerfs schema, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `"trg_posts_modified_at"`) {
		t.Errorf("trigger name should use clean name, got:\n%s", allSQL)
	}
	// Trigger should reference tigerfs schema table
	if !strings.Contains(allSQL, `"tigerfs"."posts"`) {
		t.Errorf("trigger should reference tigerfs.posts, got:\n%s", allSQL)
	}
}

func TestGenerateBuildSQL_Markdown(t *testing.T) {
	stmts, err := GenerateBuildSQL("public", "posts", FormatMarkdown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	allSQL := strings.Join(stmts, "\n")

	// First statement should create the tigerfs schema
	if !strings.Contains(stmts[0], `CREATE SCHEMA IF NOT EXISTS "tigerfs"`) {
		t.Errorf("first statement should create tigerfs schema, got: %s", stmts[0])
	}
	// Should contain all parts
	if !strings.Contains(allSQL, "CREATE TABLE") {
		t.Errorf("should contain CREATE TABLE, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "CREATE VIEW") {
		t.Errorf("should contain CREATE VIEW, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "COMMENT ON VIEW") {
		t.Errorf("should contain COMMENT ON VIEW, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "CREATE TRIGGER") {
		t.Errorf("should contain CREATE TRIGGER, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `encoding TEXT NOT NULL DEFAULT 'utf8'`) {
		t.Errorf("should contain encoding column, got:\n%s", allSQL)
	}
	// Table in tigerfs schema, view in user schema
	if !strings.Contains(allSQL, `"tigerfs"."posts"`) {
		t.Errorf("table should be in tigerfs schema, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `"public"."posts" AS SELECT * FROM "tigerfs"."posts"`) {
		t.Errorf("view should be in public schema referencing tigerfs, got:\n%s", allSQL)
	}
	// Should have 6 statements: schema, table, view, comment, function, trigger
	if len(stmts) != 6 {
		t.Errorf("expected 6 statements, got %d", len(stmts))
	}

	// Non-history apps should NOT have log or savepoint tables
	if strings.Contains(allSQL, "_log") {
		t.Errorf("non-history app should not have log table, got:\n%s", allSQL)
	}
	if strings.Contains(allSQL, "_savepoint") {
		t.Errorf("non-history app should not have savepoint table, got:\n%s", allSQL)
	}
}

func TestGenerateBuildSQL_PlainText(t *testing.T) {
	stmts, err := GenerateBuildSQL("public", "notes", FormatPlainText)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	allSQL := strings.Join(stmts, "\n")
	if !strings.Contains(allSQL, "tigerfs:txt") {
		t.Errorf("should use tigerfs:txt comment, got:\n%s", allSQL)
	}
}

func TestGenerateBuildSQL_UnsupportedFormat(t *testing.T) {
	_, err := GenerateBuildSQL("public", "test", FormatNative)
	if err == nil {
		t.Fatal("expected error for native format")
	}

	_, err = GenerateBuildSQL("public", "test", FormatTasks)
	if err == nil {
		t.Fatal("expected error for tasks format")
	}
}

func TestSynth_GenerateHistorySQL_Markdown(t *testing.T) {
	stmts := GenerateHistorySQL("public", "memory", FormatMarkdown)
	allSQL := strings.Join(stmts, "\n")

	// History table in tigerfs schema with clean name
	if !strings.Contains(allSQL, `"tigerfs"."memory_history"`) {
		t.Errorf("should reference tigerfs.memory_history table, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "_history_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY") {
		t.Errorf("should have _history_id column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "_operation TEXT NOT NULL") {
		t.Errorf("should have _operation column, got:\n%s", allSQL)
	}

	// Indexes with clean names
	if !strings.Contains(allSQL, `"idx_memory_history_by_filename"`) {
		t.Errorf("should create filename index with clean name, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `"idx_memory_history_by_id"`) {
		t.Errorf("should create id index with clean name, got:\n%s", allSQL)
	}

	// Encoding column in history table
	if !strings.Contains(allSQL, "encoding TEXT,") {
		t.Errorf("history table should contain encoding column, got:\n%s", allSQL)
	}

	// Markdown-specific columns in history table and trigger
	if !strings.Contains(allSQL, "title TEXT,") {
		t.Errorf("markdown history should have title column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "author TEXT,") {
		t.Errorf("markdown history should have author column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "headers JSONB,") {
		t.Errorf("markdown history should have headers column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.title") {
		t.Errorf("markdown trigger should copy title, got:\n%s", allSQL)
	}

	// Trigger on tigerfs schema table
	if !strings.Contains(allSQL, "BEFORE UPDATE OR DELETE") {
		t.Errorf("should create BEFORE UPDATE OR DELETE trigger, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `ON "tigerfs"."memory"`) {
		t.Errorf("trigger should be on tigerfs.memory, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "TG_OP::text") {
		t.Errorf("should record TG_OP as _operation, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.encoding") {
		t.Errorf("history trigger should copy encoding column, got:\n%s", allSQL)
	}

	// Archive function in tigerfs schema
	if !strings.Contains(allSQL, `"tigerfs"."archive_memory_history"`) {
		t.Errorf("archive function should be in tigerfs schema, got:\n%s", allSQL)
	}

	// Hypertable uses tigerfs schema
	if !strings.Contains(allSQL, "create_hypertable('tigerfs.memory_history'") {
		t.Errorf("hypertable should use tigerfs schema, got:\n%s", allSQL)
	}

	// Compression
	if !strings.Contains(allSQL, "timescaledb.compress") {
		t.Errorf("should enable compression, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "add_compression_policy('tigerfs.memory_history'") {
		t.Errorf("compression policy should use tigerfs schema, got:\n%s", allSQL)
	}

	// Should be 13 statements: history (table, 2 indexes, func, trigger, hypertable,
	// compression, policy) + log (table, hypertable, index, compression) + savepoint (table)
	if len(stmts) != 11 {
		t.Errorf("expected 11 statements, got %d", len(stmts))
	}

	// --- Log table ---
	if !strings.Contains(allSQL, `"tigerfs"."memory_log"`) {
		t.Errorf("should create log table in tigerfs schema, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "log_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY") {
		t.Errorf("log table should have log_id UUIDv7 PK, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "file_id UUID NOT NULL") {
		t.Errorf("log table should have file_id column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "filename TEXT NOT NULL") {
		t.Errorf("log table should have filename column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "history_id UUID") {
		t.Errorf("log table should have history_id column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "CHECK (type IN ('insert', 'update', 'delete', 'undo'))") {
		t.Errorf("log table should have type CHECK constraint, got:\n%s", allSQL)
	}
	// Log table uses modern CREATE TABLE WITH syntax for hypertable + columnstore
	if !strings.Contains(allSQL, "tsdb.hypertable") {
		t.Errorf("log table should be a hypertable via CREATE TABLE WITH, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "tsdb.chunk_interval = '7 days'") {
		t.Errorf("log hypertable should have 7-day chunk interval, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `"idx_memory_log_by_file"`) {
		t.Errorf("log table should have (file_id, log_id) index, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "tsdb.segmentby = 'file_id'") {
		t.Errorf("log columnstore should segment by file_id, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "tsdb.orderby = 'log_id ASC'") {
		t.Errorf("log columnstore should order by log_id ASC, got:\n%s", allSQL)
	}

	// --- Savepoint table ---
	if !strings.Contains(allSQL, `"tigerfs"."memory_savepoint"`) {
		t.Errorf("should create savepoint table in tigerfs schema, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "savepoint_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY") {
		t.Errorf("savepoint table should have savepoint_id UUIDv7 PK, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "name TEXT NOT NULL UNIQUE") {
		t.Errorf("savepoint table should have unique name column, got:\n%s", allSQL)
	}
}

func TestSynth_GenerateHistorySQL_PlainText(t *testing.T) {
	stmts := GenerateHistorySQL("public", "snippets", FormatPlainText)
	allSQL := strings.Join(stmts, "\n")

	// History table should exist
	if !strings.Contains(allSQL, `"tigerfs"."snippets_history"`) {
		t.Errorf("should reference tigerfs.snippets_history table, got:\n%s", allSQL)
	}

	// Plain text history should NOT have title/author/headers columns
	if strings.Contains(allSQL, "title TEXT") {
		t.Errorf("plain text history should not have title column, got:\n%s", allSQL)
	}
	if strings.Contains(allSQL, "author TEXT") {
		t.Errorf("plain text history should not have author column, got:\n%s", allSQL)
	}
	if strings.Contains(allSQL, "headers JSONB") {
		t.Errorf("plain text history should not have headers column, got:\n%s", allSQL)
	}

	// Trigger should NOT reference OLD.title/OLD.author/OLD.headers
	if strings.Contains(allSQL, "OLD.title") {
		t.Errorf("plain text trigger should not reference OLD.title, got:\n%s", allSQL)
	}
	if strings.Contains(allSQL, "OLD.author") {
		t.Errorf("plain text trigger should not reference OLD.author, got:\n%s", allSQL)
	}
	if strings.Contains(allSQL, "OLD.headers") {
		t.Errorf("plain text trigger should not reference OLD.headers, got:\n%s", allSQL)
	}

	// Should still have base columns
	if !strings.Contains(allSQL, "OLD.id") {
		t.Errorf("trigger should reference OLD.id, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.filename") {
		t.Errorf("trigger should reference OLD.filename, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.body") {
		t.Errorf("trigger should reference OLD.body, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.encoding") {
		t.Errorf("trigger should reference OLD.encoding, got:\n%s", allSQL)
	}

	// Should be 13 statements (same as markdown -- log/savepoint are format-independent)
	if len(stmts) != 11 {
		t.Errorf("expected 11 statements, got %d", len(stmts))
	}

	// Log and savepoint tables should exist for plain text too
	if !strings.Contains(allSQL, `"tigerfs"."snippets_log"`) {
		t.Errorf("should create log table for plain text app, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `"tigerfs"."snippets_savepoint"`) {
		t.Errorf("should create savepoint table for plain text app, got:\n%s", allSQL)
	}
}

func TestSynth_GenerateBuildSQLWithFeatures_History(t *testing.T) {
	features := FeatureSet{Format: FormatMarkdown, History: true}
	stmts, err := GenerateBuildSQLWithFeatures("public", "memory", features)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	allSQL := strings.Join(stmts, "\n")

	// Should have base (6: schema+table+view+comment+func+trigger) + history (11) = 17 statements
	if len(stmts) != 17 {
		t.Errorf("expected 17 statements, got %d", len(stmts))
	}

	// First statement should create tigerfs schema
	if !strings.Contains(stmts[0], "CREATE SCHEMA IF NOT EXISTS") {
		t.Errorf("first statement should create schema, got: %s", stmts[0])
	}

	// View comment should include history
	if !strings.Contains(allSQL, "tigerfs:md,history") {
		t.Errorf("comment should include history flag, got:\n%s", allSQL)
	}

	// History table in tigerfs schema with clean name
	if !strings.Contains(allSQL, `"tigerfs"."memory_history"`) {
		t.Errorf("should create history table in tigerfs schema, got:\n%s", allSQL)
	}

	// History trigger should copy encoding column
	if !strings.Contains(allSQL, "OLD.encoding") {
		t.Errorf("history trigger should copy encoding column, got:\n%s", allSQL)
	}
	// History table should have encoding column
	if !strings.Contains(allSQL, "encoding TEXT,") {
		t.Errorf("history table should contain encoding column, got:\n%s", allSQL)
	}
}

func TestSynth_GenerateHistoryOnlySQL(t *testing.T) {
	existing := FeatureSet{Format: FormatMarkdown}
	stmts := GenerateHistoryOnlySQL("public", "memory", existing)
	allSQL := strings.Join(stmts, "\n")

	// Should start with comment update
	if !strings.Contains(stmts[0], "COMMENT ON VIEW") {
		t.Errorf("first statement should update comment, got: %s", stmts[0])
	}
	if !strings.Contains(stmts[0], "tigerfs:md,history") {
		t.Errorf("comment should include history, got: %s", stmts[0])
	}

	// Should have 1 (comment) + 11 (history + log + savepoint) = 12 statements
	if len(stmts) != 12 {
		t.Errorf("expected 12 statements, got %d", len(stmts))
	}

	// History infrastructure in tigerfs schema
	if !strings.Contains(allSQL, `"tigerfs"."memory_history"`) {
		t.Errorf("should create history table in tigerfs schema, got:\n%s", allSQL)
	}
}

func TestSynth_QuoteIdent(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"public", `"public"`},
		{"my_table", `"my_table"`},
		{`has"quote`, `"has""quote"`},
	}

	for _, tt := range tests {
		got := db.QuoteIdent(tt.input)
		if got != tt.expected {
			t.Errorf("db.QuoteIdent(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
