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
	if !strings.Contains(sql, `parent_id UUID REFERENCES "tigerfs"."posts"(id) DEFERRABLE INITIALLY IMMEDIATE`) {
		t.Errorf("should have parent_id FK with DEFERRABLE, got:\n%s", sql)
	}
	if !strings.Contains(sql, "filename TEXT NOT NULL") {
		t.Errorf("should have filename column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "filetype TEXT NOT NULL DEFAULT 'file'") {
		t.Errorf("should have filetype column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE") {
		t.Errorf("should have UNIQUE NULLS NOT DISTINCT constraint with DEFERRABLE, got:\n%s", sql)
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
	if !strings.Contains(sql, `parent_id UUID REFERENCES "tigerfs"."snippets"(id) DEFERRABLE INITIALLY IMMEDIATE`) {
		t.Errorf("should have parent_id FK with DEFERRABLE, got:\n%s", sql)
	}
	if !strings.Contains(sql, "filename TEXT NOT NULL") {
		t.Errorf("should have filename column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "filetype TEXT NOT NULL DEFAULT 'file'") {
		t.Errorf("should have filetype column, got:\n%s", sql)
	}
	if !strings.Contains(sql, "UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE") {
		t.Errorf("should have UNIQUE NULLS NOT DISTINCT constraint with DEFERRABLE, got:\n%s", sql)
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

func TestGenerateParentDirMtimeTriggerSQL(t *testing.T) {
	stmts := GenerateParentDirMtimeTriggerSQL("public", "posts")
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(stmts))
	}

	funcSQL := stmts[0]
	triggerSQL := stmts[1]

	// Function
	if !strings.Contains(funcSQL, "CREATE OR REPLACE FUNCTION") {
		t.Errorf("should create function, got:\n%s", funcSQL)
	}
	if !strings.Contains(funcSQL, `"tigerfs"."bump_posts_parent_mtime"`) {
		t.Errorf("function should be in tigerfs schema with correct name, got:\n%s", funcSQL)
	}
	if !strings.Contains(funcSQL, "RETURNS TRIGGER") {
		t.Errorf("should return trigger, got:\n%s", funcSQL)
	}
	if !strings.Contains(funcSQL, "RETURN NULL") {
		t.Errorf("AFTER trigger must return NULL, got:\n%s", funcSQL)
	}
	// Should handle all three operations
	if !strings.Contains(funcSQL, "TG_OP = 'INSERT'") {
		t.Errorf("should handle INSERT, got:\n%s", funcSQL)
	}
	if !strings.Contains(funcSQL, "TG_OP = 'DELETE'") {
		t.Errorf("should handle DELETE, got:\n%s", funcSQL)
	}
	if !strings.Contains(funcSQL, "TG_OP = 'UPDATE'") {
		t.Errorf("should handle UPDATE, got:\n%s", funcSQL)
	}
	// Should reference parent_id and filetype
	if !strings.Contains(funcSQL, "NEW.parent_id") {
		t.Errorf("should reference NEW.parent_id, got:\n%s", funcSQL)
	}
	if !strings.Contains(funcSQL, "OLD.parent_id") {
		t.Errorf("should reference OLD.parent_id, got:\n%s", funcSQL)
	}
	if !strings.Contains(funcSQL, "filetype = 'directory'") {
		t.Errorf("should guard against non-directory parents, got:\n%s", funcSQL)
	}
	// Should use IS DISTINCT FROM for NULL-safe comparison
	if !strings.Contains(funcSQL, "IS DISTINCT FROM") {
		t.Errorf("should use IS DISTINCT FROM for NULL-safe comparison, got:\n%s", funcSQL)
	}

	// Trigger
	if !strings.Contains(triggerSQL, "CREATE TRIGGER") {
		t.Errorf("should create trigger, got:\n%s", triggerSQL)
	}
	if !strings.Contains(triggerSQL, `"trg_posts_parent_mtime"`) {
		t.Errorf("trigger name should use correct pattern, got:\n%s", triggerSQL)
	}
	if !strings.Contains(triggerSQL, "AFTER INSERT OR DELETE OR UPDATE OF parent_id, filename") {
		t.Errorf("should be AFTER trigger with column filter, got:\n%s", triggerSQL)
	}
	if !strings.Contains(triggerSQL, "FOR EACH ROW") {
		t.Errorf("should be row-level trigger, got:\n%s", triggerSQL)
	}
	if !strings.Contains(triggerSQL, `"tigerfs"."posts"`) {
		t.Errorf("trigger should reference tigerfs.posts, got:\n%s", triggerSQL)
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
	// Second statement should create resolve_path function
	if !strings.Contains(stmts[1], "resolve_path") {
		t.Errorf("second statement should create resolve_path function, got: %s", stmts[1])
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
	if !strings.Contains(allSQL, `"public"."posts" WITH (security_invoker = true) AS SELECT * FROM "tigerfs"."posts"`) {
		t.Errorf("view should be in public schema referencing tigerfs with security_invoker, got:\n%s", allSQL)
	}
	// Should have 10 statements: schema, resolve_path, table, parent_index, view, comment,
	// modified_at function + trigger, parent_mtime function + trigger
	if len(stmts) != 10 {
		t.Errorf("expected 10 statements, got %d", len(stmts))
	}

	// Parent index for ReadDir and path resolution
	if !strings.Contains(allSQL, `"idx_posts_parent"`) {
		t.Errorf("should create parent index, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "(parent_id, filename)") {
		t.Errorf("parent index should be on (parent_id, filename), got:\n%s", allSQL)
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

	// ADR-017 column renames: id->file_id, _history_id->version_id, _operation->operation
	if !strings.Contains(allSQL, "file_id UUID,") {
		t.Errorf("history table should have file_id column (renamed from id), got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "parent_id UUID,") {
		t.Errorf("history table should have parent_id column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "version_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY") {
		t.Errorf("should have version_id column (renamed from _history_id), got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "operation TEXT NOT NULL") {
		t.Errorf("should have operation column (renamed from _operation), got:\n%s", allSQL)
	}

	// CHECK constraints on filetype, encoding, operation
	if !strings.Contains(allSQL, "filetype TEXT CHECK (filetype IN ('file', 'directory'))") {
		t.Errorf("history should have filetype CHECK constraint, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "encoding TEXT CHECK (encoding IN ('utf8', 'base64'))") {
		t.Errorf("history should have encoding CHECK constraint, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "CHECK (operation IN ('create', 'edit', 'rename', 'delete'))") {
		t.Errorf("history should have operation CHECK constraint including 'create' tombstone, got:\n%s", allSQL)
	}

	// History table uses modern CREATE TABLE WITH syntax for hypertable + columnstore
	if !strings.Contains(allSQL, "tsdb.partition_column = 'version_id'") {
		t.Errorf("history hypertable should partition by version_id, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "tsdb.segmentby = 'file_id'") {
		t.Errorf("history columnstore should segment by file_id, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "tsdb.orderby = 'version_id DESC'") {
		t.Errorf("history columnstore should order by version_id DESC, got:\n%s", allSQL)
	}

	// Indexes with clean names (using new column names)
	if !strings.Contains(allSQL, `"idx_memory_history_by_filename"`) {
		t.Errorf("should create filename index with clean name, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "(filename, version_id DESC)") {
		t.Errorf("filename index should use version_id, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `"idx_memory_history_by_id"`) {
		t.Errorf("should create id index with clean name, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "(file_id, version_id DESC)") {
		t.Errorf("id index should use file_id and version_id, got:\n%s", allSQL)
	}

	// Encoding column in history table
	if !strings.Contains(allSQL, "encoding TEXT CHECK") {
		t.Errorf("history table should contain encoding column with CHECK, got:\n%s", allSQL)
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

	// Trigger copies parent_id and uses new column names. After tombstone, the
	// trigger fires on INSERT too, capturing operation='create'.
	if !strings.Contains(allSQL, "BEFORE INSERT OR UPDATE OR DELETE") {
		t.Errorf("should create BEFORE INSERT OR UPDATE OR DELETE trigger (tombstone), got:\n%s", allSQL)
	}
	// Confirm the INSERT branch is wired in the trigger function.
	if !strings.Contains(allSQL, "IF TG_OP = 'INSERT' THEN") {
		t.Errorf("trigger function should branch on INSERT for tombstone capture, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "'create'") {
		t.Errorf("trigger function should label INSERT history rows as 'create', got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "NEW.id, NEW.parent_id, NEW.filename") {
		t.Errorf("trigger function INSERT branch should reference NEW.*, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `ON "tigerfs"."memory"`) {
		t.Errorf("trigger should be on tigerfs.memory, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.parent_id") {
		t.Errorf("trigger should copy OLD.parent_id, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "version_id, operation") {
		t.Errorf("trigger INSERT should use version_id and operation columns, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "WHEN 'DELETE' THEN 'delete'") {
		t.Errorf("trigger should map TG_OP to filesystem-centric types, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "THEN 'rename'") {
		t.Errorf("trigger should detect rename from filename/parent_id changes, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.encoding") {
		t.Errorf("history trigger should copy encoding column, got:\n%s", allSQL)
	}

	// Archive function in tigerfs schema
	if !strings.Contains(allSQL, `"tigerfs"."archive_memory_history"`) {
		t.Errorf("archive function should be in tigerfs schema, got:\n%s", allSQL)
	}

	// Should be 12 statements: history (table with WITH, 2 indexes, func, trigger)
	// + log (table, 3 indexes: by_file, by_user, by_type) + savepoint (table) + metadata (table, index)
	if len(stmts) != 12 {
		t.Errorf("expected 12 statements, got %d", len(stmts))
	}

	// --- Log table ---
	if !strings.Contains(allSQL, `"tigerfs"."memory_log"`) {
		t.Errorf("should create log table in tigerfs schema, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "log_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY") {
		t.Errorf("log table should have log_id UUIDv7 PK, got:\n%s", allSQL)
	}
	// Log table should have version_id (renamed from history_id)
	if !strings.Contains(allSQL, "version_id UUID") {
		t.Errorf("log table should have version_id column, got:\n%s", allSQL)
	}
	// Filesystem-centric type names (ADR-017)
	if !strings.Contains(allSQL, "CHECK (type IN ('create', 'edit', 'rename', 'delete', 'undo'))") {
		t.Errorf("log table should have filesystem-centric type CHECK constraint, got:\n%s", allSQL)
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
	if !strings.Contains(allSQL, `"idx_memory_log_by_user" ON "tigerfs"."memory_log" (user_id, log_id ASC)`) {
		t.Errorf("log table should have (user_id, log_id) index backing .log/.by/user_id/, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `"idx_memory_log_by_type" ON "tigerfs"."memory_log" (type, log_id ASC)`) {
		t.Errorf("log table should have (type, log_id) index backing .log/.by/type/, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "tsdb.orderby = 'log_id ASC'") {
		t.Errorf("log columnstore should order by log_id ASC, got:\n%s", allSQL)
	}

	// --- Savepoint table ---
	if !strings.Contains(allSQL, `"tigerfs"."memory_savepoint"`) {
		t.Errorf("should create savepoint table in tigerfs schema, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "name TEXT NOT NULL PRIMARY KEY") {
		t.Errorf("savepoint table should have name as PK, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "savepoint_id UUID NOT NULL DEFAULT uuidv7() UNIQUE") {
		t.Errorf("savepoint table should have savepoint_id UUIDv7 UNIQUE, got:\n%s", allSQL)
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

	// Should still have base columns (OLD.id maps to file_id in history)
	if !strings.Contains(allSQL, "OLD.id") {
		t.Errorf("trigger should reference OLD.id, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.parent_id") {
		t.Errorf("trigger should reference OLD.parent_id, got:\n%s", allSQL)
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

	// Should be 12 statements (same as markdown -- log/savepoint/metadata are format-independent)
	if len(stmts) != 12 {
		t.Errorf("expected 12 statements, got %d", len(stmts))
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

	// Should have base (10: schema+resolve_path+table+parent_idx+view+comment+func+trigger+parent_mtime_func+parent_mtime_trigger)
	// + history (12: table+2idx+func+trigger+log+3logidx+savepoint+metadata+metadataidx) = 22 statements
	if len(stmts) != 22 {
		t.Errorf("expected 22 statements, got %d", len(stmts))
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

	// History trigger should copy encoding and parent_id columns
	if !strings.Contains(allSQL, "OLD.encoding") {
		t.Errorf("history trigger should copy encoding column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "OLD.parent_id") {
		t.Errorf("history trigger should copy parent_id column, got:\n%s", allSQL)
	}
	// History table should have encoding column with CHECK
	if !strings.Contains(allSQL, "encoding TEXT CHECK") {
		t.Errorf("history table should contain encoding column with CHECK, got:\n%s", allSQL)
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

	// Should have 1 (comment) + 12 (history + log + 3 logidx + savepoint + metadata) = 13 statements
	if len(stmts) != 13 {
		t.Errorf("expected 13 statements, got %d", len(stmts))
	}

	// History infrastructure in tigerfs schema
	if !strings.Contains(allSQL, `"tigerfs"."memory_history"`) {
		t.Errorf("should create history table in tigerfs schema, got:\n%s", allSQL)
	}
}

func TestSynth_GenerateResolvePathSQL(t *testing.T) {
	sql := GenerateResolvePathSQL()

	// Function should be in tigerfs schema
	if !strings.Contains(sql, `"tigerfs".resolve_path`) {
		t.Errorf("should create function in tigerfs schema, got:\n%s", sql)
	}
	// Should accept REGCLASS, UUID, TEXT[] parameters
	if !strings.Contains(sql, "tbl REGCLASS") {
		t.Errorf("should accept REGCLASS parameter, got:\n%s", sql)
	}
	if !strings.Contains(sql, "start_parent UUID") {
		t.Errorf("should accept start_parent UUID parameter, got:\n%s", sql)
	}
	if !strings.Contains(sql, "segments TEXT[]") {
		t.Errorf("should accept segments TEXT[] parameter, got:\n%s", sql)
	}
	// Should return table with expected columns
	if !strings.Contains(sql, "RETURNS TABLE(depth INT, resolved_id UUID, resolved_name TEXT)") {
		t.Errorf("should return (depth, resolved_id, resolved_name), got:\n%s", sql)
	}
	// Should use EXECUTE with format() for dynamic table (REGCLASS)
	if !strings.Contains(sql, "EXECUTE format(") {
		t.Errorf("should use EXECUTE format() for dynamic table, got:\n%s", sql)
	}
	// Should use IS NOT DISTINCT FROM for NULL-safe parent_id comparison
	if !strings.Contains(sql, "IS NOT DISTINCT FROM") {
		t.Errorf("should use IS NOT DISTINCT FROM for NULL-safe comparison, got:\n%s", sql)
	}
	// Should be PL/pgSQL
	if !strings.Contains(sql, "LANGUAGE plpgsql") {
		t.Errorf("should be PL/pgSQL function, got:\n%s", sql)
	}
	// Should be CREATE OR REPLACE (idempotent)
	if !strings.Contains(sql, "CREATE OR REPLACE FUNCTION") {
		t.Errorf("should use CREATE OR REPLACE, got:\n%s", sql)
	}
}

func TestSynth_GenerateParentIndexSQL(t *testing.T) {
	sql := GenerateParentIndexSQL("memory")

	if !strings.Contains(sql, `"idx_memory_parent"`) {
		t.Errorf("index name should be idx_memory_parent, got:\n%s", sql)
	}
	if !strings.Contains(sql, `"tigerfs"."memory"`) {
		t.Errorf("should reference tigerfs.memory table, got:\n%s", sql)
	}
	if !strings.Contains(sql, "(parent_id, filename)") {
		t.Errorf("should index (parent_id, filename), got:\n%s", sql)
	}
	if !strings.Contains(sql, "CREATE INDEX") {
		t.Errorf("should be CREATE INDEX, got:\n%s", sql)
	}
}

func TestSynth_GenerateParentIndexSQL_SpecialName(t *testing.T) {
	sql := GenerateParentIndexSQL("my-app")

	// Should properly quote identifiers with special characters
	if !strings.Contains(sql, `"idx_my-app_parent"`) {
		t.Errorf("should quote index name with special chars, got:\n%s", sql)
	}
	if !strings.Contains(sql, `"tigerfs"."my-app"`) {
		t.Errorf("should quote table name with special chars, got:\n%s", sql)
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
