package synth

import (
	"strings"
	"testing"
)

func TestGenerateMarkdownTableSQL(t *testing.T) {
	sql := GenerateMarkdownTableSQL("public", "posts")

	if !strings.Contains(sql, `"public"."_posts"`) {
		t.Errorf("should reference _posts table, got:\n%s", sql)
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

	if !strings.Contains(sql, `"public"."_snippets"`) {
		t.Errorf("should reference _snippets table, got:\n%s", sql)
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
	sql := GenerateViewSQL("public", "posts", "_posts")

	if !strings.Contains(sql, "CREATE VIEW") {
		t.Errorf("should be CREATE VIEW, got:\n%s", sql)
	}
	if !strings.Contains(sql, `"public"."posts"`) {
		t.Errorf("should reference posts view, got:\n%s", sql)
	}
	if !strings.Contains(sql, `"public"."_posts"`) {
		t.Errorf("should SELECT FROM _posts, got:\n%s", sql)
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
	stmts := GenerateModifiedAtTriggerSQL("public", "_posts")
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
	// Function and trigger names should be properly quoted as single identifiers
	if !strings.Contains(allSQL, `"set__posts_modified_at"`) {
		t.Errorf("function name should be a single quoted identifier, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, `"trg__posts_modified_at"`) {
		t.Errorf("trigger name should be a single quoted identifier, got:\n%s", allSQL)
	}
}

func TestGenerateBuildSQL_Markdown(t *testing.T) {
	stmts, err := GenerateBuildSQL("public", "posts", FormatMarkdown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	allSQL := strings.Join(stmts, "\n")

	// Should contain all four parts
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
	// Should have 5 statements: table, view, comment, function, trigger
	if len(stmts) != 5 {
		t.Errorf("expected 5 statements, got %d", len(stmts))
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

func TestSynth_GenerateHistorySQL(t *testing.T) {
	stmts := GenerateHistorySQL("public", "memory")
	allSQL := strings.Join(stmts, "\n")

	// History table
	if !strings.Contains(allSQL, `"public"."_memory_history"`) {
		t.Errorf("should reference _memory_history table, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "_history_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY") {
		t.Errorf("should have _history_id column, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "_operation TEXT NOT NULL") {
		t.Errorf("should have _operation column, got:\n%s", allSQL)
	}

	// Indexes
	if !strings.Contains(allSQL, "idx__memory_history_by_filename") {
		t.Errorf("should create filename index, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "idx__memory_history_by_id") {
		t.Errorf("should create id index, got:\n%s", allSQL)
	}

	// Encoding column in history table
	if !strings.Contains(allSQL, "encoding TEXT,") {
		t.Errorf("history table should contain encoding column, got:\n%s", allSQL)
	}

	// Trigger
	if !strings.Contains(allSQL, "BEFORE UPDATE OR DELETE") {
		t.Errorf("should create BEFORE UPDATE OR DELETE trigger, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "TG_OP::text") {
		t.Errorf("should record TG_OP as _operation, got:\n%s", allSQL)
	}
	// History trigger should copy encoding column
	if !strings.Contains(allSQL, "OLD.encoding") {
		t.Errorf("history trigger should copy encoding column, got:\n%s", allSQL)
	}

	// Hypertable
	if !strings.Contains(allSQL, "create_hypertable") {
		t.Errorf("should create hypertable, got:\n%s", allSQL)
	}

	// Compression
	if !strings.Contains(allSQL, "timescaledb.compress") {
		t.Errorf("should enable compression, got:\n%s", allSQL)
	}
	if !strings.Contains(allSQL, "add_compression_policy") {
		t.Errorf("should add compression policy, got:\n%s", allSQL)
	}

	// Should be 8 statements: table, 2 indexes, func, trigger, hypertable, compression, policy
	if len(stmts) != 8 {
		t.Errorf("expected 8 statements, got %d", len(stmts))
	}
}

func TestSynth_GenerateBuildSQLWithFeatures_History(t *testing.T) {
	features := FeatureSet{Format: FormatMarkdown, History: true}
	stmts, err := GenerateBuildSQLWithFeatures("public", "memory", features)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	allSQL := strings.Join(stmts, "\n")

	// Should have base (5) + history (8) = 13 statements
	if len(stmts) != 13 {
		t.Errorf("expected 13 statements, got %d", len(stmts))
	}

	// View comment should include history
	if !strings.Contains(allSQL, "tigerfs:md,history") {
		t.Errorf("comment should include history flag, got:\n%s", allSQL)
	}

	// History table should exist
	if !strings.Contains(allSQL, "_memory_history") {
		t.Errorf("should create history table, got:\n%s", allSQL)
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

	// Should have 1 (comment) + 8 (history) = 9 statements
	if len(stmts) != 9 {
		t.Errorf("expected 9 statements, got %d", len(stmts))
	}

	// History infrastructure
	if !strings.Contains(allSQL, "_memory_history") {
		t.Errorf("should create history table, got:\n%s", allSQL)
	}
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"public", `"public"`},
		{"my_table", `"my_table"`},
		{`has"quote`, `"has""quote"`},
	}

	for _, tt := range tests {
		got := quoteIdent(tt.input)
		if got != tt.expected {
			t.Errorf("quoteIdent(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
