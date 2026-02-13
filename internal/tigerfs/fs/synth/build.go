package synth

import "fmt"

// GenerateMarkdownTableSQL returns the CREATE TABLE statement for a markdown app.
// The table name is prefixed with underscore (backing table convention).
//
// Schema:
//   - id: UUID primary key with auto-generation
//   - filename: unique text for .md file naming
//   - title, author: text frontmatter columns
//   - headers: JSONB for user-defined frontmatter key-value pairs
//   - body: text for markdown content
//   - created_at: timestamptz with auto-default
//   - modified_at: timestamptz with auto-default
func GenerateMarkdownTableSQL(schema, name string) string {
	return fmt.Sprintf(`CREATE TABLE %s.%s (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    title TEXT,
    author TEXT,
    headers JSONB DEFAULT '{}'::jsonb,
    body TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
)`, quoteIdent(schema), quoteIdent("_"+name))
}

// GeneratePlainTextTableSQL returns the CREATE TABLE statement for a plain text app.
// The table name is prefixed with underscore (backing table convention).
//
// Schema:
//   - id: UUID primary key with auto-generation
//   - filename: unique text for .txt file naming
//   - body: text for file content
//   - created_at: timestamptz with auto-default
//   - modified_at: timestamptz with auto-default
func GeneratePlainTextTableSQL(schema, name string) string {
	return fmt.Sprintf(`CREATE TABLE %s.%s (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    body TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(filename, filetype)
)`, quoteIdent(schema), quoteIdent("_"+name))
}

// GenerateViewSQL returns a CREATE VIEW statement that selects all columns
// from the backing table. The view name is the user-facing app name;
// the backing table is prefixed with underscore.
func GenerateViewSQL(schema, viewName, tableName string) string {
	return fmt.Sprintf(`CREATE VIEW %s.%s AS SELECT * FROM %s.%s`,
		quoteIdent(schema), quoteIdent(viewName),
		quoteIdent(schema), quoteIdent(tableName))
}

// GenerateViewCommentSQL returns a COMMENT ON VIEW statement that sets the
// TigerFS format marker (e.g., "tigerfs:md").
func GenerateViewCommentSQL(schema, viewName string, format SynthFormat) string {
	comment := FormatComment(format)
	return fmt.Sprintf(`COMMENT ON VIEW %s.%s IS '%s'`,
		quoteIdent(schema), quoteIdent(viewName), comment)
}

// GenerateModifiedAtTriggerSQL returns two SQL statements to create a trigger function
// and trigger that auto-updates the modified_at column on UPDATE.
// Returns two separate statements because the function body contains semicolons
// inside $$ dollar-quoting, which prevents simple delimiter-based splitting.
func GenerateModifiedAtTriggerSQL(schema, tableName string) []string {
	// Build the full function and trigger names, then quote as single identifiers.
	// Cannot embed quoteIdent() inside a name — "set_"_posts"_modified_at" is invalid SQL.
	funcName := fmt.Sprintf("%s.%s", quoteIdent(schema), quoteIdent("set_"+tableName+"_modified_at"))
	triggerName := quoteIdent("trg_" + tableName + "_modified_at")

	createFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql`, funcName)

	createTrigger := fmt.Sprintf(`CREATE TRIGGER %s
    BEFORE UPDATE ON %s.%s
    FOR EACH ROW EXECUTE FUNCTION %s()`,
		triggerName,
		quoteIdent(schema), quoteIdent(tableName),
		funcName)

	return []string{createFunc, createTrigger}
}

// GenerateBuildSQL returns the complete SQL statements to create a synthesized app.
// This includes the backing table, view, view comment, and modified_at trigger.
// Returns a slice of individual statements (not delimited) because the trigger
// function uses dollar-quoting which contains semicolons.
func GenerateBuildSQL(schema, appName string, format SynthFormat) ([]string, error) {
	tableName := "_" + appName

	var tableSQL string
	switch format {
	case FormatMarkdown:
		tableSQL = GenerateMarkdownTableSQL(schema, appName)
	case FormatPlainText:
		tableSQL = GeneratePlainTextTableSQL(schema, appName)
	default:
		return nil, fmt.Errorf("unsupported format for .build: %s", format.String())
	}

	viewSQL := GenerateViewSQL(schema, appName, tableName)
	commentSQL := GenerateViewCommentSQL(schema, appName, format)
	triggerStmts := GenerateModifiedAtTriggerSQL(schema, tableName)

	stmts := []string{tableSQL, viewSQL, commentSQL}
	stmts = append(stmts, triggerStmts...)
	return stmts, nil
}

// quoteIdent quotes a PostgreSQL identifier to prevent SQL injection.
// Uses double-quote escaping per PostgreSQL standard.
func quoteIdent(name string) string {
	// Simple quoting: double any existing double-quotes and wrap in quotes
	escaped := ""
	for _, r := range name {
		if r == '"' {
			escaped += `""`
		} else {
			escaped += string(r)
		}
	}
	return `"` + escaped + `"`
}
