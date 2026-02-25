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
//   - encoding: text indicating body encoding ('utf8' or 'base64')
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
    encoding TEXT NOT NULL DEFAULT 'utf8' CHECK (encoding IN ('utf8', 'base64')),
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
//   - encoding: text indicating body encoding ('utf8' or 'base64')
//   - created_at: timestamptz with auto-default
//   - modified_at: timestamptz with auto-default
func GeneratePlainTextTableSQL(schema, name string) string {
	return fmt.Sprintf(`CREATE TABLE %s.%s (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    body TEXT,
    encoding TEXT NOT NULL DEFAULT 'utf8' CHECK (encoding IN ('utf8', 'base64')),
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

// GenerateViewCommentSQLWithFeatures returns a COMMENT ON VIEW statement
// encoding the full feature set (e.g., "tigerfs:md,history").
func GenerateViewCommentSQLWithFeatures(schema, viewName string, features FeatureSet) string {
	comment := FeatureComment(features)
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
	return GenerateBuildSQLWithFeatures(schema, appName, FeatureSet{Format: format})
}

// GenerateBuildSQLWithFeatures returns SQL statements to create a synthesized app
// with optional features like versioned history. When features.History is true,
// appends history table, trigger, hypertable, and compression statements.
func GenerateBuildSQLWithFeatures(schema, appName string, features FeatureSet) ([]string, error) {
	tableName := "_" + appName

	var tableSQL string
	switch features.Format {
	case FormatMarkdown:
		tableSQL = GenerateMarkdownTableSQL(schema, appName)
	case FormatPlainText:
		tableSQL = GeneratePlainTextTableSQL(schema, appName)
	default:
		return nil, fmt.Errorf("unsupported format for .build: %s", features.Format.String())
	}

	viewSQL := GenerateViewSQL(schema, appName, tableName)
	commentSQL := GenerateViewCommentSQLWithFeatures(schema, appName, features)
	triggerStmts := GenerateModifiedAtTriggerSQL(schema, tableName)

	stmts := []string{tableSQL, viewSQL, commentSQL}
	stmts = append(stmts, triggerStmts...)

	if features.History {
		historyStmts := GenerateHistorySQL(schema, appName)
		stmts = append(stmts, historyStmts...)
	}

	return stmts, nil
}

// GenerateHistorySQL returns SQL statements to create versioned history
// infrastructure for an existing synth app. This includes:
//   - History table mirroring the source table columns plus metadata
//   - Index on (filename, _history_id DESC) for by-filename queries
//   - Index on (id, _history_id DESC) for by-UUID queries
//   - Archive trigger function and BEFORE UPDATE OR DELETE trigger
//   - TimescaleDB hypertable conversion
//   - Compression policy
//
// The column list is based on the markdown table schema (the most common case).
// For apps with different schemas, the trigger dynamically copies OLD.* values.
func GenerateHistorySQL(schema, appName string) []string {
	tableName := "_" + appName
	historyTable := "_" + appName + "_history"
	qualifiedTable := fmt.Sprintf("%s.%s", quoteIdent(schema), quoteIdent(tableName))
	qualifiedHistory := fmt.Sprintf("%s.%s", quoteIdent(schema), quoteIdent(historyTable))

	// The columns mirror the markdown table schema. Using the same columns
	// for all synth formats works because plain text tables are a subset
	// (they have id, filename, filetype, body, created_at, modified_at).
	// Extra columns (title, author, headers) will be NULL for plain text apps.
	createTable := fmt.Sprintf(`CREATE TABLE %s (
    id UUID,
    filename TEXT NOT NULL,
    filetype TEXT,
    title TEXT,
    author TEXT,
    headers JSONB,
    body TEXT,
    encoding TEXT,
    created_at TIMESTAMPTZ,
    modified_at TIMESTAMPTZ,
    _history_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    _operation TEXT NOT NULL
)`, qualifiedHistory)

	createIndexFilename := fmt.Sprintf(
		`CREATE INDEX %s ON %s (filename, _history_id DESC)`,
		quoteIdent("idx_"+historyTable+"_by_filename"),
		qualifiedHistory,
	)

	createIndexID := fmt.Sprintf(
		`CREATE INDEX %s ON %s (id, _history_id DESC)`,
		quoteIdent("idx_"+historyTable+"_by_id"),
		qualifiedHistory,
	)

	// Archive trigger function — copies OLD row to history table on UPDATE or DELETE
	funcName := fmt.Sprintf("%s.%s", quoteIdent(schema), quoteIdent("archive_"+historyTable))
	createFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO %s
        (id, filename, filetype, title, author, headers, body, encoding, created_at, modified_at,
         _history_id, _operation)
    VALUES
        (OLD.id, OLD.filename, OLD.filetype, OLD.title, OLD.author, OLD.headers, OLD.body,
         OLD.encoding, OLD.created_at, OLD.modified_at,
         uuidv7(), TG_OP::text);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql`, funcName, qualifiedHistory)

	triggerName := quoteIdent("trg_" + historyTable + "_archive")
	createTrigger := fmt.Sprintf(`CREATE TRIGGER %s
    BEFORE UPDATE OR DELETE ON %s
    FOR EACH ROW EXECUTE FUNCTION %s()`,
		triggerName, qualifiedTable, funcName)

	// TimescaleDB hypertable conversion — UUIDv7 as partition column
	createHypertable := fmt.Sprintf(
		`SELECT create_hypertable('%s.%s', '_history_id', chunk_time_interval => INTERVAL '1 month')`,
		schema, historyTable,
	)

	// Compression policy — segment by filename, order by _history_id DESC
	setCompression := fmt.Sprintf(
		`ALTER TABLE %s SET (timescaledb.compress, timescaledb.compress_segmentby = 'filename', timescaledb.compress_orderby = '_history_id DESC')`,
		qualifiedHistory,
	)

	addCompressionPolicy := fmt.Sprintf(
		`SELECT add_compression_policy('%s.%s', compress_after => INTERVAL '1 day')`,
		schema, historyTable,
	)

	return []string{
		createTable,
		createIndexFilename,
		createIndexID,
		createFunc,
		createTrigger,
		createHypertable,
		setCompression,
		addCompressionPolicy,
	}
}

// GenerateHistoryOnlySQL returns SQL statements to add versioned history to an
// existing synth app. This creates the history table and trigger, and updates
// the view comment to include the history flag.
func GenerateHistoryOnlySQL(schema, appName string, existingFeatures FeatureSet) []string {
	// Update the view comment to include history
	updatedFeatures := existingFeatures
	updatedFeatures.History = true
	commentSQL := GenerateViewCommentSQLWithFeatures(schema, appName, updatedFeatures)

	historyStmts := GenerateHistorySQL(schema, appName)
	stmts := []string{commentSQL}
	stmts = append(stmts, historyStmts...)
	return stmts
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
