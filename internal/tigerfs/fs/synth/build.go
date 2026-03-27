package synth

import (
	"fmt"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TigerFSSchema is the dedicated PostgreSQL schema for TigerFS backing tables,
// triggers, functions, and history tables.
const TigerFSSchema = "tigerfs"

// GenerateMarkdownTableSQL returns the CREATE TABLE statement for a markdown app.
// The backing table is created in the tigerfs schema with the clean app name
// (no underscore prefix).
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
)`, db.QuoteIdent(TigerFSSchema), db.QuoteIdent(name))
}

// GeneratePlainTextTableSQL returns the CREATE TABLE statement for a plain text app.
// The backing table is created in the tigerfs schema with the clean app name
// (no underscore prefix).
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
)`, db.QuoteIdent(TigerFSSchema), db.QuoteIdent(name))
}

// GenerateViewSQL returns a CREATE VIEW statement that selects all columns
// from the backing table. The view is created in viewSchema, and references
// the table in tableSchema. For build apps, the view lives in the user's
// schema while the table lives in the tigerfs schema. For synthesized views
// on existing tables, both schemas may be the same.
func GenerateViewSQL(viewSchema, viewName, tableSchema, tableName string) string {
	return fmt.Sprintf(`CREATE VIEW %s.%s AS SELECT * FROM %s.%s`,
		db.QuoteIdent(viewSchema), db.QuoteIdent(viewName),
		db.QuoteIdent(tableSchema), db.QuoteIdent(tableName))
}

// GenerateViewCommentSQL returns a COMMENT ON VIEW statement that sets the
// TigerFS format marker (e.g., "tigerfs:md").
func GenerateViewCommentSQL(schema, viewName string, format SynthFormat) string {
	comment := FormatComment(format)
	return fmt.Sprintf(`COMMENT ON VIEW %s.%s IS '%s'`,
		db.QuoteIdent(schema), db.QuoteIdent(viewName), comment)
}

// GenerateViewCommentSQLWithFeatures returns a COMMENT ON VIEW statement
// encoding the full feature set (e.g., "tigerfs:md,history").
func GenerateViewCommentSQLWithFeatures(schema, viewName string, features FeatureSet) string {
	comment := FeatureComment(features)
	return fmt.Sprintf(`COMMENT ON VIEW %s.%s IS '%s'`,
		db.QuoteIdent(schema), db.QuoteIdent(viewName), comment)
}

// GenerateModifiedAtTriggerSQL returns two SQL statements to create a trigger function
// and trigger that auto-updates the modified_at column on UPDATE.
// The function and trigger are created in the tigerfs schema, operating on the
// backing table (also in tigerfs schema). The tableName should be the clean
// app name without underscore prefix.
// Returns two separate statements because the function body contains semicolons
// inside $$ dollar-quoting, which prevents simple delimiter-based splitting.
func GenerateModifiedAtTriggerSQL(schema, tableName string) []string {
	// Build the full function and trigger names, then quote as single identifiers.
	// Cannot embed QuoteIdent() inside a name — "set_"posts"_modified_at" is invalid SQL.
	funcName := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent("set_"+tableName+"_modified_at"))
	triggerName := db.QuoteIdent("trg_" + tableName + "_modified_at")

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
		db.QuoteIdent(TigerFSSchema), db.QuoteIdent(tableName),
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
// with optional features like versioned history. The first statement creates the
// tigerfs schema. The backing table, triggers, and functions are in the tigerfs
// schema; the view is in the user's schema. When features.History is true,
// appends history table, trigger, hypertable, and compression statements.
func GenerateBuildSQLWithFeatures(schema, appName string, features FeatureSet) ([]string, error) {
	var tableSQL string
	switch features.Format {
	case FormatMarkdown:
		tableSQL = GenerateMarkdownTableSQL(schema, appName)
	case FormatPlainText:
		tableSQL = GeneratePlainTextTableSQL(schema, appName)
	default:
		return nil, fmt.Errorf("unsupported format for .build: %s", features.Format.String())
	}

	createSchema := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, db.QuoteIdent(TigerFSSchema))
	viewSQL := GenerateViewSQL(schema, appName, TigerFSSchema, appName)
	commentSQL := GenerateViewCommentSQLWithFeatures(schema, appName, features)
	triggerStmts := GenerateModifiedAtTriggerSQL(schema, appName)

	stmts := []string{createSchema, tableSQL, viewSQL, commentSQL}
	stmts = append(stmts, triggerStmts...)

	if features.History {
		historyStmts := GenerateHistorySQL(schema, appName, features.Format)
		stmts = append(stmts, historyStmts...)
	}

	return stmts, nil
}

// GenerateHistorySQL returns SQL statements to create versioned history
// infrastructure for an existing synth app. All history infrastructure
// (table, indexes, functions, triggers) lives in the tigerfs schema.
// This includes:
//   - History table mirroring the source table columns plus metadata
//   - Index on (filename, _history_id DESC) for by-filename queries
//   - Index on (id, _history_id DESC) for by-UUID queries
//   - Archive trigger function and BEFORE UPDATE OR DELETE trigger
//   - TimescaleDB hypertable conversion
//   - Compression policy
//
// The column list varies by format: markdown includes title, author, headers;
// plain text only has the base columns (id, filename, filetype, body, etc.).
// The archive trigger must match the source table's actual columns.
func GenerateHistorySQL(schema, appName string, format SynthFormat) []string {
	tableName := appName
	historyTable := appName + "_history"
	qualifiedTable := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(tableName))
	qualifiedHistory := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(historyTable))

	// Build column lists based on format. Markdown has title/author/headers;
	// plain text does not. The trigger must only reference columns that exist
	// on the source table, otherwise PostgreSQL errors with "record OLD has no field".
	var formatColumns string
	var formatOldValues string
	if format == FormatMarkdown {
		formatColumns = "\n    title TEXT,\n    author TEXT,\n    headers JSONB,"
		formatOldValues = "OLD.title, OLD.author, OLD.headers, "
	}

	createTable := fmt.Sprintf(`CREATE TABLE %s (
    id UUID,
    filename TEXT NOT NULL,
    filetype TEXT,%s
    body TEXT,
    encoding TEXT,
    created_at TIMESTAMPTZ,
    modified_at TIMESTAMPTZ,
    _history_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    _operation TEXT NOT NULL
)`, qualifiedHistory, formatColumns)

	createIndexFilename := fmt.Sprintf(
		`CREATE INDEX %s ON %s (filename, _history_id DESC)`,
		db.QuoteIdent("idx_"+historyTable+"_by_filename"),
		qualifiedHistory,
	)

	createIndexID := fmt.Sprintf(
		`CREATE INDEX %s ON %s (id, _history_id DESC)`,
		db.QuoteIdent("idx_"+historyTable+"_by_id"),
		qualifiedHistory,
	)

	// Archive trigger function — copies OLD row to history table on UPDATE or DELETE.
	// Column list must match the source table's actual columns.
	funcName := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent("archive_"+historyTable))

	var insertColumns, insertValues string
	if format == FormatMarkdown {
		insertColumns = "id, filename, filetype, title, author, headers, body, encoding, created_at, modified_at"
		insertValues = fmt.Sprintf("OLD.id, OLD.filename, OLD.filetype, %sOLD.body,\n         OLD.encoding, OLD.created_at, OLD.modified_at", formatOldValues)
	} else {
		insertColumns = "id, filename, filetype, body, encoding, created_at, modified_at"
		insertValues = "OLD.id, OLD.filename, OLD.filetype, OLD.body,\n         OLD.encoding, OLD.created_at, OLD.modified_at"
	}

	createFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO %s
        (%s,
         _history_id, _operation)
    VALUES
        (%s,
         uuidv7(), TG_OP::text);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql`, funcName, qualifiedHistory, insertColumns, insertValues)

	triggerName := db.QuoteIdent("trg_" + historyTable + "_archive")
	createTrigger := fmt.Sprintf(`CREATE TRIGGER %s
    BEFORE UPDATE OR DELETE ON %s
    FOR EACH ROW EXECUTE FUNCTION %s()`,
		triggerName, qualifiedTable, funcName)

	// TimescaleDB hypertable conversion — UUIDv7 as partition column
	createHypertable := fmt.Sprintf(
		`SELECT create_hypertable('%s.%s', '_history_id', chunk_time_interval => INTERVAL '1 month')`,
		TigerFSSchema, historyTable,
	)

	// Compression policy — segment by filename, order by _history_id DESC
	setCompression := fmt.Sprintf(
		`ALTER TABLE %s SET (timescaledb.compress, timescaledb.compress_segmentby = 'filename', timescaledb.compress_orderby = '_history_id DESC')`,
		qualifiedHistory,
	)

	addCompressionPolicy := fmt.Sprintf(
		`SELECT add_compression_policy('%s.%s', compress_after => INTERVAL '1 day')`,
		TigerFSSchema, historyTable,
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

	historyStmts := GenerateHistorySQL(schema, appName, existingFeatures.Format)
	stmts := []string{commentSQL}
	stmts = append(stmts, historyStmts...)
	return stmts
}
