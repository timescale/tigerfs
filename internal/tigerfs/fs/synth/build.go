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
	qualifiedTable := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(name))
	return fmt.Sprintf(`CREATE TABLE %s (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    parent_id UUID REFERENCES %s(id) DEFERRABLE INITIALLY IMMEDIATE,
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    title TEXT,
    author TEXT,
    headers JSONB DEFAULT '{}'::jsonb,
    body TEXT,
    encoding TEXT NOT NULL DEFAULT 'utf8' CHECK (encoding IN ('utf8', 'base64')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE
)`, qualifiedTable, qualifiedTable)
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
	qualifiedTable := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(name))
	return fmt.Sprintf(`CREATE TABLE %s (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    parent_id UUID REFERENCES %s(id) DEFERRABLE INITIALLY IMMEDIATE,
    filename TEXT NOT NULL,
    filetype TEXT NOT NULL DEFAULT 'file' CHECK (filetype IN ('file', 'directory')),
    body TEXT,
    encoding TEXT NOT NULL DEFAULT 'utf8' CHECK (encoding IN ('utf8', 'base64')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (parent_id, filename, filetype) DEFERRABLE INITIALLY IMMEDIATE
)`, qualifiedTable, qualifiedTable)
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

// GenerateParentDirMtimeTriggerSQL returns two SQL statements to create a trigger
// function and trigger that updates the parent directory's modified_at when
// children are added, removed, or moved. This gives directories POSIX-correct
// mtime semantics: mtime changes on child create/delete/rename, not on content edits.
//
// The trigger is AFTER (side-effect on a different row) and uses column-level
// filtering (UPDATE OF parent_id, filename) so content-only edits never fire it.
// No recursion risk: the UPDATE to the parent only changes modified_at, which is
// not in the column filter list.
func GenerateParentDirMtimeTriggerSQL(schema, tableName string) []string {
	funcName := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent("bump_"+tableName+"_parent_mtime"))
	triggerName := db.QuoteIdent("trg_" + tableName + "_parent_mtime")
	qualifiedTable := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(tableName))

	createFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.parent_id IS NOT NULL THEN
            UPDATE %s SET modified_at = now()
            WHERE id = NEW.parent_id AND filetype = 'directory';
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD.parent_id IS NOT NULL THEN
            UPDATE %s SET modified_at = now()
            WHERE id = OLD.parent_id AND filetype = 'directory';
        END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.parent_id IS DISTINCT FROM NEW.parent_id
           OR OLD.filename IS DISTINCT FROM NEW.filename THEN
            IF OLD.parent_id IS NOT NULL
               AND OLD.parent_id IS DISTINCT FROM NEW.parent_id THEN
                UPDATE %s SET modified_at = now()
                WHERE id = OLD.parent_id AND filetype = 'directory';
            END IF;
            IF NEW.parent_id IS NOT NULL THEN
                UPDATE %s SET modified_at = now()
                WHERE id = NEW.parent_id AND filetype = 'directory';
            END IF;
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql`, funcName, qualifiedTable, qualifiedTable, qualifiedTable, qualifiedTable)

	createTrigger := fmt.Sprintf(`CREATE TRIGGER %s
    AFTER INSERT OR DELETE OR UPDATE OF parent_id, filename ON %s
    FOR EACH ROW EXECUTE FUNCTION %s()`,
		triggerName, qualifiedTable, funcName)

	return []string{createFunc, createTrigger}
}

// GenerateBuildSQL returns the complete SQL statements to create a synthesized app.
// This includes the backing table, view, view comment, and modified_at trigger.
// Returns a slice of individual statements (not delimited) because the trigger
// function uses dollar-quoting which contains semicolons.
func GenerateBuildSQL(schema, appName string, format SynthFormat) ([]string, error) {
	return GenerateBuildSQLWithFeatures(schema, appName, FeatureSet{Format: format})
}

// GenerateResolvePathSQL returns a CREATE OR REPLACE FUNCTION statement for the
// tigerfs.resolve_path PL/pgSQL function. This function resolves a sequence of
// path segments to row IDs using the parent-pointer model (ADR-017).
//
// Parameters:
//   - tbl: REGCLASS reference to the source table
//   - start_parent: UUID of the starting parent (NULL for root)
//   - segments: TEXT[] array of path segments to resolve
//
// Returns a table of (depth, resolved_id, resolved_name) rows, one per resolved
// segment. The Go layer populates its path cache from these results. If any segment
// doesn't resolve, the function returns fewer rows than segments.
func GenerateResolvePathSQL() string {
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s.resolve_path(
    tbl REGCLASS,
    start_parent UUID,
    segments TEXT[]
)
RETURNS TABLE(depth INT, resolved_id UUID, resolved_name TEXT) AS $$
DECLARE
    current_id UUID := start_parent;
    i INT := 0;
    seg TEXT;
BEGIN
    FOREACH seg IN ARRAY segments LOOP
        i := i + 1;
        EXECUTE format('SELECT id FROM %%s WHERE filename = $1 AND parent_id IS NOT DISTINCT FROM $2', tbl)
        INTO current_id
        USING seg, current_id;
        IF current_id IS NULL THEN RETURN; END IF;
        depth := i;
        resolved_id := current_id;
        resolved_name := seg;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql`, db.QuoteIdent(TigerFSSchema))
}

// GenerateParentIndexSQL returns a CREATE INDEX statement for the parent_id + filename
// index. This index supports ReadDir (WHERE parent_id = X) and path resolution lookups.
func GenerateParentIndexSQL(appName string) string {
	qualifiedTable := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(appName))
	return fmt.Sprintf(`CREATE INDEX %s ON %s (parent_id, filename)`,
		db.QuoteIdent("idx_"+appName+"_parent"),
		qualifiedTable,
	)
}

// GenerateBuildSQLWithFeatures returns SQL statements to create a synthesized app
// with optional features like versioned history. The first statements create the
// tigerfs schema and the resolve_path function. The backing table, triggers, and
// functions are in the tigerfs schema; the view is in the user's schema. When
// features.History is true, appends history hypertable, trigger, log, and savepoint.
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
	resolvePathFunc := GenerateResolvePathSQL()
	parentIndex := GenerateParentIndexSQL(appName)
	viewSQL := GenerateViewSQL(schema, appName, TigerFSSchema, appName)
	commentSQL := GenerateViewCommentSQLWithFeatures(schema, appName, features)
	triggerStmts := GenerateModifiedAtTriggerSQL(schema, appName)

	parentMtimeStmts := GenerateParentDirMtimeTriggerSQL(schema, appName)

	stmts := []string{createSchema, resolvePathFunc, tableSQL, parentIndex, viewSQL, commentSQL}
	stmts = append(stmts, triggerStmts...)
	stmts = append(stmts, parentMtimeStmts...)

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
//   - History hypertable with columnstore (file_id, parent_id, version_id, operation)
//   - Index on (filename, version_id DESC) for by-filename queries
//   - Index on (file_id, version_id DESC) for by-UUID queries
//   - Archive trigger function and BEFORE UPDATE OR DELETE trigger
//   - Log hypertable for undo operations (create/edit/rename/delete/undo)
//   - Savepoint table for named bookmarks
//
// The column list varies by format: markdown includes title, author, headers;
// plain text only has the base columns (file_id, filename, filetype, body, etc.).
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

	// History table uses modern CREATE TABLE WITH syntax for hypertable + columnstore.
	// This replaces the old create_hypertable() + ALTER TABLE SET + add_compression_policy() calls.
	// Column renames from ADR-017: id->file_id, _history_id->version_id, _operation->operation.
	// Added: parent_id, CHECK constraints on filetype/encoding/operation.
	// `operation` values: 'create' captures the BEFORE-INSERT state (tombstone --
	// makes undo-of-undo for DELETE operations work, since the original delete's
	// undo INSERT now has a fresh history row of its own); 'edit'/'rename'/'delete'
	// capture the BEFORE-UPDATE/DELETE state. No 'undo' value: undo is realized as
	// one of these four physical ops, not a distinct kind of history row.
	createTable := fmt.Sprintf(`CREATE TABLE %s (
    file_id UUID,
    parent_id UUID,
    filename TEXT NOT NULL,
    filetype TEXT CHECK (filetype IN ('file', 'directory')),%s
    body TEXT,
    encoding TEXT CHECK (encoding IN ('utf8', 'base64')),
    created_at TIMESTAMPTZ,
    modified_at TIMESTAMPTZ,
    version_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    operation TEXT NOT NULL CHECK (operation IN ('create', 'edit', 'rename', 'delete'))
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'version_id',
    tsdb.chunk_interval = '7 days',
    tsdb.segmentby = 'file_id',
    tsdb.orderby = 'version_id DESC'
)`, qualifiedHistory, formatColumns)

	createIndexFilename := fmt.Sprintf(
		`CREATE INDEX %s ON %s (filename, version_id DESC)`,
		db.QuoteIdent("idx_"+historyTable+"_by_filename"),
		qualifiedHistory,
	)

	createIndexID := fmt.Sprintf(
		`CREATE INDEX %s ON %s (file_id, version_id DESC)`,
		db.QuoteIdent("idx_"+historyTable+"_by_id"),
		qualifiedHistory,
	)

	// Archive trigger function -- captures every row state change in the history
	// table. The INSERT branch writes the NEW row with operation='create'
	// (tombstone), so undo-of-undo for DELETE operations finds a fresh, self-
	// describing history row to dispatch on. The UPDATE/DELETE branch writes the
	// OLD row with operation='rename'/'edit'/'delete'. Column lists must match the
	// source table's columns.
	funcName := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent("archive_"+historyTable))

	// Markdown format adds title/author/headers columns; plain text does not.
	var formatNewValues string
	if format == FormatMarkdown {
		formatNewValues = "NEW.title, NEW.author, NEW.headers, "
	}

	var insertColumns, oldInsertValues, newInsertValues string
	if format == FormatMarkdown {
		insertColumns = "file_id, parent_id, filename, filetype, title, author, headers, body, encoding, created_at, modified_at"
		oldInsertValues = fmt.Sprintf("OLD.id, OLD.parent_id, OLD.filename, OLD.filetype, %sOLD.body,\n         OLD.encoding, OLD.created_at, OLD.modified_at", formatOldValues)
		newInsertValues = fmt.Sprintf("NEW.id, NEW.parent_id, NEW.filename, NEW.filetype, %sNEW.body,\n         NEW.encoding, NEW.created_at, NEW.modified_at", formatNewValues)
	} else {
		insertColumns = "file_id, parent_id, filename, filetype, body, encoding, created_at, modified_at"
		oldInsertValues = "OLD.id, OLD.parent_id, OLD.filename, OLD.filetype, OLD.body,\n         OLD.encoding, OLD.created_at, OLD.modified_at"
		newInsertValues = "NEW.id, NEW.parent_id, NEW.filename, NEW.filetype, NEW.body,\n         NEW.encoding, NEW.created_at, NEW.modified_at"
	}

	createFunc := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO %s
            (%s,
             version_id, operation)
        VALUES
            (%s,
             uuidv7(),
             'create');
        RETURN NEW;
    END IF;
    INSERT INTO %s
        (%s,
         version_id, operation)
    VALUES
        (%s,
         uuidv7(),
         CASE TG_OP
             WHEN 'DELETE' THEN 'delete'
             WHEN 'UPDATE' THEN
                 CASE WHEN OLD.filename != NEW.filename
                           OR OLD.parent_id IS DISTINCT FROM NEW.parent_id
                      THEN 'rename'
                      ELSE 'edit'
                 END
         END);
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql`, funcName, qualifiedHistory, insertColumns, newInsertValues, qualifiedHistory, insertColumns, oldInsertValues)

	triggerName := db.QuoteIdent("trg_" + historyTable + "_archive")
	createTrigger := fmt.Sprintf(`CREATE TRIGGER %s
    BEFORE INSERT OR UPDATE OR DELETE ON %s
    FOR EACH ROW EXECUTE FUNCTION %s()`,
		triggerName, qualifiedTable, funcName)

	// --- Operation log table (ADR-016 Section 1, ADR-017 updates) ---
	// Records every data change for undo operations. Uses UUIDv7 PKs for
	// time-ordered entries and SkipScan-optimized queries.
	// Column renames: history_id->version_id. Type values are filesystem-centric:
	// create/edit/rename/delete/undo (replaces insert/update/delete/undo).
	logTable := appName + "_log"
	qualifiedLog := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(logTable))

	// Log table with hypertable + columnstore settings inline via CREATE TABLE WITH.
	// TimescaleDB automatically creates a columnstore policy using the chunk interval
	// as the compression interval (no explicit compress_after needed).
	createLogTable := fmt.Sprintf(`CREATE TABLE %s (
    log_id UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    file_id UUID NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('create', 'edit', 'rename', 'delete', 'undo')),
    user_id TEXT,
    filename TEXT NOT NULL,
    version_id UUID,
    description TEXT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'log_id',
    tsdb.chunk_interval = '7 days',
    tsdb.segmentby = 'file_id',
    tsdb.orderby = 'log_id ASC'
)`, qualifiedLog)

	// Composite index for SkipScan on the undo DISTINCT ON query
	createLogIndex := fmt.Sprintf(
		`CREATE INDEX %s ON %s (file_id, log_id ASC)`,
		db.QuoteIdent("idx_"+logTable+"_by_file"),
		qualifiedLog,
	)

	// Composite indexes backing the documented .log/.by/user_id/ and .log/.by/type/
	// workflows (recipes.md Recipe 3, files.md Operation Log). Same (col, log_id ASC)
	// shape as createLogIndex so SkipScan + chunk pruning + segmentby-on-file_id
	// stay coherent.
	createLogIndexUser := fmt.Sprintf(
		`CREATE INDEX %s ON %s (user_id, log_id ASC)`,
		db.QuoteIdent("idx_"+logTable+"_by_user"),
		qualifiedLog,
	)
	createLogIndexType := fmt.Sprintf(
		`CREATE INDEX %s ON %s (type, log_id ASC)`,
		db.QuoteIdent("idx_"+logTable+"_by_type"),
		qualifiedLog,
	)

	// --- Savepoint table (ADR-016 Section 2) ---
	// Named bookmarks for undo-to-savepoint operations. Regular table (not a
	// hypertable) -- savepoints are small and don't need time-series features.
	savepointTable := appName + "_savepoint"
	qualifiedSavepoint := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(savepointTable))

	createSavepointTable := fmt.Sprintf(`CREATE TABLE %s (
    name TEXT NOT NULL PRIMARY KEY,
    savepoint_id UUID NOT NULL DEFAULT uuidv7() UNIQUE,
    user_id TEXT,
    description TEXT
)`, qualifiedSavepoint)

	// --- Metadata table ---
	// Per-app metadata about the workspace itself (format migrations, future
	// system markers). Distinct from operational log entries. The undo engine
	// consults this table to refuse undo across format boundaries.
	// Regular table (not a hypertable) -- O(10) rows per database lifetime.
	metadataTable := appName + MetadataTableSuffix
	qualifiedMetadata := fmt.Sprintf("%s.%s", db.QuoteIdent(TigerFSSchema), db.QuoteIdent(metadataTable))

	createMetadataTable := fmt.Sprintf(`CREATE TABLE %s (
    entry_id    UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    subject     TEXT NOT NULL,
    user_id     TEXT,
    description TEXT,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb
)`, qualifiedMetadata)

	createMetadataIndex := fmt.Sprintf(
		`CREATE INDEX %s ON %s (subject, entry_id)`,
		db.QuoteIdent("idx_"+metadataTable+"_subject"),
		qualifiedMetadata,
	)

	return []string{
		// History infrastructure (table is hypertable via WITH clause)
		createTable,
		createIndexFilename,
		createIndexID,
		createFunc,
		createTrigger,
		// Undo log infrastructure
		createLogTable,
		createLogIndex,
		createLogIndexUser,
		createLogIndexType,
		// Savepoint infrastructure
		createSavepointTable,
		// Metadata infrastructure
		createMetadataTable,
		createMetadataIndex,
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
