package fuse

import (
	"context"
	"fmt"
	"strings"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// TemplateGenerator generates DDL templates for staging operations.
// Templates provide context-aware SQL stubs that users can edit before committing.
type TemplateGenerator interface {
	// Generate returns the DDL template content.
	Generate(ctx context.Context) (string, error)
}

// =============================================================================
// Table Templates
// =============================================================================

// CreateTableTemplate generates a template for creating a new table.
type CreateTableTemplate struct {
	// Schema is the PostgreSQL schema where the table will be created
	Schema string
	// TableName is the name for the new table
	TableName string
}

// Generate returns a CREATE TABLE template with common patterns.
func (t *CreateTableTemplate) Generate(ctx context.Context) (string, error) {
	return fmt.Sprintf(`-- Create table: %s.%s
-- Modify this template and save, then touch .commit to execute

CREATE TABLE "%s"."%s" (
    -- Primary key (choose one pattern):
    -- id SERIAL PRIMARY KEY,
    -- id BIGSERIAL PRIMARY KEY,
    -- id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Add columns here:
    -- name TEXT NOT NULL,
    -- email TEXT UNIQUE,
    -- created_at TIMESTAMPTZ DEFAULT now(),
    -- updated_at TIMESTAMPTZ DEFAULT now()
);
`, t.Schema, t.TableName, t.Schema, t.TableName), nil
}

// ModifyTableTemplate generates a template for modifying an existing table.
// Fetches the current table DDL to include as context in the template.
type ModifyTableTemplate struct {
	// Schema is the PostgreSQL schema containing the table
	Schema string
	// TableName is the name of the table to modify
	TableName string
	// DB provides access to fetch current table DDL
	DB db.DDLReader
}

// Generate returns an ALTER TABLE template with the current schema as context.
func (t *ModifyTableTemplate) Generate(ctx context.Context) (string, error) {
	var sb strings.Builder

	// Fetch current DDL for context
	currentDDL, err := t.DB.GetTableDDL(ctx, t.Schema, t.TableName)
	if err != nil {
		return "", fmt.Errorf("failed to get current table DDL: %w", err)
	}

	sb.WriteString(fmt.Sprintf("-- Modify table: %s.%s\n", t.Schema, t.TableName))
	sb.WriteString("-- Edit this template and save, then touch .commit to execute\n\n")

	// Include current schema as comments
	sb.WriteString("-- Current schema:\n")
	for _, line := range strings.Split(currentDDL, "\n") {
		if line != "" {
			sb.WriteString("-- ")
			sb.WriteString(line)
			sb.WriteString("\n")
		}
	}

	sb.WriteString("\n-- Examples:\n")
	sb.WriteString("-- ADD COLUMN column_name TYPE\n")
	sb.WriteString("-- ADD COLUMN column_name TYPE NOT NULL DEFAULT value\n")
	sb.WriteString("-- DROP COLUMN column_name\n")
	sb.WriteString("-- ALTER COLUMN column_name TYPE new_type\n")
	sb.WriteString("-- ALTER COLUMN column_name SET NOT NULL\n")
	sb.WriteString("-- ALTER COLUMN column_name DROP NOT NULL\n")
	sb.WriteString("-- RENAME COLUMN old_name TO new_name\n")
	sb.WriteString("-- ADD CONSTRAINT name CHECK (condition)\n")
	sb.WriteString("-- DROP CONSTRAINT name\n")

	sb.WriteString(fmt.Sprintf("\nALTER TABLE \"%s\".\"%s\"\n", t.Schema, t.TableName))
	sb.WriteString("    -- ADD COLUMN new_column TEXT;\n")

	return sb.String(), nil
}

// DeleteTableTemplate generates a template for dropping a table.
// Includes dependency information (foreign keys, row count) to help users
// understand the impact of the deletion.
type DeleteTableTemplate struct {
	// Schema is the PostgreSQL schema containing the table
	Schema string
	// TableName is the name of the table to delete
	TableName string
	// DB provides access to table metadata (columns, row count, foreign keys)
	DB db.DBClient
}

// Generate returns a DROP TABLE template with dependency information.
func (t *DeleteTableTemplate) Generate(ctx context.Context) (string, error) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("-- Delete table: %s.%s\n", t.Schema, t.TableName))
	sb.WriteString("-- Edit this template and save, then touch .commit to execute\n\n")

	// Get columns
	columns, err := t.DB.GetColumns(ctx, t.Schema, t.TableName)
	if err != nil {
		return "", fmt.Errorf("failed to get columns: %w", err)
	}
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	// Get row count estimate
	rowCount, err := t.DB.GetRowCountSmart(ctx, t.Schema, t.TableName)
	if err != nil {
		return "", fmt.Errorf("failed to get row count: %w", err)
	}

	sb.WriteString(fmt.Sprintf("-- Table: %s.%s\n", t.Schema, t.TableName))
	sb.WriteString(fmt.Sprintf("-- Columns: %s\n", strings.Join(columnNames, ", ")))
	sb.WriteString(fmt.Sprintf("-- Rows: ~%d\n", rowCount))

	// Get referencing foreign keys
	refs, err := t.DB.GetReferencingForeignKeys(ctx, t.Schema, t.TableName)
	if err != nil {
		return "", fmt.Errorf("failed to get referencing foreign keys: %w", err)
	}

	if len(refs) > 0 {
		sb.WriteString("\n-- WARNING: Foreign keys reference this table:\n")
		for _, ref := range refs {
			sb.WriteString(fmt.Sprintf("--   %s.%s.%s -> %s.%s (~%d rows)\n",
				ref.SourceSchema, ref.SourceTable,
				strings.Join(ref.SourceColumns, ","),
				t.TableName,
				strings.Join(ref.TargetColumns, ","),
				ref.EstimatedRows))
		}
		sb.WriteString("\n-- Uncomment ONE of the following:\n")
		sb.WriteString(fmt.Sprintf("-- DROP TABLE \"%s\".\"%s\" RESTRICT;  -- Fail if dependencies exist\n", t.Schema, t.TableName))
		sb.WriteString(fmt.Sprintf("-- DROP TABLE \"%s\".\"%s\" CASCADE;   -- Drop dependent objects too\n", t.Schema, t.TableName))
	} else {
		sb.WriteString("\n-- No foreign keys reference this table.\n")
		sb.WriteString("\n-- Uncomment to delete:\n")
		sb.WriteString(fmt.Sprintf("-- DROP TABLE \"%s\".\"%s\";\n", t.Schema, t.TableName))
	}

	return sb.String(), nil
}

// =============================================================================
// Index Templates
// =============================================================================

// CreateIndexTemplate generates a template for creating an index.
// Includes examples of common index patterns (btree, unique, partial, expression).
type CreateIndexTemplate struct {
	// Schema is the PostgreSQL schema containing the table
	Schema string
	// TableName is the table the index will be created on
	TableName string
}

// Generate returns a CREATE INDEX template.
func (t *CreateIndexTemplate) Generate(ctx context.Context) (string, error) {
	return fmt.Sprintf(`-- Create index on: %s.%s
-- Edit this template and save, then touch .commit to execute

-- Standard B-tree index:
-- CREATE INDEX idx_%s_columnname ON "%s"."%s" (column_name);

-- Unique index:
-- CREATE UNIQUE INDEX idx_%s_columnname ON "%s"."%s" (column_name);

-- Composite index:
-- CREATE INDEX idx_%s_col1_col2 ON "%s"."%s" (col1, col2);

-- Partial index:
-- CREATE INDEX idx_%s_active ON "%s"."%s" (column_name) WHERE active = true;

-- Expression index:
-- CREATE INDEX idx_%s_lower_email ON "%s"."%s" (lower(email));

CREATE INDEX idx_%s_COLUMNNAME ON "%s"."%s" (
    -- column(s) here
);
`, t.Schema, t.TableName,
		t.TableName, t.Schema, t.TableName,
		t.TableName, t.Schema, t.TableName,
		t.TableName, t.Schema, t.TableName,
		t.TableName, t.Schema, t.TableName,
		t.TableName, t.Schema, t.TableName,
		t.TableName, t.Schema, t.TableName), nil
}

// DeleteIndexTemplate generates a template for dropping an index.
type DeleteIndexTemplate struct {
	// Schema is the PostgreSQL schema containing the index
	Schema string
	// IndexName is the name of the index to drop
	IndexName string
	// DB provides access to index metadata (currently unused but available for future enhancements)
	DB db.IndexReader
}

// Generate returns a DROP INDEX template with index details.
func (t *DeleteIndexTemplate) Generate(ctx context.Context) (string, error) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("-- Delete index: %s.%s\n", t.Schema, t.IndexName))
	sb.WriteString("-- Edit this template and save, then touch .commit to execute\n\n")

	// Note: We can't easily get index details without the table name
	// In a full implementation, we'd query pg_indexes directly by index name
	sb.WriteString(fmt.Sprintf("-- Index: %s\n", t.IndexName))
	sb.WriteString(fmt.Sprintf("-- Schema: %s\n", t.Schema))
	sb.WriteString("\n-- Uncomment to delete:\n")
	sb.WriteString(fmt.Sprintf("-- DROP INDEX \"%s\".\"%s\";\n", t.Schema, t.IndexName))

	return sb.String(), nil
}

// =============================================================================
// Schema Templates
// =============================================================================

// CreateSchemaTemplate generates a template for creating a schema.
// Includes examples for setting owner and granting permissions.
type CreateSchemaTemplate struct {
	// SchemaName is the name for the new schema
	SchemaName string
}

// Generate returns a CREATE SCHEMA template.
func (t *CreateSchemaTemplate) Generate(ctx context.Context) (string, error) {
	return fmt.Sprintf(`-- Create schema: %s
-- Edit this template and save, then touch .commit to execute

CREATE SCHEMA "%s";

-- Optional: Set schema owner
-- ALTER SCHEMA "%s" OWNER TO role_name;

-- Optional: Grant permissions
-- GRANT USAGE ON SCHEMA "%s" TO role_name;
-- GRANT ALL ON SCHEMA "%s" TO role_name;
`, t.SchemaName, t.SchemaName, t.SchemaName, t.SchemaName, t.SchemaName), nil
}

// DeleteSchemaTemplate generates a template for dropping a schema.
// Checks if schema contains tables and includes CASCADE/RESTRICT options accordingly.
type DeleteSchemaTemplate struct {
	// SchemaName is the name of the schema to drop
	SchemaName string
	// DB provides access to check schema contents (table count)
	DB db.DDLReader
}

// Generate returns a DROP SCHEMA template with content information.
func (t *DeleteSchemaTemplate) Generate(ctx context.Context) (string, error) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("-- Delete schema: %s\n", t.SchemaName))
	sb.WriteString("-- Edit this template and save, then touch .commit to execute\n\n")

	// Get table count
	tableCount, err := t.DB.GetSchemaTableCount(ctx, t.SchemaName)
	if err != nil {
		return "", fmt.Errorf("failed to get schema table count: %w", err)
	}

	sb.WriteString(fmt.Sprintf("-- Schema: %s\n", t.SchemaName))
	sb.WriteString(fmt.Sprintf("-- Tables: %d\n", tableCount))

	if tableCount > 0 {
		sb.WriteString("\n-- WARNING: Schema is not empty!\n")
		sb.WriteString("\n-- Uncomment ONE of the following:\n")
		sb.WriteString(fmt.Sprintf("-- DROP SCHEMA \"%s\" RESTRICT;  -- Fail if schema contains objects\n", t.SchemaName))
		sb.WriteString(fmt.Sprintf("-- DROP SCHEMA \"%s\" CASCADE;   -- Drop all contained objects\n", t.SchemaName))
	} else {
		sb.WriteString("\n-- Schema is empty.\n")
		sb.WriteString("\n-- Uncomment to delete:\n")
		sb.WriteString(fmt.Sprintf("-- DROP SCHEMA \"%s\";\n", t.SchemaName))
	}

	return sb.String(), nil
}

// =============================================================================
// View Templates
// =============================================================================

// CreateViewTemplate generates a template for creating a view.
// Includes examples for standard views and materialized views.
type CreateViewTemplate struct {
	// Schema is the PostgreSQL schema where the view will be created
	Schema string
	// ViewName is the name for the new view
	ViewName string
}

// Generate returns a CREATE VIEW template.
func (t *CreateViewTemplate) Generate(ctx context.Context) (string, error) {
	return fmt.Sprintf(`-- Create view: %s.%s
-- Edit this template and save, then touch .commit to execute

CREATE VIEW "%s"."%s" AS
SELECT
    -- columns here
    -- col1,
    -- col2,
    -- col3
FROM
    -- source table(s)
    -- schema.table_name
WHERE
    -- conditions (optional)
    -- column = value
;

-- Optional: Create as materialized view for better performance
-- CREATE MATERIALIZED VIEW "%s"."%s" AS
-- SELECT ...
-- WITH DATA;
`, t.Schema, t.ViewName, t.Schema, t.ViewName, t.Schema, t.ViewName), nil
}

// DeleteViewTemplate generates a template for dropping a view.
// Includes the view definition as context and checks for dependent views.
type DeleteViewTemplate struct {
	// Schema is the PostgreSQL schema containing the view
	Schema string
	// ViewName is the name of the view to drop
	ViewName string
	// DB provides access to view definition and dependency information
	DB db.DDLReader
}

// Generate returns a DROP VIEW template with view details.
func (t *DeleteViewTemplate) Generate(ctx context.Context) (string, error) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("-- Delete view: %s.%s\n", t.Schema, t.ViewName))
	sb.WriteString("-- Edit this template and save, then touch .commit to execute\n\n")

	// Get view definition
	viewDef, err := t.DB.GetViewDefinition(ctx, t.Schema, t.ViewName)
	if err != nil {
		// View might not exist yet or error getting definition
		sb.WriteString(fmt.Sprintf("-- View: %s.%s\n", t.Schema, t.ViewName))
		sb.WriteString("-- (Could not retrieve view definition)\n")
	} else {
		sb.WriteString("-- View definition:\n")
		for _, line := range strings.Split(viewDef, "\n") {
			if line != "" {
				sb.WriteString("-- ")
				sb.WriteString(line)
				sb.WriteString("\n")
			}
		}
	}

	// Get dependent views
	dependentViews, err := t.DB.GetDependentViews(ctx, t.Schema, t.ViewName)
	if err == nil && len(dependentViews) > 0 {
		sb.WriteString("\n-- WARNING: Other views depend on this view:\n")
		for _, v := range dependentViews {
			sb.WriteString(fmt.Sprintf("--   %s\n", v))
		}
		sb.WriteString("\n-- Uncomment ONE of the following:\n")
		sb.WriteString(fmt.Sprintf("-- DROP VIEW \"%s\".\"%s\" RESTRICT;  -- Fail if dependencies exist\n", t.Schema, t.ViewName))
		sb.WriteString(fmt.Sprintf("-- DROP VIEW \"%s\".\"%s\" CASCADE;   -- Drop dependent views too\n", t.Schema, t.ViewName))
	} else {
		sb.WriteString("\n-- Uncomment to delete:\n")
		sb.WriteString(fmt.Sprintf("-- DROP VIEW \"%s\".\"%s\";\n", t.Schema, t.ViewName))
	}

	return sb.String(), nil
}
