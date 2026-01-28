package fuse

import (
	"context"
	"strings"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// =============================================================================
// CreateTableTemplate Tests
// =============================================================================

func TestCreateTableTemplate_Generate(t *testing.T) {
	template := &CreateTableTemplate{
		Schema:    "public",
		TableName: "users",
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Check for expected content
	expectedPatterns := []string{
		"CREATE TABLE",
		`"public"."users"`,
		"PRIMARY KEY",
		"SERIAL",
		"TEXT NOT NULL",
	}

	for _, pattern := range expectedPatterns {
		if !strings.Contains(result, pattern) {
			t.Errorf("Expected result to contain %q", pattern)
		}
	}
}

func TestCreateTableTemplate_Generate_CustomSchema(t *testing.T) {
	template := &CreateTableTemplate{
		Schema:    "myschema",
		TableName: "orders",
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if !strings.Contains(result, `"myschema"."orders"`) {
		t.Error("Expected result to contain schema.table reference")
	}
}

// =============================================================================
// ModifyTableTemplate Tests
// =============================================================================

func TestModifyTableTemplate_Generate(t *testing.T) {
	mock := db.NewMockDBClient()
	mock.MockDDLReader.GetTableDDLFunc = func(ctx context.Context, schema, table string) (string, error) {
		return `CREATE TABLE "public"."users" (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text
);`, nil
	}

	template := &ModifyTableTemplate{
		Schema:    "public",
		TableName: "users",
		DB:        mock,
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include current schema as comments
	if !strings.Contains(result, "-- Current schema:") {
		t.Error("Expected result to contain current schema section")
	}

	// Should include the table definition as comments
	if !strings.Contains(result, "-- CREATE TABLE") {
		t.Error("Expected result to contain commented current DDL")
	}

	// Should include ALTER TABLE stub
	if !strings.Contains(result, `ALTER TABLE "public"."users"`) {
		t.Error("Expected result to contain ALTER TABLE stub")
	}

	// Should include examples
	expectedExamples := []string{
		"ADD COLUMN",
		"DROP COLUMN",
		"ALTER COLUMN",
		"RENAME COLUMN",
	}

	for _, example := range expectedExamples {
		if !strings.Contains(result, example) {
			t.Errorf("Expected result to contain example: %q", example)
		}
	}
}

// =============================================================================
// DeleteTableTemplate Tests
// =============================================================================

func TestDeleteTableTemplate_Generate_NoDependencies(t *testing.T) {
	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
			{Name: "email", DataType: "text"},
		}, nil
	}
	mock.MockCountReader.GetRowCountSmartFunc = func(ctx context.Context, schema, table string) (int64, error) {
		return 42, nil
	}
	mock.MockDDLReader.GetReferencingForeignKeysFunc = func(ctx context.Context, schema, table string) ([]db.ForeignKeyRef, error) {
		return []db.ForeignKeyRef{}, nil
	}

	template := &DeleteTableTemplate{
		Schema:    "public",
		TableName: "users",
		DB:        mock,
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include table info
	if !strings.Contains(result, "-- Table: public.users") {
		t.Error("Expected result to contain table name")
	}

	if !strings.Contains(result, "-- Columns: id, name, email") {
		t.Error("Expected result to contain column list")
	}

	if !strings.Contains(result, "-- Rows: ~42") {
		t.Error("Expected result to contain row count")
	}

	// Should NOT include CASCADE warning
	if strings.Contains(result, "CASCADE") {
		t.Error("Expected result NOT to mention CASCADE when no dependencies")
	}

	// Should include simple DROP TABLE
	if !strings.Contains(result, `-- DROP TABLE "public"."users";`) {
		t.Error("Expected result to contain DROP TABLE statement")
	}
}

func TestDeleteTableTemplate_Generate_WithDependencies(t *testing.T) {
	mock := db.NewMockDBClient()
	mock.MockSchemaReader.GetColumnsFunc = func(ctx context.Context, schema, table string) ([]db.Column, error) {
		return []db.Column{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
		}, nil
	}
	mock.MockCountReader.GetRowCountSmartFunc = func(ctx context.Context, schema, table string) (int64, error) {
		return 100, nil
	}
	mock.MockDDLReader.GetReferencingForeignKeysFunc = func(ctx context.Context, schema, table string) ([]db.ForeignKeyRef, error) {
		return []db.ForeignKeyRef{
			{
				ConstraintName: "orders_user_id_fkey",
				SourceSchema:   "public",
				SourceTable:    "orders",
				SourceColumns:  []string{"user_id"},
				TargetColumns:  []string{"id"},
				EstimatedRows:  3847,
			},
		}, nil
	}

	template := &DeleteTableTemplate{
		Schema:    "public",
		TableName: "users",
		DB:        mock,
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include FK warning
	if !strings.Contains(result, "WARNING: Foreign keys reference this table") {
		t.Error("Expected result to contain FK warning")
	}

	// Should include FK details
	if !strings.Contains(result, "public.orders.user_id") {
		t.Error("Expected result to contain FK source info")
	}

	if !strings.Contains(result, "~3847 rows") {
		t.Error("Expected result to contain FK row count")
	}

	// Should include both RESTRICT and CASCADE options
	if !strings.Contains(result, "RESTRICT") {
		t.Error("Expected result to contain RESTRICT option")
	}

	if !strings.Contains(result, "CASCADE") {
		t.Error("Expected result to contain CASCADE option")
	}
}

// =============================================================================
// CreateIndexTemplate Tests
// =============================================================================

func TestCreateIndexTemplate_Generate(t *testing.T) {
	template := &CreateIndexTemplate{
		Schema:    "public",
		TableName: "users",
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include various index patterns
	expectedPatterns := []string{
		"CREATE INDEX",
		"CREATE UNIQUE INDEX",
		"idx_users_",
		`"public"."users"`,
		"-- Partial index",
		"-- Expression index",
	}

	for _, pattern := range expectedPatterns {
		if !strings.Contains(result, pattern) {
			t.Errorf("Expected result to contain %q", pattern)
		}
	}
}

// =============================================================================
// DeleteIndexTemplate Tests
// =============================================================================

func TestDeleteIndexTemplate_Generate(t *testing.T) {
	mock := db.NewMockDBClient()

	template := &DeleteIndexTemplate{
		Schema:    "public",
		IndexName: "users_email_idx",
		DB:        mock,
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include index info
	if !strings.Contains(result, "-- Index: users_email_idx") {
		t.Error("Expected result to contain index name")
	}

	// Should include DROP INDEX statement
	if !strings.Contains(result, `-- DROP INDEX "public"."users_email_idx";`) {
		t.Error("Expected result to contain DROP INDEX statement")
	}
}

// =============================================================================
// CreateSchemaTemplate Tests
// =============================================================================

func TestCreateSchemaTemplate_Generate(t *testing.T) {
	template := &CreateSchemaTemplate{
		SchemaName: "myapp",
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include CREATE SCHEMA
	if !strings.Contains(result, `CREATE SCHEMA "myapp"`) {
		t.Error("Expected result to contain CREATE SCHEMA")
	}

	// Should include optional owner/permission examples
	if !strings.Contains(result, "OWNER TO") {
		t.Error("Expected result to contain owner example")
	}

	if !strings.Contains(result, "GRANT") {
		t.Error("Expected result to contain grant example")
	}
}

// =============================================================================
// DeleteSchemaTemplate Tests
// =============================================================================

func TestDeleteSchemaTemplate_Generate_Empty(t *testing.T) {
	mock := db.NewMockDBClient()
	mock.MockDDLReader.GetSchemaTableCountFunc = func(ctx context.Context, schema string) (int, error) {
		return 0, nil
	}

	template := &DeleteSchemaTemplate{
		SchemaName: "myapp",
		DB:         mock,
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should indicate schema is empty
	if !strings.Contains(result, "Schema is empty") {
		t.Error("Expected result to indicate schema is empty")
	}

	// Should NOT include CASCADE warning
	if strings.Contains(result, "CASCADE") {
		t.Error("Expected result NOT to mention CASCADE for empty schema")
	}

	// Should include simple DROP SCHEMA
	if !strings.Contains(result, `-- DROP SCHEMA "myapp";`) {
		t.Error("Expected result to contain DROP SCHEMA statement")
	}
}

func TestDeleteSchemaTemplate_Generate_NotEmpty(t *testing.T) {
	mock := db.NewMockDBClient()
	mock.MockDDLReader.GetSchemaTableCountFunc = func(ctx context.Context, schema string) (int, error) {
		return 5, nil
	}

	template := &DeleteSchemaTemplate{
		SchemaName: "myapp",
		DB:         mock,
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include table count
	if !strings.Contains(result, "-- Tables: 5") {
		t.Error("Expected result to contain table count")
	}

	// Should include warning
	if !strings.Contains(result, "WARNING: Schema is not empty") {
		t.Error("Expected result to contain warning")
	}

	// Should include both RESTRICT and CASCADE options
	if !strings.Contains(result, "RESTRICT") {
		t.Error("Expected result to contain RESTRICT option")
	}

	if !strings.Contains(result, "CASCADE") {
		t.Error("Expected result to contain CASCADE option")
	}
}

// =============================================================================
// CreateViewTemplate Tests
// =============================================================================

func TestCreateViewTemplate_Generate(t *testing.T) {
	template := &CreateViewTemplate{
		Schema:   "public",
		ViewName: "active_users",
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include CREATE VIEW
	if !strings.Contains(result, `CREATE VIEW "public"."active_users"`) {
		t.Error("Expected result to contain CREATE VIEW")
	}

	// Should include SELECT structure
	if !strings.Contains(result, "SELECT") {
		t.Error("Expected result to contain SELECT")
	}

	// Should include materialized view option
	if !strings.Contains(result, "MATERIALIZED VIEW") {
		t.Error("Expected result to contain materialized view option")
	}
}

// =============================================================================
// DeleteViewTemplate Tests
// =============================================================================

func TestDeleteViewTemplate_Generate_NoDependencies(t *testing.T) {
	mock := db.NewMockDBClient()
	mock.MockDDLReader.GetViewDefinitionFunc = func(ctx context.Context, schema, view string) (string, error) {
		return "SELECT id, name FROM users WHERE active = true", nil
	}
	mock.MockDDLReader.GetDependentViewsFunc = func(ctx context.Context, schema, name string) ([]string, error) {
		return []string{}, nil
	}

	template := &DeleteViewTemplate{
		Schema:   "public",
		ViewName: "active_users",
		DB:       mock,
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include view definition as comment
	if !strings.Contains(result, "-- View definition:") {
		t.Error("Expected result to contain view definition section")
	}

	// Should NOT include CASCADE warning
	if strings.Contains(result, "CASCADE") {
		t.Error("Expected result NOT to mention CASCADE when no dependencies")
	}

	// Should include simple DROP VIEW
	if !strings.Contains(result, `-- DROP VIEW "public"."active_users";`) {
		t.Error("Expected result to contain DROP VIEW statement")
	}
}

func TestDeleteViewTemplate_Generate_WithDependencies(t *testing.T) {
	mock := db.NewMockDBClient()
	mock.MockDDLReader.GetViewDefinitionFunc = func(ctx context.Context, schema, view string) (string, error) {
		return "SELECT id, name FROM users", nil
	}
	mock.MockDDLReader.GetDependentViewsFunc = func(ctx context.Context, schema, name string) ([]string, error) {
		return []string{"user_summary", "active_user_count"}, nil
	}

	template := &DeleteViewTemplate{
		Schema:   "public",
		ViewName: "base_users",
		DB:       mock,
	}

	result, err := template.Generate(context.Background())

	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	// Should include dependent views warning
	if !strings.Contains(result, "WARNING: Other views depend on this view") {
		t.Error("Expected result to contain dependency warning")
	}

	// Should list dependent views
	if !strings.Contains(result, "user_summary") {
		t.Error("Expected result to list dependent view")
	}

	// Should include both RESTRICT and CASCADE options
	if !strings.Contains(result, "RESTRICT") {
		t.Error("Expected result to contain RESTRICT option")
	}

	if !strings.Contains(result, "CASCADE") {
		t.Error("Expected result to contain CASCADE option")
	}
}

// =============================================================================
// Interface Compliance Tests
// =============================================================================

func TestTemplateGenerator_Interface(t *testing.T) {
	// Verify all templates implement TemplateGenerator
	var _ TemplateGenerator = &CreateTableTemplate{}
	var _ TemplateGenerator = &ModifyTableTemplate{}
	var _ TemplateGenerator = &DeleteTableTemplate{}
	var _ TemplateGenerator = &CreateIndexTemplate{}
	var _ TemplateGenerator = &DeleteIndexTemplate{}
	var _ TemplateGenerator = &CreateSchemaTemplate{}
	var _ TemplateGenerator = &DeleteSchemaTemplate{}
	var _ TemplateGenerator = &CreateViewTemplate{}
	var _ TemplateGenerator = &DeleteViewTemplate{}
}
