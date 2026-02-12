// Package fs tests for path.go
package fs

import (
	"testing"
)

// TestPathType verifies PathType constants are distinct.
func TestPathType(t *testing.T) {
	types := []PathType{
		PathRoot, PathSchemaList, PathSchema, PathTable,
		PathCapability, PathRow, PathColumn, PathInfo,
		PathExport, PathImport, PathDDL, PathViewList, PathBuild, PathFormat,
	}
	seen := make(map[PathType]bool)
	for _, pt := range types {
		if seen[pt] {
			t.Errorf("duplicate PathType: %d", pt)
		}
		seen[pt] = true
	}
}

// TestParsePathRoot verifies root path parsing.
func TestParsePathRoot(t *testing.T) {
	result, err := ParsePath("/")
	if err != nil {
		t.Fatalf("ParsePath('/') error: %v", err)
	}
	if result.Type != PathRoot {
		t.Errorf("Type = %v, want PathRoot", result.Type)
	}
}

// TestParsePathSchemaList verifies /.schemas/ path.
func TestParsePathSchemaList(t *testing.T) {
	result, err := ParsePath("/.schemas")
	if err != nil {
		t.Fatalf("ParsePath('/.schemas') error: %v", err)
	}
	if result.Type != PathSchemaList {
		t.Errorf("Type = %v, want PathSchemaList", result.Type)
	}

	// With trailing slash
	result, err = ParsePath("/.schemas/")
	if err != nil {
		t.Fatalf("ParsePath('/.schemas/') error: %v", err)
	}
	if result.Type != PathSchemaList {
		t.Errorf("Type = %v, want PathSchemaList", result.Type)
	}
}

// TestParsePathSchema verifies /.schemas/<name> path.
func TestParsePathSchema(t *testing.T) {
	result, err := ParsePath("/.schemas/myschema")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}
	if result.Type != PathSchema {
		t.Errorf("Type = %v, want PathSchema", result.Type)
	}
	if result.Context.Schema != "myschema" {
		t.Errorf("Schema = %q, want %q", result.Context.Schema, "myschema")
	}
}

// TestParsePathTable verifies table paths.
func TestParsePathTable(t *testing.T) {
	tests := []struct {
		path   string
		schema string
		table  string
	}{
		{"/users", "", "users"},
		{"/users/", "", "users"},
		// Explicit schema access via /.schemas/
		{"/.schemas/public/users", "public", "users"},
		{"/.schemas/myschema/mytable", "myschema", "mytable"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathTable {
				t.Errorf("Type = %v, want PathTable", result.Type)
			}
			if result.Context.Schema != tt.schema {
				t.Errorf("Schema = %q, want %q", result.Context.Schema, tt.schema)
			}
			if result.Context.TableName != tt.table {
				t.Errorf("TableName = %q, want %q", result.Context.TableName, tt.table)
			}
		})
	}
}

// TestParsePathRow verifies row paths.
func TestParsePathRow(t *testing.T) {
	tests := []struct {
		path   string
		table  string
		pk     string
		format string
	}{
		{"/users/123", "users", "123", ""},
		{"/users/123.json", "users", "123", "json"},
		{"/users/123.csv", "users", "123", "csv"},
		{"/users/123.tsv", "users", "123", "tsv"},
		{"/users/abc-def", "users", "abc-def", ""},
		// Explicit schema access via /.schemas/
		{"/.schemas/public/users/456", "users", "456", ""},
		// Text primary keys
		{"/categories/automotive", "categories", "automotive", ""},
		// Row format files as separate path segments (NFS-style cd into row dir then cat .json)
		{"/users/123/.json", "users", "123", "json"},
		{"/users/123/.csv", "users", "123", "csv"},
		{"/users/123/.tsv", "users", "123", "tsv"},
		{"/users/123/.yaml", "users", "123", "yaml"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathRow {
				t.Errorf("Type = %v, want PathRow", result.Type)
			}
			if result.Context.TableName != tt.table {
				t.Errorf("TableName = %q, want %q", result.Context.TableName, tt.table)
			}
			if result.PrimaryKey != tt.pk {
				t.Errorf("PrimaryKey = %q, want %q", result.PrimaryKey, tt.pk)
			}
			if result.Format != tt.format {
				t.Errorf("Format = %q, want %q", result.Format, tt.format)
			}
		})
	}
}

// TestParsePathColumn verifies column paths.
func TestParsePathColumn(t *testing.T) {
	result, err := ParsePath("/users/123/name")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}
	if result.Type != PathColumn {
		t.Errorf("Type = %v, want PathColumn", result.Type)
	}
	if result.PrimaryKey != "123" {
		t.Errorf("PrimaryKey = %q, want %q", result.PrimaryKey, "123")
	}
	if result.Column != "name" {
		t.Errorf("Column = %q, want %q", result.Column, "name")
	}
}

// TestParsePathInfo verifies .info/ paths.
func TestParsePathInfo(t *testing.T) {
	tests := []struct {
		path     string
		table    string
		infoFile string
	}{
		{"/users/.info", "users", ""},
		{"/users/.info/", "users", ""},
		{"/users/.info/count", "users", "count"},
		{"/users/.info/ddl", "users", "ddl"},
		{"/users/.info/columns", "users", "columns"},
		{"/users/.info/schema", "users", "schema"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathInfo {
				t.Errorf("Type = %v, want PathInfo", result.Type)
			}
			if result.Context.TableName != tt.table {
				t.Errorf("TableName = %q, want %q", result.Context.TableName, tt.table)
			}
			if result.InfoFile != tt.infoFile {
				t.Errorf("InfoFile = %q, want %q", result.InfoFile, tt.infoFile)
			}
		})
	}
}

// TestParsePathCapabilityBy verifies .by/ capability paths.
func TestParsePathCapabilityBy(t *testing.T) {
	// .by/ directory listing
	result, err := ParsePath("/users/.by")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}
	if result.Type != PathCapability {
		t.Errorf("Type = %v, want PathCapability", result.Type)
	}

	// .by/<column> listing
	result, err = ParsePath("/users/.by/email")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}
	if result.Type != PathCapability {
		t.Errorf("Type = %v, want PathCapability", result.Type)
	}

	// .by/<column>/<value> - adds filter
	result, err = ParsePath("/users/.by/email/foo@bar.com")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}
	if result.Type != PathTable {
		t.Errorf("Type = %v, want PathTable (after filter applied)", result.Type)
	}
	if len(result.Context.Filters) != 1 {
		t.Fatalf("Filters count = %d, want 1", len(result.Context.Filters))
	}
	if result.Context.Filters[0].Column != "email" {
		t.Errorf("Filter column = %q, want %q", result.Context.Filters[0].Column, "email")
	}
	if result.Context.Filters[0].Value != "foo@bar.com" {
		t.Errorf("Filter value = %q, want %q", result.Context.Filters[0].Value, "foo@bar.com")
	}
	if !result.Context.Filters[0].Indexed {
		t.Error("Filter should be marked as indexed for .by/")
	}
}

// TestParsePathCapabilityFilter verifies .filter/ capability paths.
func TestParsePathCapabilityFilter(t *testing.T) {
	result, err := ParsePath("/users/.filter/status/active")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}
	if len(result.Context.Filters) != 1 {
		t.Fatalf("Filters count = %d, want 1", len(result.Context.Filters))
	}
	if result.Context.Filters[0].Column != "status" {
		t.Errorf("Filter column = %q, want %q", result.Context.Filters[0].Column, "status")
	}
	if result.Context.Filters[0].Indexed {
		t.Error("Filter should NOT be marked as indexed for .filter/")
	}
}

// TestParsePathCapabilityOrder verifies .order/ capability paths.
func TestParsePathCapabilityOrder(t *testing.T) {
	// Ascending order
	result, err := ParsePath("/users/.order/created_at")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}
	if result.Context.OrderBy != "created_at" {
		t.Errorf("OrderBy = %q, want %q", result.Context.OrderBy, "created_at")
	}
	if result.Context.OrderDesc {
		t.Error("OrderDesc should be false for ascending")
	}

	// Descending order
	result, err = ParsePath("/users/.order/created_at.desc")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}
	if result.Context.OrderBy != "created_at" {
		t.Errorf("OrderBy = %q, want %q", result.Context.OrderBy, "created_at")
	}
	if !result.Context.OrderDesc {
		t.Error("OrderDesc should be true for .desc")
	}
}

// TestParsePathCapabilityPagination verifies .first/, .last/, .sample/ paths.
func TestParsePathCapabilityPagination(t *testing.T) {
	tests := []struct {
		path      string
		limit     int
		limitType LimitType
	}{
		{"/users/.first/10", 10, LimitFirst},
		{"/users/.last/20", 20, LimitLast},
		{"/users/.sample/5", 5, LimitSample},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Context.Limit != tt.limit {
				t.Errorf("Limit = %d, want %d", result.Context.Limit, tt.limit)
			}
			if result.Context.LimitType != tt.limitType {
				t.Errorf("LimitType = %v, want %v", result.Context.LimitType, tt.limitType)
			}
		})
	}
}

// TestParsePathCapabilityChain verifies chained capabilities.
func TestParsePathCapabilityChain(t *testing.T) {
	// Complex chain: filter + order + pagination
	result, err := ParsePath("/users/.filter/active/true/.order/created_at/.first/10")
	if err != nil {
		t.Fatalf("ParsePath error: %v", err)
	}

	// Check filter
	if len(result.Context.Filters) != 1 {
		t.Fatalf("Filters count = %d, want 1", len(result.Context.Filters))
	}
	if result.Context.Filters[0].Column != "active" {
		t.Errorf("Filter column = %q, want %q", result.Context.Filters[0].Column, "active")
	}

	// Check order
	if result.Context.OrderBy != "created_at" {
		t.Errorf("OrderBy = %q, want %q", result.Context.OrderBy, "created_at")
	}

	// Check limit
	if result.Context.Limit != 10 {
		t.Errorf("Limit = %d, want 10", result.Context.Limit)
	}
	if result.Context.LimitType != LimitFirst {
		t.Errorf("LimitType = %v, want LimitFirst", result.Context.LimitType)
	}
}

// TestParsePathExport verifies .export/ paths.
func TestParsePathExport(t *testing.T) {
	tests := []struct {
		path   string
		format string
	}{
		{"/users/.export", ""},
		{"/users/.export/", ""},
		{"/users/.export/all.csv", "csv"},
		{"/users/.export/all.json", "json"},
		{"/users/.export/json", "json"},
		{"/users/.filter/active/true/.export/filtered.csv", "csv"},
		{"/users/.first/5/.export/json", "json"},
		{"/users/.first/5/.export/csv", "csv"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathExport {
				t.Errorf("Type = %v, want PathExport", result.Type)
			}
			if result.Format != tt.format {
				t.Errorf("Format = %q, want %q", result.Format, tt.format)
			}
		})
	}
}

// TestParsePathPipelineExport verifies that pipeline operations are preserved in export paths.
func TestParsePathPipelineExport(t *testing.T) {
	path := "/users/.first/5/.export/json"
	result, err := ParsePath(path)
	if err != nil {
		t.Fatalf("ParsePath(%q) error: %v", path, err)
	}

	// Verify path type
	if result.Type != PathExport {
		t.Errorf("Type = %v, want PathExport", result.Type)
	}

	// Verify format
	if result.Format != "json" {
		t.Errorf("Format = %q, want %q", result.Format, "json")
	}

	// Verify context exists
	if result.Context == nil {
		t.Fatal("Context is nil, want non-nil")
	}

	// Verify table name
	if result.Context.TableName != "users" {
		t.Errorf("TableName = %q, want %q", result.Context.TableName, "users")
	}

	// Verify limit is preserved
	if result.Context.Limit != 5 {
		t.Errorf("Limit = %d, want 5", result.Context.Limit)
	}

	// Verify limit type is First
	if result.Context.LimitType != LimitFirst {
		t.Errorf("LimitType = %v, want LimitFirst", result.Context.LimitType)
	}

	// Verify HasPipelineOperations returns true
	if !result.Context.HasPipelineOperations() {
		t.Error("HasPipelineOperations() = false, want true")
	}

	// Verify terminal state
	if !result.Context.IsTerminal {
		t.Error("IsTerminal = false, want true")
	}
}

// TestParsePathImport verifies .import/ paths.
func TestParsePathImport(t *testing.T) {
	tests := []struct {
		path       string
		importMode string
		format     string
		noHeaders  bool
	}{
		{"/users/.import", "", "", false},
		{"/users/.import/.sync", "sync", "", false},
		{"/users/.import/.overwrite", "overwrite", "", false},
		{"/users/.import/.append", "append", "", false},
		{"/users/.import/.overwrite/csv", "overwrite", "csv", false},
		{"/users/.import/.sync/json", "sync", "json", false},
		{"/users/.import/.append/data.csv", "append", "csv", false},
		// .no-headers option
		{"/users/.import/.overwrite/.no-headers", "overwrite", "", true},
		{"/users/.import/.overwrite/.no-headers/csv", "overwrite", "csv", true},
		{"/users/.import/.sync/.no-headers/tsv", "sync", "tsv", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathImport {
				t.Errorf("Type = %v, want PathImport", result.Type)
			}
			if result.ImportMode != tt.importMode {
				t.Errorf("ImportMode = %q, want %q", result.ImportMode, tt.importMode)
			}
			if result.Format != tt.format {
				t.Errorf("Format = %q, want %q", result.Format, tt.format)
			}
			if result.ImportNoHeaders != tt.noHeaders {
				t.Errorf("ImportNoHeaders = %v, want %v", result.ImportNoHeaders, tt.noHeaders)
			}
		})
	}
}

// TestParsePathDDL verifies DDL staging paths.
func TestParsePathDDL(t *testing.T) {
	tests := []struct {
		path    string
		ddlOp   string
		ddlName string
		ddlFile string
	}{
		// Root-level .create (for tables)
		{"/.create/myindex", "create", "myindex", ""},
		{"/.create/myindex/", "create", "myindex", ""},
		{"/.create/myindex/sql", "create", "myindex", "sql"},
		{"/.create/myindex/.test", "create", "myindex", ".test"},
		{"/.create/myindex/.commit", "create", "myindex", ".commit"},
		{"/.create/myindex/.abort", "create", "myindex", ".abort"},
		{"/.create/myindex/test.log", "create", "myindex", "test.log"},
		// Table-level .modify and .delete (per spec)
		{"/users/.modify", "modify", "users", ""},
		{"/users/.modify/sql", "modify", "users", "sql"},
		{"/users/.modify/.test", "modify", "users", ".test"},
		{"/users/.modify/.commit", "modify", "users", ".commit"},
		{"/users/.delete", "delete", "users", ""},
		{"/users/.delete/sql", "delete", "users", "sql"},
		{"/users/.delete/.commit", "delete", "users", ".commit"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathDDL {
				t.Errorf("Type = %v, want PathDDL", result.Type)
			}
			if result.DDLOp != tt.ddlOp {
				t.Errorf("DDLOp = %q, want %q", result.DDLOp, tt.ddlOp)
			}
			if result.DDLName != tt.ddlName {
				t.Errorf("DDLName = %q, want %q", result.DDLName, tt.ddlName)
			}
			if result.DDLFile != tt.ddlFile {
				t.Errorf("DDLFile = %q, want %q", result.DDLFile, tt.ddlFile)
			}
		})
	}
}

// TestParsePathSchemaDDL verifies schema DDL path parsing.
func TestParsePathSchemaDDL(t *testing.T) {
	tests := []struct {
		path          string
		ddlOp         string
		ddlName       string
		ddlFile       string
		ddlObjectType string
	}{
		// Schema create
		{"/.schemas/.create", "create", "", "", "schema"},
		{"/.schemas/.create/myschema", "create", "myschema", "", "schema"},
		{"/.schemas/.create/myschema/sql", "create", "myschema", "sql", "schema"},
		{"/.schemas/.create/myschema/.test", "create", "myschema", ".test", "schema"},
		{"/.schemas/.create/myschema/.commit", "create", "myschema", ".commit", "schema"},
		// Schema delete
		{"/.schemas/oldschema/.delete", "delete", "oldschema", "", "schema"},
		{"/.schemas/oldschema/.delete/sql", "delete", "oldschema", "sql", "schema"},
		{"/.schemas/oldschema/.delete/.commit", "delete", "oldschema", ".commit", "schema"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathDDL {
				t.Errorf("Type = %v, want PathDDL", result.Type)
			}
			if result.DDLOp != tt.ddlOp {
				t.Errorf("DDLOp = %q, want %q", result.DDLOp, tt.ddlOp)
			}
			if result.DDLName != tt.ddlName {
				t.Errorf("DDLName = %q, want %q", result.DDLName, tt.ddlName)
			}
			if result.DDLFile != tt.ddlFile {
				t.Errorf("DDLFile = %q, want %q", result.DDLFile, tt.ddlFile)
			}
			if result.DDLObjectType != tt.ddlObjectType {
				t.Errorf("DDLObjectType = %q, want %q", result.DDLObjectType, tt.ddlObjectType)
			}
		})
	}
}

// TestParsePathViewDDL verifies view DDL path parsing.
func TestParsePathViewDDL(t *testing.T) {
	// View create paths
	createTests := []struct {
		path          string
		ddlOp         string
		ddlName       string
		ddlFile       string
		ddlObjectType string
	}{
		{"/.views/.create", "create", "", "", "view"},
		{"/.views/.create/myview", "create", "myview", "", "view"},
		{"/.views/.create/myview/sql", "create", "myview", "sql", "view"},
		{"/.views/.create/myview/.test", "create", "myview", ".test", "view"},
		{"/.views/.create/myview/.commit", "create", "myview", ".commit", "view"},
	}

	for _, tt := range createTests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathDDL {
				t.Errorf("Type = %v, want PathDDL", result.Type)
			}
			if result.DDLOp != tt.ddlOp {
				t.Errorf("DDLOp = %q, want %q", result.DDLOp, tt.ddlOp)
			}
			if result.DDLName != tt.ddlName {
				t.Errorf("DDLName = %q, want %q", result.DDLName, tt.ddlName)
			}
			if result.DDLFile != tt.ddlFile {
				t.Errorf("DDLFile = %q, want %q", result.DDLFile, tt.ddlFile)
			}
			if result.DDLObjectType != tt.ddlObjectType {
				t.Errorf("DDLObjectType = %q, want %q", result.DDLObjectType, tt.ddlObjectType)
			}
		})
	}

	// /.views/ directory should return PathViewList
	t.Run("/.views", func(t *testing.T) {
		result, err := ParsePath("/.views")
		if err != nil {
			t.Fatalf("ParsePath(/.views) error: %v", err)
		}
		if result.Type != PathViewList {
			t.Errorf("Type = %v, want PathViewList", result.Type)
		}
	})

	// /.views/{name} should return error (views accessed from root)
	t.Run("/.views/myview_invalid", func(t *testing.T) {
		_, err := ParsePath("/.views/myview")
		if err == nil {
			t.Error("ParsePath(/.views/myview) should return error - views accessed from root")
		}
	})
}

// TestParsePathIndexDDL verifies index DDL path parsing.
func TestParsePathIndexDDL(t *testing.T) {
	tests := []struct {
		path           string
		ddlOp          string
		ddlName        string
		ddlFile        string
		ddlObjectType  string
		ddlParentTable string
	}{
		// Index create
		{"/users/.indexes/.create", "create", "", "", "index", "users"},
		{"/users/.indexes/.create/idx_email", "create", "idx_email", "", "index", "users"},
		{"/users/.indexes/.create/idx_email/sql", "create", "idx_email", "sql", "index", "users"},
		{"/users/.indexes/.create/idx_email/.test", "create", "idx_email", ".test", "index", "users"},
		{"/users/.indexes/.create/idx_email/.commit", "create", "idx_email", ".commit", "index", "users"},
		// Index delete
		{"/users/.indexes/idx_old/.delete", "delete", "idx_old", "", "index", "users"},
		{"/users/.indexes/idx_old/.delete/sql", "delete", "idx_old", "sql", "index", "users"},
		{"/users/.indexes/idx_old/.delete/.commit", "delete", "idx_old", ".commit", "index", "users"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathDDL {
				t.Errorf("Type = %v, want PathDDL", result.Type)
			}
			if result.DDLOp != tt.ddlOp {
				t.Errorf("DDLOp = %q, want %q", result.DDLOp, tt.ddlOp)
			}
			if result.DDLName != tt.ddlName {
				t.Errorf("DDLName = %q, want %q", result.DDLName, tt.ddlName)
			}
			if result.DDLFile != tt.ddlFile {
				t.Errorf("DDLFile = %q, want %q", result.DDLFile, tt.ddlFile)
			}
			if result.DDLObjectType != tt.ddlObjectType {
				t.Errorf("DDLObjectType = %q, want %q", result.DDLObjectType, tt.ddlObjectType)
			}
			if result.DDLParentTable != tt.ddlParentTable {
				t.Errorf("DDLParentTable = %q, want %q", result.DDLParentTable, tt.ddlParentTable)
			}
		})
	}

	// /{table}/.indexes/ should list indexes (PathCapability)
	t.Run("/users/.indexes", func(t *testing.T) {
		result, err := ParsePath("/users/.indexes")
		if err != nil {
			t.Fatalf("ParsePath(/users/.indexes) error: %v", err)
		}
		if result.Type != PathCapability {
			t.Errorf("Type = %v, want PathCapability", result.Type)
		}
		if result.CapabilityDir != ".indexes" {
			t.Errorf("CapabilityDir = %q, want .indexes", result.CapabilityDir)
		}
	})

	// /{table}/.indexes/{name} should be index info (PathCapability with arg)
	t.Run("/users/.indexes/idx_email", func(t *testing.T) {
		result, err := ParsePath("/users/.indexes/idx_email")
		if err != nil {
			t.Fatalf("ParsePath(/users/.indexes/idx_email) error: %v", err)
		}
		if result.Type != PathCapability {
			t.Errorf("Type = %v, want PathCapability", result.Type)
		}
		if result.CapabilityDir != ".indexes" {
			t.Errorf("CapabilityDir = %q, want .indexes", result.CapabilityDir)
		}
		if result.CapabilityArg != "idx_email" {
			t.Errorf("CapabilityArg = %q, want idx_email", result.CapabilityArg)
		}
	})
}

// TestParsePathRootLevelDDLInvalid verifies that .modify and .delete at root level are invalid.
func TestParsePathRootLevelDDLInvalid(t *testing.T) {
	// These paths were valid before but should now be parsed as table paths
	// (which will fail validation when the "table" doesn't exist)
	tests := []struct {
		path string
		desc string
	}{
		{"/.modify", "root .modify directory"},
		{"/.modify/users", "root .modify with name"},
		{"/.delete", "root .delete directory"},
		{"/.delete/tablename", "root .delete with name"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			// These should parse as table paths (the "table" name is .modify or .delete)
			// They won't return an error at parse time, but won't be PathDDL
			if err != nil {
				// If it errors, that's also acceptable
				return
			}
			if result.Type == PathDDL {
				t.Errorf("ParsePath(%q) should NOT return PathDDL for root-level %s", tt.path, tt.desc)
			}
		})
	}
}

// TestParsePathInvalid verifies error handling for invalid paths.
func TestParsePathInvalid(t *testing.T) {
	tests := []struct {
		path string
		desc string
	}{
		{"", "empty path"},
		{"users", "no leading slash"},
		{"/users/.first/abc", "non-numeric limit"},
		{"/users/.first/-1", "negative limit"},
		{"/users/.unknown/foo", "unknown capability"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := ParsePath(tt.path)
			if err == nil {
				t.Errorf("ParsePath(%q) should return error for %s", tt.path, tt.desc)
			}
		})
	}
}

// TestParsePathDisallowedCombinations verifies that invalid capability combinations are rejected.
func TestParsePathDisallowedCombinations(t *testing.T) {
	tests := []struct {
		path string
		desc string
	}{
		// Filter after order is disallowed
		{"/users/.order/created_at/.filter/status/active", "filter after order"},
		{"/users/.order/created_at/.by/email/foo@bar.com", "by after order"},

		// Double order is disallowed
		{"/users/.order/created_at/.order/name", "order after order"},

		// Double sample is disallowed
		{"/users/.sample/10/.sample/5", "sample after sample"},

		// Anything after sample is disallowed
		{"/users/.sample/10/.first/5", "first after sample"},
		{"/users/.sample/10/.last/5", "last after sample"},

		// Double first is disallowed (redundant)
		{"/users/.first/10/.first/5", "first after first"},

		// Double last is disallowed (redundant)
		{"/users/.last/10/.last/5", "last after last"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := ParsePath(tt.path)
			if err == nil {
				t.Errorf("ParsePath(%q) should return error for %s", tt.path, tt.desc)
			}
		})
	}
}

// TestParsePathAllowedCombinations verifies that valid capability combinations work.
func TestParsePathAllowedCombinations(t *testing.T) {
	tests := []struct {
		path string
		desc string
	}{
		// Filter before order is allowed
		{"/users/.filter/status/active/.order/created_at", "filter then order"},
		{"/users/.by/email/foo@bar.com/.order/created_at", "by then order"},

		// Multiple filters are allowed
		{"/users/.filter/status/active/.filter/role/admin", "filter then filter"},
		{"/users/.by/status/active/.by/role/admin", "by then by"},
		{"/users/.by/status/active/.filter/role/admin", "by then filter"},

		// First then last (nested pagination) is allowed
		{"/users/.first/100/.last/10", "first then last"},
		{"/users/.last/100/.first/10", "last then first"},

		// Order then limit is allowed
		{"/users/.order/created_at/.first/10", "order then first"},
		{"/users/.order/created_at/.last/10", "order then last"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := ParsePath(tt.path)
			if err != nil {
				t.Errorf("ParsePath(%q) should succeed for %s, got error: %v", tt.path, tt.desc, err)
			}
		})
	}
}

// TestParsePathEdgeCases verifies edge cases.
func TestParsePathEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		path string
		test func(t *testing.T, result *ParsedPath)
	}{
		{
			name: "pk with dots",
			path: "/users/foo.bar.baz",
			test: func(t *testing.T, r *ParsedPath) {
				// Should be PK "foo.bar.baz" not format detection
				if r.PrimaryKey != "foo.bar.baz" {
					t.Errorf("PrimaryKey = %q, want %q", r.PrimaryKey, "foo.bar.baz")
				}
				if r.Format != "" {
					t.Errorf("Format = %q, want empty (baz is not a known format)", r.Format)
				}
			},
		},
		{
			name: "pk with special chars",
			path: "/users/abc-123_456",
			test: func(t *testing.T, r *ParsedPath) {
				if r.PrimaryKey != "abc-123_456" {
					t.Errorf("PrimaryKey = %q, want %q", r.PrimaryKey, "abc-123_456")
				}
			},
		},
		{
			name: "table name with underscore",
			path: "/user_accounts/123",
			test: func(t *testing.T, r *ParsedPath) {
				if r.Context.TableName != "user_accounts" {
					t.Errorf("TableName = %q, want %q", r.Context.TableName, "user_accounts")
				}
			},
		},
		{
			name: "nested pagination",
			path: "/users/.first/100/.last/10",
			test: func(t *testing.T, r *ParsedPath) {
				if r.Context.Limit != 10 {
					t.Errorf("Limit = %d, want 10", r.Context.Limit)
				}
				if r.Context.PreviousLimit != 100 {
					t.Errorf("PreviousLimit = %d, want 100", r.Context.PreviousLimit)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			tt.test(t, result)
		})
	}
}

// TestParsePathPipelineColumn verifies column paths through pipelines
func TestParsePathPipelineColumn(t *testing.T) {
	tests := []struct {
		path   string
		table  string
		pk     string
		column string
	}{
		{"/orders/.by/product_id/100/019c257f-2fb2-7f27-8365-d510ed5b1f23/quantity", "orders", "019c257f-2fb2-7f27-8365-d510ed5b1f23", "quantity"},
		{"/categories/automotive/name", "categories", "automotive", "name"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tt.path, err)
			}
			if result.Type != PathColumn {
				t.Errorf("Type = %v, want PathColumn", result.Type)
			}
			if result.Context.TableName != tt.table {
				t.Errorf("TableName = %q, want %q", result.Context.TableName, tt.table)
			}
			if result.PrimaryKey != tt.pk {
				t.Errorf("PrimaryKey = %q, want %q", result.PrimaryKey, tt.pk)
			}
			if result.Column != tt.column {
				t.Errorf("Column = %q, want %q", result.Column, tt.column)
			}
		})
	}
}

// TestParsePathBuild verifies /.build/ path parsing.
func TestParsePathBuild(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantType  PathType
		wantBuild string
		wantErr   bool
	}{
		{
			name:     "build directory",
			path:     "/.build",
			wantType: PathBuild,
		},
		{
			name:     "build directory with trailing slash",
			path:     "/.build/",
			wantType: PathBuild,
		},
		{
			name:      "build with app name",
			path:      "/.build/posts",
			wantType:  PathBuild,
			wantBuild: "posts",
		},
		{
			name:      "build with app name trailing slash",
			path:      "/.build/notes/",
			wantType:  PathBuild,
			wantBuild: "notes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", result.Type, tt.wantType)
			}
			if result.BuildName != tt.wantBuild {
				t.Errorf("BuildName = %q, want %q", result.BuildName, tt.wantBuild)
			}
		})
	}
}

// TestParsePathBuildInSchema verifies /.schemas/<schema>/.build/ path parsing.
func TestParsePathBuildInSchema(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantType   PathType
		wantBuild  string
		wantSchema string
	}{
		{
			name:       "schema build directory",
			path:       "/.schemas/myschema/.build",
			wantType:   PathBuild,
			wantSchema: "myschema",
		},
		{
			name:       "schema build with name",
			path:       "/.schemas/myschema/.build/posts",
			wantType:   PathBuild,
			wantBuild:  "posts",
			wantSchema: "myschema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", result.Type, tt.wantType)
			}
			if result.BuildName != tt.wantBuild {
				t.Errorf("BuildName = %q, want %q", result.BuildName, tt.wantBuild)
			}
			if result.Context == nil {
				t.Fatal("Context should not be nil for schema-level build")
			}
			if result.Context.Schema != tt.wantSchema {
				t.Errorf("Schema = %q, want %q", result.Context.Schema, tt.wantSchema)
			}
		})
	}
}

// TestParsePathFormat verifies /{table}/.format/ path parsing.
func TestParsePathFormat(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantType   PathType
		wantTable  string
		wantTarget string
	}{
		{
			name:      "format directory",
			path:      "/posts/.format",
			wantType:  PathFormat,
			wantTable: "posts",
		},
		{
			name:      "format directory trailing slash",
			path:      "/posts/.format/",
			wantType:  PathFormat,
			wantTable: "posts",
		},
		{
			name:       "format with target",
			path:       "/posts/.format/markdown",
			wantType:   PathFormat,
			wantTable:  "posts",
			wantTarget: "markdown",
		},
		{
			name:       "format with txt target",
			path:       "/notes/.format/txt",
			wantType:   PathFormat,
			wantTable:  "notes",
			wantTarget: "txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePath(tt.path)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", result.Type, tt.wantType)
			}
			if result.Context == nil {
				t.Fatal("Context should not be nil")
			}
			if result.Context.TableName != tt.wantTable {
				t.Errorf("TableName = %q, want %q", result.Context.TableName, tt.wantTable)
			}
			if result.FormatTarget != tt.wantTarget {
				t.Errorf("FormatTarget = %q, want %q", result.FormatTarget, tt.wantTarget)
			}
		})
	}
}
