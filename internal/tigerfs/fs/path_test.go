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
		PathExport, PathImport, PathDDL,
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
		{"/users", "public", "users"},
		{"/users/", "public", "users"},
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
	}{
		{"/users/.import", ""},
		{"/users/.import/.sync", "sync"},
		{"/users/.import/.overwrite", "overwrite"},
		{"/users/.import/.append", "append"},
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
		{"/.create/myindex", "create", "myindex", ""},
		{"/.create/myindex/", "create", "myindex", ""},
		{"/.create/myindex/sql", "create", "myindex", "sql"},
		{"/.create/myindex/.test", "create", "myindex", ".test"},
		{"/.create/myindex/.commit", "create", "myindex", ".commit"},
		{"/.create/myindex/.abort", "create", "myindex", ".abort"},
		{"/.create/myindex/test.log", "create", "myindex", "test.log"},
		{"/.modify/users", "modify", "users", ""},
		{"/.delete/old_index", "delete", "old_index", ""},
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
