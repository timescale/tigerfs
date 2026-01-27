package fuse

import (
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

func TestGetExtensionForType(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		want     string
	}{
		// Text types → .txt
		{"text", "text", ".txt"},
		{"TEXT uppercase", "TEXT", ".txt"},
		{"character varying", "character varying", ".txt"},
		{"varchar", "varchar", ".txt"},
		{"character", "character", ".txt"},
		{"char", "char", ".txt"},
		{"bpchar", "bpchar", ".txt"},
		{"varchar with length", "character varying(255)", ".txt"},
		{"char with length", "character(10)", ".txt"},

		// JSON types → .json
		{"json", "json", ".json"},
		{"jsonb", "jsonb", ".json"},
		{"JSONB uppercase", "JSONB", ".json"},

		// XML → .xml
		{"xml", "xml", ".xml"},

		// Binary → .bin
		{"bytea", "bytea", ".bin"},

		// PostGIS → .wkb
		{"geometry", "geometry", ".wkb"},
		{"geography", "geography", ".wkb"},

		// Types without extensions
		{"integer", "integer", ""},
		{"bigint", "bigint", ""},
		{"smallint", "smallint", ""},
		{"numeric", "numeric", ""},
		{"numeric with precision", "numeric(10,2)", ""},
		{"boolean", "boolean", ""},
		{"date", "date", ""},
		{"timestamp", "timestamp", ""},
		{"timestamp with time zone", "timestamp with time zone", ""},
		{"uuid", "uuid", ""},
		{"inet", "inet", ""},
		{"array", "integer[]", ""},
		{"text array", "text[]", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetExtensionForType(tt.dataType)
			if got != tt.want {
				t.Errorf("GetExtensionForType(%q) = %q, want %q", tt.dataType, got, tt.want)
			}
		})
	}
}

func TestAddExtensionToColumn(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		dataType   string
		want       string
	}{
		{"text column", "name", "text", "name.txt"},
		{"json column", "metadata", "jsonb", "metadata.json"},
		{"xml column", "config", "xml", "config.xml"},
		{"binary column", "avatar", "bytea", "avatar.bin"},
		{"geometry column", "location", "geometry", "location.wkb"},
		{"integer column", "age", "integer", "age"},
		{"boolean column", "active", "boolean", "active"},
		{"column with underscore", "first_name", "text", "first_name.txt"},
		{"column ending in json", "config_json", "jsonb", "config_json.json"},
		// Edge case: column name already has a dot (PostgreSQL allows this)
		{"column name with dot", "data.txt", "text", "data.txt.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AddExtensionToColumn(tt.columnName, tt.dataType)
			if got != tt.want {
				t.Errorf("AddExtensionToColumn(%q, %q) = %q, want %q", tt.columnName, tt.dataType, got, tt.want)
			}
		})
	}
}

func TestStripExtension(t *testing.T) {
	tests := []struct {
		name         string
		filename     string
		wantColumn   string
		wantStripped bool
	}{
		{"txt extension", "name.txt", "name", true},
		{"json extension", "metadata.json", "metadata", true},
		{"xml extension", "config.xml", "config", true},
		{"bin extension", "avatar.bin", "avatar", true},
		{"wkb extension", "location.wkb", "location", true},
		{"no extension", "age", "age", false},
		{"unknown extension", "file.pdf", "file.pdf", false},
		{"double extension keeps first", "data.backup.txt", "data.backup", true},
		{"underscore name with txt", "first_name.txt", "first_name", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotColumn, gotStripped := StripExtension(tt.filename)
			if gotColumn != tt.wantColumn {
				t.Errorf("StripExtension(%q) column = %q, want %q", tt.filename, gotColumn, tt.wantColumn)
			}
			if gotStripped != tt.wantStripped {
				t.Errorf("StripExtension(%q) stripped = %v, want %v", tt.filename, gotStripped, tt.wantStripped)
			}
		})
	}
}

func TestFindColumnByFilename(t *testing.T) {
	columns := []db.Column{
		{Name: "id", DataType: "integer"},
		{Name: "name", DataType: "text"},
		{Name: "email", DataType: "character varying"},
		{Name: "metadata", DataType: "jsonb"},
		{Name: "avatar", DataType: "bytea"},
		{Name: "location", DataType: "geometry"},
		{Name: "active", DataType: "boolean"},
		{Name: "config_json", DataType: "jsonb"}, // Column name happens to end in _json
		{Name: "data.txt", DataType: "text"},     // Column name literally contains .txt (PostgreSQL allows this)
	}

	tests := []struct {
		name      string
		filename  string
		wantName  string
		wantFound bool
	}{
		// Exact matches (no extension needed for types without extensions)
		{"exact match integer", "id", "id", true},
		{"exact match boolean", "active", "active", true},

		// Extension matches
		{"text with extension", "name.txt", "name", true},
		{"varchar with extension", "email.txt", "email", true},
		{"jsonb with extension", "metadata.json", "metadata", true},
		{"bytea with extension", "avatar.bin", "avatar", true},
		{"geometry with extension", "location.wkb", "location", true},
		{"column ending in json with extension", "config_json.json", "config_json", true},

		// Also allow exact column name (backward compat when extensions disabled)
		{"text without extension", "name", "name", true},
		{"jsonb without extension", "metadata", "metadata", true},

		// Wrong extension should not match
		{"wrong extension", "name.json", "", false},
		{"wrong extension on jsonb", "metadata.txt", "", false},

		// Non-existent columns
		{"non-existent", "foo", "", false},
		{"non-existent with extension", "foo.txt", "", false},

		// Edge case: column name literally contains .txt (PostgreSQL allows "data.txt" as column name)
		{"literal dot-txt column exact", "data.txt", "data.txt", true},        // Exact match wins
		{"literal dot-txt column with ext", "data.txt.txt", "data.txt", true}, // data.txt + .txt extension
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, found := FindColumnByFilename(columns, tt.filename)
			if found != tt.wantFound {
				t.Errorf("FindColumnByFilename(%q) found = %v, want %v", tt.filename, found, tt.wantFound)
			}
			if found && col.Name != tt.wantName {
				t.Errorf("FindColumnByFilename(%q) name = %q, want %q", tt.filename, col.Name, tt.wantName)
			}
		})
	}
}
