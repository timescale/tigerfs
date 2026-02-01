package nfs

import "testing"

// TestHasFormatExtension tests the hasFormatExtension helper function
func TestHasFormatExtension(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		// Valid format extensions
		{"csv_extension", "123.csv", true},
		{"json_extension", "123.json", true},
		{"tsv_extension", "123.tsv", true},
		{"yaml_extension", "123.yaml", true},

		// No extension
		{"no_extension", "123", false},
		{"directory_name", "users", false},

		// Non-format extensions
		{"txt_extension", "file.txt", false},
		{"bin_extension", "data.bin", false},
		{"sql_extension", "query.sql", false},
		{"md_extension", "readme.md", false},

		// Edge cases
		{"empty_string", "", false},
		{"dot_only", ".", false},
		{"hidden_file", ".json", true}, // .json is the extension
		{"double_extension", "file.tar.json", true},
		{"uppercase_ignored", "123.JSON", false}, // case sensitive
		{"uppercase_csv", "123.CSV", false},

		// Primary key values that might look like extensions
		{"pk_with_dot", "user.name", false},
		{"uuid_style", "550e8400-e29b-41d4-a716-446655440000", false},
		{"numeric", "12345", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasFormatExtension(tt.filename)
			if result != tt.expected {
				t.Errorf("hasFormatExtension(%q) = %v, want %v", tt.filename, result, tt.expected)
			}
		})
	}
}
