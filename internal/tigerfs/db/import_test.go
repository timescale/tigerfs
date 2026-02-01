package db

import "testing"

// TestEscapeCopyValue tests the escapeCopyValue helper function
// which escapes values for PostgreSQL COPY text format.
func TestEscapeCopyValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		// Basic strings - no escaping needed
		{
			name:     "simple_string",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "empty_string",
			input:    "",
			expected: "",
		},
		{
			name:     "string_with_spaces",
			input:    "hello world",
			expected: "hello world",
		},

		// Backslash escaping
		{
			name:     "single_backslash",
			input:    `a\b`,
			expected: `a\\b`,
		},
		{
			name:     "multiple_backslashes",
			input:    `a\\b`,
			expected: `a\\\\b`,
		},
		{
			name:     "backslash_at_end",
			input:    `path\`,
			expected: `path\\`,
		},

		// Newline escaping
		{
			name:     "newline",
			input:    "line1\nline2",
			expected: `line1\nline2`,
		},
		{
			name:     "multiple_newlines",
			input:    "a\nb\nc",
			expected: `a\nb\nc`,
		},

		// Carriage return escaping
		{
			name:     "carriage_return",
			input:    "line1\rline2",
			expected: `line1\rline2`,
		},
		{
			name:     "crlf",
			input:    "line1\r\nline2",
			expected: `line1\r\nline2`,
		},

		// Tab escaping
		{
			name:     "tab",
			input:    "col1\tcol2",
			expected: `col1\tcol2`,
		},
		{
			name:     "multiple_tabs",
			input:    "a\tb\tc",
			expected: `a\tb\tc`,
		},

		// Combined special characters
		{
			name:     "all_special_chars",
			input:    "a\\b\nc\rd\te",
			expected: `a\\b\nc\rd\te`,
		},
		{
			name:     "backslash_before_newline",
			input:    "a\\\nb",
			expected: `a\\\nb`,
		},

		// Non-string types (converted via fmt.Sprintf)
		{
			name:     "integer",
			input:    42,
			expected: "42",
		},
		{
			name:     "float",
			input:    3.14,
			expected: "3.14",
		},
		{
			name:     "boolean_true",
			input:    true,
			expected: "true",
		},
		{
			name:     "boolean_false",
			input:    false,
			expected: "false",
		},
		{
			name:     "nil",
			input:    nil,
			expected: "<nil>",
		},

		// Unicode - should pass through unchanged
		{
			name:     "unicode",
			input:    "日本語",
			expected: "日本語",
		},
		{
			name:     "emoji",
			input:    "hello 👋 world",
			expected: "hello 👋 world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeCopyValue(tt.input)
			if result != tt.expected {
				t.Errorf("escapeCopyValue(%#v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
