package synth

import (
	"strings"
	"testing"
)

func TestSynthesizePlainText(t *testing.T) {
	columns := []string{"id", "filename", "body"}
	values := []interface{}{"uuid-1", "hello", "Hello World"}
	roles := &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"}

	data, err := SynthesizePlainText(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "Hello World") {
		t.Errorf("should contain body, got: %q", content)
	}
	if !strings.HasSuffix(content, "\n") {
		t.Errorf("should end with newline, got: %q", content)
	}
}

func TestSynthesizePlainText_NullBody(t *testing.T) {
	columns := []string{"id", "filename", "body"}
	values := []interface{}{"uuid-1", "hello", nil}
	roles := &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"}

	data, err := SynthesizePlainText(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(data) != "" {
		t.Errorf("NULL body should produce empty content, got: %q", string(data))
	}
}

func TestSynthesizePlainText_TrailingNewline(t *testing.T) {
	columns := []string{"id", "filename", "body"}
	values := []interface{}{"uuid-1", "hello", "Already has newline\n"}
	roles := &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"}

	data, err := SynthesizePlainText(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not double-newline
	if strings.HasSuffix(string(data), "\n\n") {
		t.Errorf("should not double trailing newline, got: %q", string(data))
	}
}

func TestParsePlainText(t *testing.T) {
	body := ParsePlainText([]byte("Hello World\n"))
	if body != "Hello World\n" {
		t.Errorf("ParsePlainText should return content as-is, got: %q", body)
	}
}

func TestParsePlainText_Empty(t *testing.T) {
	body := ParsePlainText([]byte(""))
	if body != "" {
		t.Errorf("ParsePlainText of empty should be empty, got: %q", body)
	}
}

func TestGetPlainTextFilename(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		values   []interface{}
		roles    *ColumnRoles
		expected string
	}{
		{
			name:     "basic filename",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"1", "hello", "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "hello.txt",
		},
		{
			name:     "already has .txt",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"1", "hello.txt", "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "hello.txt",
		},
		{
			name:     "NULL filename falls back to PK",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"42", nil, "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "42.txt",
		},
		{
			name:     "preserves slashes",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"1", "path/to/file", "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "path/to/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetPlainTextFilename(tt.columns, tt.values, tt.roles)
			if got != tt.expected {
				t.Errorf("GetPlainTextFilename() = %q, want %q", got, tt.expected)
			}
		})
	}
}
