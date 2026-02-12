package synth

import (
	"strings"
	"testing"
)

func TestGenerateSynthesizedViewSQL_Markdown(t *testing.T) {
	sql, err := GenerateSynthesizedViewSQL("public", "posts", FormatMarkdown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, `"posts_md"`) {
		t.Errorf("should create posts_md view, got:\n%s", sql)
	}
	if !strings.Contains(sql, "CREATE VIEW") {
		t.Errorf("should contain CREATE VIEW, got:\n%s", sql)
	}
	if !strings.Contains(sql, "tigerfs:md") {
		t.Errorf("should set tigerfs:md comment, got:\n%s", sql)
	}
}

func TestGenerateSynthesizedViewSQL_PlainText(t *testing.T) {
	sql, err := GenerateSynthesizedViewSQL("public", "snippets", FormatPlainText)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, `"snippets_txt"`) {
		t.Errorf("should create snippets_txt view, got:\n%s", sql)
	}
	if !strings.Contains(sql, "tigerfs:txt") {
		t.Errorf("should set tigerfs:txt comment, got:\n%s", sql)
	}
}

func TestGenerateSynthesizedViewSQL_Native(t *testing.T) {
	_, err := GenerateSynthesizedViewSQL("public", "posts", FormatNative)
	if err == nil {
		t.Fatal("expected error for native format")
	}
}

func TestSynthViewName(t *testing.T) {
	tests := []struct {
		table    string
		format   SynthFormat
		expected string
		wantErr  bool
	}{
		{"posts", FormatMarkdown, "posts_md", false},
		{"notes", FormatPlainText, "notes_txt", false},
		{"todos", FormatTasks, "todos_tasks", false},
		{"posts", FormatNative, "", true},
	}

	for _, tt := range tests {
		name, err := SynthViewName(tt.table, tt.format)
		if tt.wantErr {
			if err == nil {
				t.Errorf("SynthViewName(%q, %v) expected error", tt.table, tt.format)
			}
			continue
		}
		if err != nil {
			t.Errorf("SynthViewName(%q, %v) error: %v", tt.table, tt.format, err)
			continue
		}
		if name != tt.expected {
			t.Errorf("SynthViewName(%q, %v) = %q, want %q", tt.table, tt.format, name, tt.expected)
		}
	}
}

func TestViewNameFromTableAndFormat(t *testing.T) {
	tests := []struct {
		table     string
		formatStr string
		wantView  string
		wantErr   bool
	}{
		{"posts", "markdown", "posts_md", false},
		{"posts", "md", "posts_md", false},
		{"notes", "txt", "notes_txt", false},
		{"posts", "native", "", true},
		{"posts", "tasks", "", true},
		{"posts", "invalid", "", true},
	}

	for _, tt := range tests {
		view, _, err := ViewNameFromTableAndFormat(tt.table, tt.formatStr)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ViewNameFromTableAndFormat(%q, %q) expected error", tt.table, tt.formatStr)
			}
			continue
		}
		if err != nil {
			t.Errorf("ViewNameFromTableAndFormat(%q, %q) error: %v", tt.table, tt.formatStr, err)
			continue
		}
		if view != tt.wantView {
			t.Errorf("ViewNameFromTableAndFormat(%q, %q) = %q, want %q", tt.table, tt.formatStr, view, tt.wantView)
		}
	}
}
