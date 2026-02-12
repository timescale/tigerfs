package synth

import "testing"

func TestSynthFormat_String(t *testing.T) {
	tests := []struct {
		format SynthFormat
		want   string
	}{
		{FormatNative, "native"},
		{FormatMarkdown, "markdown"},
		{FormatPlainText, "txt"},
		{FormatTasks, "tasks"},
	}
	for _, tt := range tests {
		if got := tt.format.String(); got != tt.want {
			t.Errorf("SynthFormat(%d).String() = %q, want %q", tt.format, got, tt.want)
		}
	}
}

func TestSynthFormat_Extension(t *testing.T) {
	tests := []struct {
		format SynthFormat
		want   string
	}{
		{FormatNative, ""},
		{FormatMarkdown, ".md"},
		{FormatPlainText, ".txt"},
		{FormatTasks, ".md"},
	}
	for _, tt := range tests {
		if got := tt.format.Extension(); got != tt.want {
			t.Errorf("SynthFormat(%d).Extension() = %q, want %q", tt.format, got, tt.want)
		}
	}
}

func TestParseFormatName(t *testing.T) {
	tests := []struct {
		input string
		want  SynthFormat
	}{
		{"markdown", FormatMarkdown},
		{"md", FormatMarkdown},
		{"MARKDOWN", FormatMarkdown},
		{"  markdown  ", FormatMarkdown},
		{"txt", FormatPlainText},
		{"text", FormatPlainText},
		{"plaintext", FormatPlainText},
		{"plain", FormatPlainText},
		{"tasks", FormatTasks},
		{"todo", FormatTasks},
		{"items", FormatTasks},
		{"unknown", FormatNative},
		{"", FormatNative},
	}
	for _, tt := range tests {
		if got := ParseFormatName(tt.input); got != tt.want {
			t.Errorf("ParseFormatName(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestDetectFormat_SuffixPriority(t *testing.T) {
	tests := []struct {
		name    string
		view    string
		columns []string
		want    SynthFormat
	}{
		{
			name:    "suffix _md",
			view:    "posts_md",
			columns: []string{"id", "data"},
			want:    FormatMarkdown,
		},
		{
			name:    "suffix _txt",
			view:    "notes_txt",
			columns: []string{"id", "data"},
			want:    FormatPlainText,
		},
		{
			name:    "suffix _tasks",
			view:    "work_tasks",
			columns: []string{"id", "data"},
			want:    FormatTasks,
		},
		{
			name:    "suffix _todo",
			view:    "my_todo",
			columns: []string{"id", "data"},
			want:    FormatTasks,
		},
		{
			name:    "suffix _items",
			view:    "backlog_items",
			columns: []string{"id", "data"},
			want:    FormatTasks,
		},
		{
			name:    "suffix takes priority over columns",
			view:    "notes_txt",
			columns: []string{"filename", "body", "author"},
			want:    FormatPlainText, // suffix wins, not markdown from columns
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DetectFormat(tt.view, tt.columns); got != tt.want {
				t.Errorf("DetectFormat(%q, %v) = %v, want %v", tt.view, tt.columns, got, tt.want)
			}
		})
	}
}

func TestDetectFormat_ColumnPatterns(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
		want    SynthFormat
	}{
		{
			name:    "markdown: filename + body + extra columns",
			columns: []string{"filename", "body", "author"},
			want:    FormatMarkdown,
		},
		{
			name:    "markdown: title + content + date",
			columns: []string{"title", "content", "date"},
			want:    FormatMarkdown,
		},
		{
			name:    "markdown: slug + description + tags",
			columns: []string{"slug", "description", "tags"},
			want:    FormatMarkdown,
		},
		{
			name:    "plaintext: filename + body only",
			columns: []string{"filename", "body"},
			want:    FormatPlainText,
		},
		{
			name:    "plaintext: name + text only",
			columns: []string{"name", "text"},
			want:    FormatPlainText,
		},
		{
			name:    "tasks: number + name + status + body",
			columns: []string{"number", "name", "status", "body"},
			want:    FormatTasks,
		},
		{
			name:    "tasks: has all task columns plus extras",
			columns: []string{"number", "name", "status", "body", "assignee"},
			want:    FormatTasks,
		},
		{
			name:    "native: no recognized pattern",
			columns: []string{"id", "data", "created_at"},
			want:    FormatNative,
		},
		{
			name:    "native: filename only without body",
			columns: []string{"filename", "other"},
			want:    FormatNative,
		},
		{
			name:    "native: body only without filename",
			columns: []string{"body", "other", "another"},
			want:    FormatNative,
		},
		{
			name:    "native: empty columns",
			columns: []string{},
			want:    FormatNative,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DetectFormat("generic_view", tt.columns); got != tt.want {
				t.Errorf("DetectFormat(\"generic_view\", %v) = %v, want %v", tt.columns, got, tt.want)
			}
		})
	}
}

func TestDetectFormatFromComment(t *testing.T) {
	tests := []struct {
		comment string
		want    SynthFormat
	}{
		{"tigerfs:md", FormatMarkdown},
		{"tigerfs:txt", FormatPlainText},
		{"tigerfs:tasks", FormatTasks},
		{"tigerfs:md auto-generated", FormatMarkdown},
		{"  tigerfs:md  ", FormatMarkdown},
		{"some other comment", FormatNative},
		{"", FormatNative},
		{"tigerfs:", FormatNative},
	}
	for _, tt := range tests {
		if got := DetectFormatFromComment(tt.comment); got != tt.want {
			t.Errorf("DetectFormatFromComment(%q) = %v, want %v", tt.comment, got, tt.want)
		}
	}
}

func TestFormatComment(t *testing.T) {
	tests := []struct {
		format SynthFormat
		want   string
	}{
		{FormatMarkdown, "tigerfs:md"},
		{FormatPlainText, "tigerfs:txt"},
		{FormatTasks, "tigerfs:tasks"},
		{FormatNative, ""},
	}
	for _, tt := range tests {
		if got := FormatComment(tt.format); got != tt.want {
			t.Errorf("FormatComment(%v) = %q, want %q", tt.format, got, tt.want)
		}
	}
}
