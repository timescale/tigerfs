package synth

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

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

func TestSynth_ParseFeatureString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		format  SynthFormat
		history bool
	}{
		{"markdown only", "markdown", FormatMarkdown, false},
		{"markdown with history", "markdown,history", FormatMarkdown, true},
		{"history only", "history", FormatNative, true},
		{"txt with history", "txt,history", FormatPlainText, true},
		{"whitespace handling", "  markdown , history  ", FormatMarkdown, true},
		{"md alias", "md,history", FormatMarkdown, true},
		{"txt only", "txt", FormatPlainText, false},
		{"empty string", "", FormatNative, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := ParseFeatureString(tt.input)
			if fs.Format != tt.format {
				t.Errorf("ParseFeatureString(%q).Format = %v, want %v", tt.input, fs.Format, tt.format)
			}
			if fs.History != tt.history {
				t.Errorf("ParseFeatureString(%q).History = %v, want %v", tt.input, fs.History, tt.history)
			}
		})
	}
}

func TestSynth_FeatureComment(t *testing.T) {
	tests := []struct {
		name string
		fs   FeatureSet
		want string
	}{
		{"markdown", FeatureSet{Format: FormatMarkdown}, "tigerfs:md"},
		{"markdown+history", FeatureSet{Format: FormatMarkdown, History: true}, "tigerfs:md,history"},
		{"txt+history", FeatureSet{Format: FormatPlainText, History: true}, "tigerfs:txt,history"},
		{"history only", FeatureSet{History: true}, "tigerfs:history"},
		{"native no history", FeatureSet{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FeatureComment(tt.fs); got != tt.want {
				t.Errorf("FeatureComment(%+v) = %q, want %q", tt.fs, got, tt.want)
			}
		})
	}
}

func TestSynth_DetectFeaturesFromComment(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		format  SynthFormat
		history bool
	}{
		{"md only", "tigerfs:md", FormatMarkdown, false},
		{"md+history", "tigerfs:md,history", FormatMarkdown, true},
		{"txt+history", "tigerfs:txt,history", FormatPlainText, true},
		{"tasks", "tigerfs:tasks", FormatTasks, false},
		{"unknown", "some comment", FormatNative, false},
		{"empty", "", FormatNative, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := DetectFeaturesFromComment(tt.comment)
			if fs.Format != tt.format {
				t.Errorf("DetectFeaturesFromComment(%q).Format = %v, want %v", tt.comment, fs.Format, tt.format)
			}
			if fs.History != tt.history {
				t.Errorf("DetectFeaturesFromComment(%q).History = %v, want %v", tt.comment, fs.History, tt.history)
			}
		})
	}
}

func TestSynth_FeatureCommentRoundTrip(t *testing.T) {
	// Verify that encoding and decoding a FeatureSet is lossless
	sets := []FeatureSet{
		{Format: FormatMarkdown},
		{Format: FormatMarkdown, History: true},
		{Format: FormatPlainText, History: true},
	}
	for _, fs := range sets {
		comment := FeatureComment(fs)
		decoded := DetectFeaturesFromComment(comment)
		if decoded.Format != fs.Format || decoded.History != fs.History {
			t.Errorf("round-trip failed: %+v → %q → %+v", fs, comment, decoded)
		}
	}
}

func TestSynth_UUIDv7ToVersionID(t *testing.T) {
	// Create a UUIDv7 from a known time
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	versionID := UUIDv7ToVersionID(id)

	// Verify it parses back
	ts, err := VersionIDToTimestamp(versionID)
	if err != nil {
		t.Fatalf("VersionIDToTimestamp(%q) failed: %v", versionID, err)
	}

	// The extracted time should be within 1 second of now (UUIDv7 has ms precision,
	// but version ID has second precision)
	diff := time.Since(ts)
	if diff < 0 || diff > 2*time.Second {
		t.Errorf("UUIDv7 time extraction off by %v", diff)
	}
}

func TestSynth_VersionIDToTimestamp_Invalid(t *testing.T) {
	_, err := VersionIDToTimestamp("not-a-timestamp")
	if err == nil {
		t.Error("expected error for invalid version ID")
	}
}
