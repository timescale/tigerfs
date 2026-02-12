package synth

import (
	"strings"
	"testing"
)

func TestSynthesizeMarkdown_WithFrontmatter(t *testing.T) {
	columns := []string{"id", "filename", "title", "author", "body", "created_at"}
	values := []interface{}{"uuid-1", "hello-world", "Hello World", "alice", "# Hello\n\nWelcome!", "2024-01-15"}
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title", "author", "created_at"},
		PrimaryKey:  "id",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// Should have frontmatter delimiters
	if !strings.HasPrefix(content, "---\n") {
		t.Errorf("should start with ---\\n, got: %q", content[:20])
	}

	// Should contain frontmatter values
	if !strings.Contains(content, "title: Hello World") {
		t.Errorf("should contain title frontmatter, got:\n%s", content)
	}
	if !strings.Contains(content, "author: alice") {
		t.Errorf("should contain author frontmatter, got:\n%s", content)
	}

	// Should have closing delimiter
	parts := strings.SplitN(content, "---\n", 3)
	if len(parts) < 3 {
		t.Fatalf("expected 3 parts split by ---, got %d", len(parts))
	}

	// Body should be after second delimiter
	body := parts[2]
	if !strings.Contains(body, "# Hello") {
		t.Errorf("body should contain markdown content, got:\n%s", body)
	}
}

func TestSynthesizeMarkdown_NoFrontmatter(t *testing.T) {
	columns := []string{"id", "filename", "body"}
	values := []interface{}{"uuid-1", "hello", "Just body content\n"}
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: nil,
		PrimaryKey:  "id",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// Should NOT have frontmatter delimiters
	if strings.Contains(content, "---") {
		t.Errorf("should not contain --- without frontmatter, got:\n%s", content)
	}

	// Should have body
	if !strings.Contains(content, "Just body content") {
		t.Errorf("should contain body, got:\n%s", content)
	}
}

func TestSynthesizeMarkdown_NullFrontmatterValues(t *testing.T) {
	columns := []string{"id", "filename", "title", "author", "body"}
	values := []interface{}{"uuid-1", "hello", nil, "alice", "Body text\n"}
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title", "author"},
		PrimaryKey:  "id",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// NULL title should be omitted from frontmatter
	if strings.Contains(content, "title:") {
		t.Errorf("NULL title should be omitted, got:\n%s", content)
	}

	// Non-null author should be present
	if !strings.Contains(content, "author: alice") {
		t.Errorf("author should be present, got:\n%s", content)
	}
}

func TestSynthesizeMarkdown_AllNullFrontmatter(t *testing.T) {
	columns := []string{"id", "filename", "title", "body"}
	values := []interface{}{"uuid-1", "hello", nil, "Body\n"}
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title"},
		PrimaryKey:  "id",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// All frontmatter NULL → no delimiters
	if strings.Contains(content, "---") {
		t.Errorf("should not contain --- when all frontmatter is NULL, got:\n%s", content)
	}
}

func TestSynthesizeMarkdown_EmptyBody(t *testing.T) {
	columns := []string{"id", "filename", "title", "body"}
	values := []interface{}{"uuid-1", "hello", "Test", ""}
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title"},
		PrimaryKey:  "id",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// Should have frontmatter but no body
	if !strings.HasPrefix(content, "---\n") {
		t.Errorf("should have frontmatter, got:\n%s", content)
	}
	if !strings.Contains(content, "title: Test") {
		t.Errorf("should have title, got:\n%s", content)
	}
}

func TestSynthesizeMarkdown_NullBody(t *testing.T) {
	columns := []string{"id", "filename", "title", "body"}
	values := []interface{}{"uuid-1", "hello", "Test", nil}
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title"},
		PrimaryKey:  "id",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// Should have frontmatter but no body
	if !strings.Contains(content, "title: Test") {
		t.Errorf("should have title, got:\n%s", content)
	}
}

func TestSynthesizeMarkdown_ColumnCountMismatch(t *testing.T) {
	columns := []string{"id", "filename"}
	values := []interface{}{"uuid-1"}
	roles := &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"}

	_, err := SynthesizeMarkdown(columns, values, roles)
	if err == nil {
		t.Fatal("expected error for column/value count mismatch")
	}
}

func TestSynthesizeMarkdown_ArrayFrontmatter(t *testing.T) {
	columns := []string{"id", "filename", "tags", "body"}
	values := []interface{}{"uuid-1", "hello", []interface{}{"intro", "welcome"}, "Body\n"}
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"tags"},
		PrimaryKey:  "id",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// Should contain tags as YAML sequence
	if !strings.Contains(content, "tags:") {
		t.Errorf("should contain tags key, got:\n%s", content)
	}
	if !strings.Contains(content, "intro") || !strings.Contains(content, "welcome") {
		t.Errorf("should contain tag values, got:\n%s", content)
	}
}

func TestGetMarkdownFilename(t *testing.T) {
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
			values:   []interface{}{"uuid-1", "hello-world", "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "hello-world.md",
		},
		{
			name:     "filename already has .md",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"uuid-1", "hello.md", "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "hello.md",
		},
		{
			name:     "NULL filename falls back to PK",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"42", nil, "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "42.md",
		},
		{
			name:     "empty filename falls back to PK",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"42", "", "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "42.md",
		},
		{
			name:     "slash in filename sanitized",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"1", "path/to/file", "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "path-to-file.md",
		},
		{
			name:     "colon in filename sanitized",
			columns:  []string{"id", "filename", "body"},
			values:   []interface{}{"1", "file:name", "content"},
			roles:    &ColumnRoles{Filename: "filename", Body: "body", PrimaryKey: "id"},
			expected: "file-name.md",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetMarkdownFilename(tt.columns, tt.values, tt.roles)
			if got != tt.expected {
				t.Errorf("GetMarkdownFilename() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// --- Parse tests ---

func TestParseMarkdown_WithFrontmatter(t *testing.T) {
	content := "---\ntitle: Hello World\nauthor: alice\n---\n\n# Hello\n\nContent here.\n"

	parsed, err := ParseMarkdown([]byte(content))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Frontmatter["title"] != "Hello World" {
		t.Errorf("title = %v, want %q", parsed.Frontmatter["title"], "Hello World")
	}
	if parsed.Frontmatter["author"] != "alice" {
		t.Errorf("author = %v, want %q", parsed.Frontmatter["author"], "alice")
	}
	if !strings.Contains(parsed.Body, "# Hello") {
		t.Errorf("body should contain '# Hello', got:\n%s", parsed.Body)
	}
	if !strings.Contains(parsed.Body, "Content here.") {
		t.Errorf("body should contain 'Content here.', got:\n%s", parsed.Body)
	}
}

func TestParseMarkdown_NoFrontmatter(t *testing.T) {
	content := "# Just a heading\n\nSome text.\n"

	parsed, err := ParseMarkdown([]byte(content))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Frontmatter != nil {
		t.Errorf("frontmatter should be nil, got: %v", parsed.Frontmatter)
	}
	if parsed.Body != content {
		t.Errorf("body should be entire content, got:\n%s", parsed.Body)
	}
}

func TestParseMarkdown_EmptyContent(t *testing.T) {
	parsed, err := ParseMarkdown([]byte(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Frontmatter != nil {
		t.Errorf("frontmatter should be nil, got: %v", parsed.Frontmatter)
	}
	if parsed.Body != "" {
		t.Errorf("body should be empty, got: %q", parsed.Body)
	}
}

func TestParseMarkdown_FrontmatterOnly(t *testing.T) {
	content := "---\ntitle: Test\n---\n"

	parsed, err := ParseMarkdown([]byte(content))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Frontmatter["title"] != "Test" {
		t.Errorf("title = %v, want %q", parsed.Frontmatter["title"], "Test")
	}
}

func TestParseMarkdown_InvalidYAML(t *testing.T) {
	content := "---\n[invalid yaml\n---\n\nBody\n"

	_, err := ParseMarkdown([]byte(content))
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestParseMarkdown_ArrayInFrontmatter(t *testing.T) {
	content := "---\ntags:\n  - intro\n  - welcome\n---\n\nBody\n"

	parsed, err := ParseMarkdown([]byte(content))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tags, ok := parsed.Frontmatter["tags"].([]interface{})
	if !ok {
		t.Fatalf("tags should be []interface{}, got %T", parsed.Frontmatter["tags"])
	}
	if len(tags) != 2 || tags[0] != "intro" || tags[1] != "welcome" {
		t.Errorf("tags = %v, want [intro, welcome]", tags)
	}
}

func TestMapToColumns(t *testing.T) {
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title", "author", "date"},
		PrimaryKey:  "id",
	}

	parsed := &ParsedMarkdown{
		Frontmatter: map[string]interface{}{
			"title":  "Hello World",
			"author": "alice",
		},
		Body: "# Hello\n\nContent here.\n",
	}

	cols, err := MapToColumns(parsed, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cols["body"] != "# Hello\n\nContent here.\n" {
		t.Errorf("body = %q, want body content", cols["body"])
	}
	if cols["title"] != "Hello World" {
		t.Errorf("title = %v, want %q", cols["title"], "Hello World")
	}
	if cols["author"] != "alice" {
		t.Errorf("author = %v, want %q", cols["author"], "alice")
	}
}

func TestMapToColumns_UnknownKey(t *testing.T) {
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title"},
		PrimaryKey:  "id",
	}

	parsed := &ParsedMarkdown{
		Frontmatter: map[string]interface{}{
			"title":   "Hello",
			"unknown": "value",
		},
		Body: "Content\n",
	}

	_, err := MapToColumns(parsed, roles)
	if err == nil {
		t.Fatal("expected error for unknown frontmatter key")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Errorf("error should mention 'unknown', got: %v", err)
	}
}

func TestMapToColumns_NoFrontmatter(t *testing.T) {
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title"},
		PrimaryKey:  "id",
	}

	parsed := &ParsedMarkdown{Body: "Just body\n"}

	cols, err := MapToColumns(parsed, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cols["body"] != "Just body\n" {
		t.Errorf("body = %q, want %q", cols["body"], "Just body\n")
	}
	if _, ok := cols["title"]; ok {
		t.Errorf("title should not be in result when not in frontmatter")
	}
}

// --- Extra headers tests (TestSynth_ prefix) ---

func TestSynth_SynthesizeMarkdown_WithExtraHeaders(t *testing.T) {
	columns := []string{"id", "filename", "title", "author", "headers", "body"}
	values := []interface{}{
		"uuid-1", "hello-world", "Hello World", "alice",
		map[string]interface{}{"draft": false, "category": "blog", "tags": []interface{}{"sql", "beginner"}},
		"# Hello\n",
	}
	roles := &ColumnRoles{
		Filename:     "filename",
		Body:         "body",
		Frontmatter:  []string{"title", "author"},
		PrimaryKey:   "id",
		ExtraHeaders: "headers",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// Known columns first
	if !strings.Contains(content, "title: Hello World") {
		t.Errorf("should contain title, got:\n%s", content)
	}
	if !strings.Contains(content, "author: alice") {
		t.Errorf("should contain author, got:\n%s", content)
	}

	// Extra headers in alphabetical order: category, draft, tags
	if !strings.Contains(content, "category: blog") {
		t.Errorf("should contain category extra header, got:\n%s", content)
	}
	if !strings.Contains(content, "draft: false") {
		t.Errorf("should contain draft extra header, got:\n%s", content)
	}
	if !strings.Contains(content, "tags:") {
		t.Errorf("should contain tags extra header, got:\n%s", content)
	}

	// Known columns should appear before extra headers
	titleIdx := strings.Index(content, "title:")
	categoryIdx := strings.Index(content, "category:")
	if titleIdx > categoryIdx {
		t.Errorf("known column 'title' should appear before extra header 'category'")
	}
}

func TestSynth_SynthesizeMarkdown_ExtraHeadersEmpty(t *testing.T) {
	columns := []string{"id", "filename", "title", "headers", "body"}
	values := []interface{}{"uuid-1", "hello", "Hello", map[string]interface{}{}, "Body\n"}
	roles := &ColumnRoles{
		Filename:     "filename",
		Body:         "body",
		Frontmatter:  []string{"title"},
		PrimaryKey:   "id",
		ExtraHeaders: "headers",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// Should have title but no extra header keys
	if !strings.Contains(content, "title: Hello") {
		t.Errorf("should contain title, got:\n%s", content)
	}
	// Empty headers map should not add any extra lines
	lines := strings.Split(strings.TrimSpace(content), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "headers:") {
			t.Errorf("empty headers should not appear in frontmatter, got:\n%s", content)
		}
	}
}

func TestSynth_SynthesizeMarkdown_ExtraHeadersNil(t *testing.T) {
	columns := []string{"id", "filename", "title", "headers", "body"}
	values := []interface{}{"uuid-1", "hello", "Hello", nil, "Body\n"}
	roles := &ColumnRoles{
		Filename:     "filename",
		Body:         "body",
		Frontmatter:  []string{"title"},
		PrimaryKey:   "id",
		ExtraHeaders: "headers",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// NULL headers column should behave like empty
	if !strings.Contains(content, "title: Hello") {
		t.Errorf("should contain title, got:\n%s", content)
	}
}

func TestSynth_SynthesizeMarkdown_OnlyExtraHeaders(t *testing.T) {
	columns := []string{"id", "filename", "headers", "body"}
	values := []interface{}{
		"uuid-1", "hello",
		map[string]interface{}{"draft": true, "category": "notes"},
		"Body\n",
	}
	roles := &ColumnRoles{
		Filename:     "filename",
		Body:         "body",
		Frontmatter:  nil, // no known frontmatter columns
		PrimaryKey:   "id",
		ExtraHeaders: "headers",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)

	// Should still have --- delimiters
	if !strings.HasPrefix(content, "---\n") {
		t.Errorf("should have frontmatter delimiters, got:\n%s", content)
	}
	if !strings.Contains(content, "category: notes") {
		t.Errorf("should contain category, got:\n%s", content)
	}
	if !strings.Contains(content, "draft: true") {
		t.Errorf("should contain draft, got:\n%s", content)
	}
}

func TestSynth_MapToColumns_UnknownKeysIntoExtraHeaders(t *testing.T) {
	roles := &ColumnRoles{
		Filename:     "filename",
		Body:         "body",
		Frontmatter:  []string{"title", "author"},
		PrimaryKey:   "id",
		ExtraHeaders: "headers",
	}

	parsed := &ParsedMarkdown{
		Frontmatter: map[string]interface{}{
			"title":    "Hello",
			"author":   "alice",
			"category": "blog",
			"draft":    false,
		},
		Body: "Content\n",
	}

	cols, err := MapToColumns(parsed, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cols["title"] != "Hello" {
		t.Errorf("title = %v, want %q", cols["title"], "Hello")
	}
	if cols["author"] != "alice" {
		t.Errorf("author = %v, want %q", cols["author"], "alice")
	}

	headers, ok := cols["headers"].(map[string]interface{})
	if !ok {
		t.Fatalf("headers should be map[string]interface{}, got %T", cols["headers"])
	}
	if headers["category"] != "blog" {
		t.Errorf("headers[category] = %v, want %q", headers["category"], "blog")
	}
	if headers["draft"] != false {
		t.Errorf("headers[draft] = %v, want false", headers["draft"])
	}
}

func TestSynth_MapToColumns_NoExtraHeaders_UnknownKeyRejected(t *testing.T) {
	roles := &ColumnRoles{
		Filename:    "filename",
		Body:        "body",
		Frontmatter: []string{"title"},
		PrimaryKey:  "id",
		// ExtraHeaders intentionally empty
	}

	parsed := &ParsedMarkdown{
		Frontmatter: map[string]interface{}{
			"title":   "Hello",
			"unknown": "value",
		},
		Body: "Content\n",
	}

	_, err := MapToColumns(parsed, roles)
	if err == nil {
		t.Fatal("expected error for unknown frontmatter key without extra headers column")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Errorf("error should mention 'unknown', got: %v", err)
	}
}

func TestSynth_MapToColumns_ExtraHeadersDeletion(t *testing.T) {
	roles := &ColumnRoles{
		Filename:     "filename",
		Body:         "body",
		Frontmatter:  []string{"title"},
		PrimaryKey:   "id",
		ExtraHeaders: "headers",
	}

	// User removed the "draft" key — only "title" remains in frontmatter
	parsed := &ParsedMarkdown{
		Frontmatter: map[string]interface{}{
			"title": "Hello",
		},
		Body: "Content\n",
	}

	cols, err := MapToColumns(parsed, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	headers, ok := cols["headers"].(map[string]interface{})
	if !ok {
		t.Fatalf("headers should be map[string]interface{}, got %T", cols["headers"])
	}
	// Extra headers should be empty (previously existing keys are removed)
	if len(headers) != 0 {
		t.Errorf("headers should be empty after removing extra keys, got: %v", headers)
	}
}

func TestSynth_ExtraHeadersRoundTrip(t *testing.T) {
	// Step 1: Synthesize markdown with extra headers
	columns := []string{"id", "filename", "title", "headers", "body"}
	values := []interface{}{
		"uuid-1", "hello",
		"Hello World",
		map[string]interface{}{"draft": true, "category": "blog"},
		"# Hello\n",
	}
	roles := &ColumnRoles{
		Filename:     "filename",
		Body:         "body",
		Frontmatter:  []string{"title"},
		PrimaryKey:   "id",
		ExtraHeaders: "headers",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("synthesize error: %v", err)
	}

	// Step 2: Parse the synthesized markdown
	parsed, err := ParseMarkdown(data)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	// Step 3: Map back to columns
	cols, err := MapToColumns(parsed, roles)
	if err != nil {
		t.Fatalf("map error: %v", err)
	}

	// Step 4: Verify known columns survived
	if cols["title"] != "Hello World" {
		t.Errorf("title = %v, want %q", cols["title"], "Hello World")
	}
	if cols["body"] != "# Hello\n" {
		t.Errorf("body = %q, want %q", cols["body"], "# Hello\n")
	}

	// Step 5: Verify extra headers survived
	headers, ok := cols["headers"].(map[string]interface{})
	if !ok {
		t.Fatalf("headers should be map[string]interface{}, got %T", cols["headers"])
	}
	if headers["draft"] != true {
		t.Errorf("headers[draft] = %v, want true", headers["draft"])
	}
	if headers["category"] != "blog" {
		t.Errorf("headers[category] = %v, want %q", headers["category"], "blog")
	}
}

func TestSynthesizeMarkdown_TrailingNewline(t *testing.T) {
	columns := []string{"id", "filename", "body"}
	values := []interface{}{"uuid-1", "hello", "No trailing newline"}
	roles := &ColumnRoles{
		Filename:   "filename",
		Body:       "body",
		PrimaryKey: "id",
	}

	data, err := SynthesizeMarkdown(columns, values, roles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	content := string(data)
	if !strings.HasSuffix(content, "\n") {
		t.Errorf("should end with newline, got: %q", content)
	}
}
