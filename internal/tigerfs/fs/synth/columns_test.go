package synth

import (
	"testing"
)

func TestDetectColumnRoles_Markdown(t *testing.T) {
	tests := []struct {
		name      string
		columns   []string
		pk        string
		wantFile  string
		wantBody  string
		wantFront []string
		wantModAt string
		wantCreAt string
		wantErr   bool
	}{
		{
			name:      "standard columns",
			columns:   []string{"id", "filename", "title", "author", "body", "created_at"},
			pk:        "id",
			wantFile:  "filename",
			wantBody:  "body",
			wantFront: []string{"title", "author"},
			wantCreAt: "created_at",
		},
		{
			name:      "slug and content conventions",
			columns:   []string{"id", "slug", "content", "date", "tags"},
			pk:        "id",
			wantFile:  "slug",
			wantBody:  "content",
			wantFront: []string{"date", "tags"},
		},
		{
			name:      "name and description conventions",
			columns:   []string{"id", "name", "category", "description"},
			pk:        "id",
			wantFile:  "name",
			wantBody:  "description",
			wantFront: []string{"category"},
		},
		{
			name:      "title and text conventions",
			columns:   []string{"id", "title", "text", "status"},
			pk:        "id",
			wantFile:  "title",
			wantBody:  "text",
			wantFront: []string{"status"},
		},
		{
			name:      "priority: filename beats name/title/slug",
			columns:   []string{"id", "name", "filename", "body", "extra"},
			pk:        "id",
			wantFile:  "filename",
			wantBody:  "body",
			wantFront: []string{"name", "extra"},
		},
		{
			name:      "priority: body beats content/description/text",
			columns:   []string{"id", "filename", "content", "body", "extra"},
			pk:        "id",
			wantFile:  "filename",
			wantBody:  "body",
			wantFront: []string{"content", "extra"},
		},
		{
			name:      "pk excluded from frontmatter",
			columns:   []string{"id", "filename", "body"},
			pk:        "id",
			wantFile:  "filename",
			wantBody:  "body",
			wantFront: nil, // no frontmatter columns remain
		},
		{
			name:      "modified_at detected and excluded from frontmatter",
			columns:   []string{"id", "filename", "body", "modified_at", "author"},
			pk:        "id",
			wantFile:  "filename",
			wantBody:  "body",
			wantFront: []string{"author"},
			wantModAt: "modified_at",
		},
		{
			name:      "updated_at detected as modified_at convention",
			columns:   []string{"id", "filename", "body", "updated_at"},
			pk:        "id",
			wantFile:  "filename",
			wantBody:  "body",
			wantFront: nil,
			wantModAt: "updated_at",
		},
		{
			name:      "both modified_at and created_at detected",
			columns:   []string{"id", "filename", "body", "modified_at", "created_at", "author"},
			pk:        "id",
			wantFile:  "filename",
			wantBody:  "body",
			wantFront: []string{"author"},
			wantModAt: "modified_at",
			wantCreAt: "created_at",
		},
		{
			name:      "no timestamp columns",
			columns:   []string{"id", "filename", "body", "author"},
			pk:        "id",
			wantFile:  "filename",
			wantBody:  "body",
			wantFront: []string{"author"},
		},
		{
			name:    "no filename column",
			columns: []string{"id", "data", "body"},
			pk:      "id",
			wantErr: true,
		},
		{
			name:    "no body column",
			columns: []string{"id", "filename", "data"},
			pk:      "id",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roles, err := DetectColumnRoles(tt.columns, FormatMarkdown, tt.pk)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if roles.Filename != tt.wantFile {
				t.Errorf("Filename = %q, want %q", roles.Filename, tt.wantFile)
			}
			if roles.Body != tt.wantBody {
				t.Errorf("Body = %q, want %q", roles.Body, tt.wantBody)
			}
			if roles.PrimaryKey != tt.pk {
				t.Errorf("PrimaryKey = %q, want %q", roles.PrimaryKey, tt.pk)
			}
			if roles.ModifiedAt != tt.wantModAt {
				t.Errorf("ModifiedAt = %q, want %q", roles.ModifiedAt, tt.wantModAt)
			}
			if roles.CreatedAt != tt.wantCreAt {
				t.Errorf("CreatedAt = %q, want %q", roles.CreatedAt, tt.wantCreAt)
			}
			if len(roles.Frontmatter) != len(tt.wantFront) {
				t.Fatalf("Frontmatter = %v, want %v", roles.Frontmatter, tt.wantFront)
			}
			for i, got := range roles.Frontmatter {
				if got != tt.wantFront[i] {
					t.Errorf("Frontmatter[%d] = %q, want %q", i, got, tt.wantFront[i])
				}
			}
		})
	}
}

func TestDetectColumnRoles_PlainText(t *testing.T) {
	roles, err := DetectColumnRoles(
		[]string{"id", "filename", "body"},
		FormatPlainText,
		"id",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if roles.Filename != "filename" {
		t.Errorf("Filename = %q, want %q", roles.Filename, "filename")
	}
	if roles.Body != "body" {
		t.Errorf("Body = %q, want %q", roles.Body, "body")
	}
	// PlainText should have no frontmatter
	if len(roles.Frontmatter) != 0 {
		t.Errorf("Frontmatter = %v, want empty", roles.Frontmatter)
	}
}

func TestDetectColumnRoles_Native(t *testing.T) {
	_, err := DetectColumnRoles([]string{"id", "data"}, FormatNative, "id")
	if err == nil {
		t.Fatal("expected error for FormatNative, got nil")
	}
}

func TestDetectColumnRoles_Tasks(t *testing.T) {
	_, err := DetectColumnRoles(
		[]string{"number", "name", "status", "body"},
		FormatTasks,
		"id",
	)
	if err == nil {
		t.Fatal("expected error for FormatTasks (not yet implemented), got nil")
	}
}

func TestDetectColumnRoles_CaseInsensitive(t *testing.T) {
	roles, err := DetectColumnRoles(
		[]string{"ID", "Filename", "Body", "Author"},
		FormatMarkdown,
		"ID",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if roles.Filename != "Filename" {
		t.Errorf("Filename = %q, want %q (preserves original case)", roles.Filename, "Filename")
	}
	if roles.Body != "Body" {
		t.Errorf("Body = %q, want %q (preserves original case)", roles.Body, "Body")
	}
}
