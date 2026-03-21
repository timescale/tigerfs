package db

import "testing"

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple identifier",
			input: "id",
			want:  `"id"`,
		},
		{
			name:  "identifier with underscore",
			input: "user_name",
			want:  `"user_name"`,
		},
		{
			name:  "identifier with embedded double quote",
			input: `my"col`,
			want:  `"my""col"`,
		},
		{
			name:  "identifier with multiple embedded quotes",
			input: `a"b"c`,
			want:  `"a""b""c"`,
		},
		{
			name:  "identifier with space",
			input: "my col",
			want:  `"my col"`,
		},
		{
			name:  "empty string",
			input: "",
			want:  `""`,
		},
		{
			name:  "identifier with semicolon (SQL injection attempt)",
			input: `id"; DROP TABLE users;--`,
			want:  `"id""; DROP TABLE users;--"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuoteIdent(tt.input)
			if got != tt.want {
				t.Errorf("QuoteIdent(%q) = %q, want %q", tt.input, got, tt.want)
			}
			// Verify qi() alias produces same result
			if qi(tt.input) != got {
				t.Errorf("qi(%q) != QuoteIdent(%q)", tt.input, tt.input)
			}
		})
	}
}

func TestQuoteTable(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		table  string
		want   string
	}{
		{
			name:   "simple schema and table",
			schema: "public",
			table:  "users",
			want:   `"public"."users"`,
		},
		{
			name:   "embedded quote in table",
			schema: "public",
			table:  `my"table`,
			want:   `"public"."my""table"`,
		},
		{
			name:   "embedded quote in schema",
			schema: `my"schema`,
			table:  "users",
			want:   `"my""schema"."users"`,
		},
		{
			name:   "embedded quotes in both",
			schema: `my"schema`,
			table:  `my"table`,
			want:   `"my""schema"."my""table"`,
		},
		{
			name:   "SQL injection in schema",
			schema: `public"; DROP TABLE users;--`,
			table:  "users",
			want:   `"public""; DROP TABLE users;--"."users"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuoteTable(tt.schema, tt.table)
			if got != tt.want {
				t.Errorf("QuoteTable(%q, %q) = %q, want %q", tt.schema, tt.table, got, tt.want)
			}
			// Verify qt() alias produces same result
			if qt(tt.schema, tt.table) != got {
				t.Errorf("qt(%q, %q) != QuoteTable(%q, %q)", tt.schema, tt.table, tt.schema, tt.table)
			}
		})
	}
}
