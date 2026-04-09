package db

import (
	"testing"

	"github.com/google/uuid"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
)

func TestSinglePKMatch(t *testing.T) {
	m := SinglePKMatch("id", "42")
	if len(m.Columns) != 1 || m.Columns[0] != "id" {
		t.Errorf("Columns = %v, want [id]", m.Columns)
	}
	if len(m.Values) != 1 || m.Values[0] != "42" {
		t.Errorf("Values = %v, want [42]", m.Values)
	}
}

func TestPKMatch_WhereClause(t *testing.T) {
	tests := []struct {
		name       string
		columns    []string
		values     []string
		startParam int
		want       string
	}{
		{
			name:       "single column",
			columns:    []string{"id"},
			values:     []string{"1"},
			startParam: 1,
			want:       `"id" = $1`,
		},
		{
			name:       "two columns",
			columns:    []string{"customer_id", "product_id"},
			values:     []string{"5", "42"},
			startParam: 1,
			want:       `"customer_id" = $1 AND "product_id" = $2`,
		},
		{
			name:       "three columns starting at $3",
			columns:    []string{"a", "b", "c"},
			values:     []string{"1", "2", "3"},
			startParam: 3,
			want:       `"a" = $3 AND "b" = $4 AND "c" = $5`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &PKMatch{Columns: tt.columns, Values: tt.values}
			got := m.WhereClause(tt.startParam)
			if got != tt.want {
				t.Errorf("WhereClause(%d) = %q, want %q", tt.startParam, got, tt.want)
			}
		})
	}
}

func TestPKMatch_WhereArgs(t *testing.T) {
	m := &PKMatch{
		Columns: []string{"a", "b"},
		Values:  []string{"hello", "world"},
	}
	args := m.WhereArgs()
	if len(args) != 2 {
		t.Fatalf("WhereArgs() len = %d, want 2", len(args))
	}
	if args[0] != "hello" || args[1] != "world" {
		t.Errorf("WhereArgs() = %v, want [hello world]", args)
	}
}

func TestPKMatch_ParamCount(t *testing.T) {
	m := &PKMatch{Columns: []string{"a", "b", "c"}, Values: []string{"1", "2", "3"}}
	if m.ParamCount() != 3 {
		t.Errorf("ParamCount() = %d, want 3", m.ParamCount())
	}
}

func TestPrimaryKey_Encode_SingleColumn(t *testing.T) {
	pk := &PrimaryKey{Columns: []string{"id"}}

	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{"integer", []string{"123"}, "123"},
		{"uuid", []string{"550e8400-e29b-41d4-a716-446655440000"}, "550e8400-e29b-41d4-a716-446655440000"},
		{"text", []string{"hello"}, "hello"},
		{"text with comma", []string{"hello,world"}, "hello,world"},         // NOT encoded for single-col
		{"text with percent", []string{"100%done"}, "100%done"},             // NOT encoded for single-col
		{"text with slash", []string{"path/to/thing"}, "path%2Fto%2Fthing"}, // slash IS encoded
		{"empty string", []string{""}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pk.Encode(tt.values)
			if got != tt.want {
				t.Errorf("Encode(%v) = %q, want %q", tt.values, got, tt.want)
			}
		})
	}
}

func TestPrimaryKey_Encode_Composite(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
		values  []string
		want    string
	}{
		{
			name:    "two integers",
			columns: []string{"customer_id", "product_id"},
			values:  []string{"5", "42"},
			want:    "5,42",
		},
		{
			name:    "three columns",
			columns: []string{"a", "b", "c"},
			values:  []string{"1", "2", "3"},
			want:    "1,2,3",
		},
		{
			name:    "value with comma",
			columns: []string{"name", "seq"},
			values:  []string{"hello,world", "1"},
			want:    "hello%2Cworld,1",
		},
		{
			name:    "value with percent",
			columns: []string{"name", "seq"},
			values:  []string{"100%done", "2"},
			want:    "100%25done,2",
		},
		{
			name:    "value with slash",
			columns: []string{"path", "version"},
			values:  []string{"a/b", "1"},
			want:    "a%2Fb,1",
		},
		{
			name:    "int and uuid",
			columns: []string{"user_id", "session_id"},
			values:  []string{"42", "550e8400-e29b-41d4-a716-446655440000"},
			want:    "42,550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:    "empty values",
			columns: []string{"a", "b"},
			values:  []string{"", ""},
			want:    ",",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pk := &PrimaryKey{Columns: tt.columns}
			got := pk.Encode(tt.values)
			if got != tt.want {
				t.Errorf("Encode(%v) = %q, want %q", tt.values, got, tt.want)
			}
		})
	}
}

func TestPrimaryKey_Decode_SingleColumn(t *testing.T) {
	pk := &PrimaryKey{Columns: []string{"id"}}

	tests := []struct {
		name    string
		dirname string
		want    string
	}{
		{"integer", "123", "123"},
		{"uuid", "550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000"},
		{"text", "hello", "hello"},
		{"text with comma", "hello,world", "hello,world"},   // commas NOT decoded for single-col
		{"text with percent", "100%done", "100%done"},       // unknown escapes pass through
		{"text with encoded slash", "path%2Fto", "path/to"}, // slash IS decoded
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := pk.Decode(tt.dirname)
			if err != nil {
				t.Fatalf("Decode(%q) error: %v", tt.dirname, err)
			}
			if len(match.Values) != 1 || match.Values[0] != tt.want {
				t.Errorf("Decode(%q).Values = %v, want [%q]", tt.dirname, match.Values, tt.want)
			}
			if match.Columns[0] != "id" {
				t.Errorf("Decode(%q).Columns = %v, want [id]", tt.dirname, match.Columns)
			}
		})
	}
}

func TestPrimaryKey_Decode_Composite(t *testing.T) {
	pk := &PrimaryKey{Columns: []string{"customer_id", "product_id"}}

	tests := []struct {
		name    string
		dirname string
		want    []string
	}{
		{"two integers", "5,42", []string{"5", "42"}},
		{"with encoded comma", "hello%2Cworld,42", []string{"hello,world", "42"}},
		{"with encoded percent", "100%25done,2", []string{"100%done", "2"}},
		{"with encoded slash", "a%2Fb,1", []string{"a/b", "1"}},
		{"empty values", ",", []string{"", ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := pk.Decode(tt.dirname)
			if err != nil {
				t.Fatalf("Decode(%q) error: %v", tt.dirname, err)
			}
			if len(match.Values) != len(tt.want) {
				t.Fatalf("Decode(%q).Values len = %d, want %d", tt.dirname, len(match.Values), len(tt.want))
			}
			for i, v := range match.Values {
				if v != tt.want[i] {
					t.Errorf("Decode(%q).Values[%d] = %q, want %q", tt.dirname, i, v, tt.want[i])
				}
			}
		})
	}
}

func TestPrimaryKey_Decode_Errors(t *testing.T) {
	pk := &PrimaryKey{Columns: []string{"a", "b"}}

	tests := []struct {
		name    string
		dirname string
	}{
		{"too few parts", "only_one"},
		{"too many parts", "1,2,3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pk.Decode(tt.dirname)
			if err == nil {
				t.Errorf("Decode(%q) expected error, got nil", tt.dirname)
			}
		})
	}
}

func TestPrimaryKey_RoundTrip_SingleColumn(t *testing.T) {
	pk := &PrimaryKey{Columns: []string{"id"}}
	values := []string{"hello"}

	encoded := pk.Encode(values)
	match, err := pk.Decode(encoded)
	if err != nil {
		t.Fatalf("round-trip error: %v", err)
	}
	if match.Values[0] != values[0] {
		t.Errorf("round-trip: encoded=%q, decoded=%q, want %q", encoded, match.Values[0], values[0])
	}
}

func TestPrimaryKey_ThreeColumnPK(t *testing.T) {
	pk := &PrimaryKey{Columns: []string{"a", "b", "c"}}

	t.Run("encode three values", func(t *testing.T) {
		got := pk.Encode([]string{"x", "y", "z"})
		if got != "x,y,z" {
			t.Errorf("Encode([x,y,z]) = %q, want %q", got, "x,y,z")
		}
	})

	t.Run("decode three values", func(t *testing.T) {
		match, err := pk.Decode("x,y,z")
		if err != nil {
			t.Fatalf("Decode(x,y,z) error: %v", err)
		}
		want := []string{"x", "y", "z"}
		for i, v := range match.Values {
			if v != want[i] {
				t.Errorf("Values[%d] = %q, want %q", i, v, want[i])
			}
		}
		if len(match.Columns) != 3 || match.Columns[0] != "a" || match.Columns[1] != "b" || match.Columns[2] != "c" {
			t.Errorf("Columns = %v, want [a b c]", match.Columns)
		}
	})

	t.Run("where clause", func(t *testing.T) {
		match := &PKMatch{Columns: []string{"a", "b", "c"}, Values: []string{"x", "y", "z"}}
		got := match.WhereClause(1)
		want := `"a" = $1 AND "b" = $2 AND "c" = $3`
		if got != want {
			t.Errorf("WhereClause(1) = %q, want %q", got, want)
		}
	})

	t.Run("round-trip with special chars in all values", func(t *testing.T) {
		values := []string{"hello,world", "100%done", "path/to"}
		encoded := pk.Encode(values)
		match, err := pk.Decode(encoded)
		if err != nil {
			t.Fatalf("round-trip error for %v (encoded=%q): %v", values, encoded, err)
		}
		for i, v := range match.Values {
			if v != values[i] {
				t.Errorf("round-trip[%d]: got %q, want %q", i, v, values[i])
			}
		}
	})

	t.Run("decode wrong part count", func(t *testing.T) {
		_, err := pk.Decode("only,two")
		if err == nil {
			t.Error("Decode(only,two) expected error for 3-column PK, got nil")
		}
		_, err = pk.Decode("a,b,c,d")
		if err == nil {
			t.Error("Decode(a,b,c,d) expected error for 3-column PK, got nil")
		}
	})
}

// TestPrimaryKey_TimestampPK verifies that RFC3339 timestamps with colons,
// plus signs, and timezone offsets encode and decode correctly in composite PKs.
// This is the format used by TimescaleDB hypertable PKs.
func TestPrimaryKey_TimestampPK(t *testing.T) {
	pk := &PrimaryKey{Columns: []string{"time", "product_id", "user_id"}}

	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{
			name:   "UTC timestamp",
			values: []string{"2024-01-15T10:00:00Z", "1", "1"},
			want:   "2024-01-15T10:00:00Z,1,1",
		},
		{
			name:   "negative timezone offset",
			values: []string{"2024-01-15T05:00:00-05:00", "1", "1"},
			want:   "2024-01-15T05:00:00-05:00,1,1",
		},
		{
			name:   "positive timezone offset",
			values: []string{"2024-01-15T15:00:00+05:00", "2", "3"},
			want:   "2024-01-15T15:00:00+05:00,2,3",
		},
		{
			name:   "nanosecond precision",
			values: []string{"2024-01-15T10:00:00.123456789Z", "1", "42"},
			want:   "2024-01-15T10:00:00.123456789Z,1,42",
		},
		{
			name:   "timestamp with spaces (non-RFC3339)",
			values: []string{"2024-01-15 10:00:00+00", "5", "10"},
			want:   "2024-01-15 10:00:00+00,5,10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encode
			encoded := pk.Encode(tt.values)
			if encoded != tt.want {
				t.Errorf("Encode(%v) = %q, want %q", tt.values, encoded, tt.want)
			}

			// Test round-trip
			match, err := pk.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode(%q) error: %v", encoded, err)
			}
			for i, v := range match.Values {
				if v != tt.values[i] {
					t.Errorf("round-trip[%d]: got %q, want %q", i, v, tt.values[i])
				}
			}
			if match.Columns[0] != "time" || match.Columns[1] != "product_id" || match.Columns[2] != "user_id" {
				t.Errorf("Columns = %v, want [time product_id user_id]", match.Columns)
			}
		})
	}
}

func TestPrimaryKey_RoundTrip_Composite(t *testing.T) {
	pk := &PrimaryKey{Columns: []string{"a", "b", "c"}}

	testCases := [][]string{
		{"1", "2", "3"},
		{"hello,world", "foo%bar", "a/b"},
		{"", "", ""},
		{"uuid-1234", "42", "text value"},
	}

	for _, values := range testCases {
		encoded := pk.Encode(values)
		match, err := pk.Decode(encoded)
		if err != nil {
			t.Fatalf("round-trip error for %v (encoded=%q): %v", values, encoded, err)
		}
		for i, v := range match.Values {
			if v != values[i] {
				t.Errorf("round-trip[%d]: encoded=%q, got %q, want %q", i, encoded, v, values[i])
			}
		}
	}
}

func TestPKDecode_UUIDv7DisplayName(t *testing.T) {
	// Generate a UUIDv7, convert to display name, then decode via pkDecodeSingle.
	// The result should be the standard hex UUID string (for DB queries).
	v7, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	displayName := format.UUIDv7ToDisplayName(v7)

	pk := &PrimaryKey{Columns: []string{"id"}}
	match, err := pk.Decode(displayName)
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", displayName, err)
	}

	expectedHex := v7.String()
	if match.Values[0] != expectedHex {
		t.Errorf("Decode(%q) = %q, want hex UUID %q", displayName, match.Values[0], expectedHex)
	}
}

func TestPKDecode_HexUUID_Unchanged(t *testing.T) {
	// A standard hex UUID (v4 or v7) should pass through Decode unchanged.
	hexUUID := "019590a0-1234-7fff-8000-a1b2c3d4e5f6"
	pk := &PrimaryKey{Columns: []string{"id"}}
	match, err := pk.Decode(hexUUID)
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", hexUUID, err)
	}
	if match.Values[0] != hexUUID {
		t.Errorf("Decode(%q) = %q, want unchanged", hexUUID, match.Values[0])
	}
}

func TestPKDecode_NonUUID_Unchanged(t *testing.T) {
	// A regular string PK value (not a UUID) should pass through unchanged.
	pk := &PrimaryKey{Columns: []string{"name"}}
	match, err := pk.Decode("hello-world")
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", "hello-world", err)
	}
	if match.Values[0] != "hello-world" {
		t.Errorf("Decode(%q) = %q, want unchanged", "hello-world", match.Values[0])
	}
}

func TestPKEncodeDecode_UUIDv7_RoundTrip(t *testing.T) {
	// Full round-trip: UUIDv7 bytes -> scanAndEncodePK (display name) ->
	// Decode (back to hex UUID). The hex UUID should match the original.
	v7, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate what scanAndEncodePK does for a UUIDv7
	bytes := [16]byte(v7)
	if !format.IsUUIDv7(bytes) {
		t.Fatal("generated UUID is not v7")
	}
	displayName := format.UUIDv7ToDisplayName(bytes)

	// Decode back (simulating path resolution)
	pk := &PrimaryKey{Columns: []string{"id"}}
	match, err := pk.Decode(displayName)
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", displayName, err)
	}

	expectedHex := v7.String()
	if match.Values[0] != expectedHex {
		t.Errorf("round-trip: display=%q decoded=%q want=%q", displayName, match.Values[0], expectedHex)
	}
}

func TestPKEncodeDecode_UUIDv4_RoundTrip(t *testing.T) {
	// UUIDv4 should NOT get display name encoding -- it stays as hex
	// through the entire encode/decode cycle.
	v4 := uuid.New()
	hexStr := v4.String()

	// pkDecodeSingle should return hex unchanged (IsDisplayName returns false)
	pk := &PrimaryKey{Columns: []string{"id"}}
	match, err := pk.Decode(hexStr)
	if err != nil {
		t.Fatalf("Decode(%q) error: %v", hexStr, err)
	}
	if match.Values[0] != hexStr {
		t.Errorf("v4 UUID decode: got %q, want %q (unchanged)", match.Values[0], hexStr)
	}
}

func TestScanAndEncodePK_UUIDv7(t *testing.T) {
	// scanAndEncodePK should produce display name format for UUIDv7
	v7, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	values := []interface{}{[16]byte(v7)}
	encoded, err := scanAndEncodePK(values, []string{"id"})
	if err != nil {
		t.Fatalf("scanAndEncodePK error: %v", err)
	}
	if !format.IsDisplayName(encoded) {
		t.Errorf("scanAndEncodePK for UUIDv7 produced non-display-name: %q", encoded)
	}

	// Verify round-trip back to the same UUID
	recovered, err := format.DisplayNameToUUIDv7(encoded)
	if err != nil {
		t.Fatalf("DisplayNameToUUIDv7(%q) error: %v", encoded, err)
	}
	if v7 != recovered {
		t.Errorf("round-trip mismatch: orig=%s encoded=%q recovered=%s", v7, encoded, recovered)
	}
}

func TestScanAndEncodePK_UUIDv4(t *testing.T) {
	// scanAndEncodePK should produce standard hex for UUIDv4
	v4 := uuid.New()
	values := []interface{}{[16]byte(v4)}
	encoded, err := scanAndEncodePK(values, []string{"id"})
	if err != nil {
		t.Fatalf("scanAndEncodePK error: %v", err)
	}
	if format.IsDisplayName(encoded) {
		t.Errorf("scanAndEncodePK for UUIDv4 produced display name: %q (should be hex)", encoded)
	}
	if encoded != v4.String() {
		t.Errorf("scanAndEncodePK for UUIDv4: got %q, want %q", encoded, v4.String())
	}
}

func TestScanAndEncodePK_NonUUID(t *testing.T) {
	// Non-UUID PK values should pass through unchanged
	values := []interface{}{"hello"}
	encoded, err := scanAndEncodePK(values, []string{"name"})
	if err != nil {
		t.Fatalf("scanAndEncodePK error: %v", err)
	}
	if encoded != "hello" {
		t.Errorf("scanAndEncodePK for string: got %q, want %q", encoded, "hello")
	}
}
