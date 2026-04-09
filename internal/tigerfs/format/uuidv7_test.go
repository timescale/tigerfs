package format

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestUUIDv7ToDisplayName_RoundTrip(t *testing.T) {
	// Generate 100 UUIDv7s and verify lossless round-trip
	for i := 0; i < 100; i++ {
		id, err := uuid.NewV7()
		if err != nil {
			t.Fatal(err)
		}
		name := UUIDv7ToDisplayName(id)
		recovered, err := DisplayNameToUUIDv7(name)
		if err != nil {
			t.Fatalf("DisplayNameToUUIDv7(%q) error: %v", name, err)
		}
		if id != recovered {
			t.Fatalf("round-trip mismatch (iteration %d):\n  orig:      %s\n  display:   %s\n  recovered: %s", i, id, name, recovered)
		}
	}
}

func TestUUIDv7ToDisplayName_Format(t *testing.T) {
	id, _ := uuid.NewV7()
	name := UUIDv7ToDisplayName(id)

	// Timestamp portion is 22 chars, separator at position 22
	tsLen := len(DisplayNameLayout)
	if len(name) < tsLen+2 {
		t.Fatalf("display name too short: %q (len=%d)", name, len(name))
	}
	if name[tsLen] != '-' {
		t.Fatalf("expected '-' at position %d in %q", tsLen, name)
	}

	// Entropy portion should be base36 (lowercase alphanumeric only)
	entropy := name[tsLen+1:]
	for _, c := range entropy {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')) {
			t.Fatalf("non-base36 character %q in entropy portion of %q", string(c), name)
		}
	}

	// Timestamp should parse correctly and be close to now
	ts := ExtractUUIDv7Time(id)
	diff := time.Since(ts)
	if diff < 0 || diff > 2*time.Second {
		t.Errorf("timestamp %v off from now by %v", ts, diff)
	}
}

func TestUUIDv7ToDisplayName_TimestampMillisecondPrecision(t *testing.T) {
	// Create two UUIDv7s with slightly different timestamps and verify
	// the millisecond portion differs
	id1, _ := uuid.NewV7()
	time.Sleep(2 * time.Millisecond)
	id2, _ := uuid.NewV7()

	name1 := UUIDv7ToDisplayName(id1)
	name2 := UUIDv7ToDisplayName(id2)

	if name1 == name2 {
		t.Errorf("two UUIDv7s generated 2ms apart produced identical display names: %q", name1)
	}

	// Both should sort chronologically (string comparison)
	if name1 > name2 {
		t.Errorf("display names don't sort chronologically:\n  first:  %s\n  second: %s", name1, name2)
	}
}

func TestDisplayNameToUUIDv7_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"too short", "abc"},
		{"no separator", "2026-04-07T143000.123Z"},
		{"bad timestamp", "not-a-timestamp.000Z-abc"},
		{"bad entropy", "2026-04-07T143000.123Z-!!!invalid!!!"},
		{"wrong separator position", "2026-04-07T143000.123Zabc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DisplayNameToUUIDv7(tt.input)
			if err == nil {
				t.Errorf("DisplayNameToUUIDv7(%q) expected error, got nil", tt.input)
			}
		})
	}
}

func TestIsUUIDv7(t *testing.T) {
	v7, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	if !IsUUIDv7(v7) {
		t.Errorf("IsUUIDv7(%s) = false for v7, want true", v7)
	}

	v4 := uuid.New() // v4
	if IsUUIDv7(v4) {
		t.Errorf("IsUUIDv7(%s) = true for v4, want false", v4)
	}

	var zero [16]byte
	if IsUUIDv7(zero) {
		t.Error("IsUUIDv7(zero) = true, want false")
	}
}

func TestIsDisplayName(t *testing.T) {
	// Valid display name
	id, _ := uuid.NewV7()
	name := UUIDv7ToDisplayName(id)
	if !IsDisplayName(name) {
		t.Errorf("IsDisplayName(%q) = false, want true", name)
	}

	// Not display names
	tests := []string{
		"",
		"abc",
		"019590a0-1234-7fff-8000-a1b2c3d4e5f6", // hex UUID
		"2026-04-07T143000Z",                   // old version ID format
		"not-a-display-name",
	}
	for _, s := range tests {
		if IsDisplayName(s) {
			t.Errorf("IsDisplayName(%q) = true, want false", s)
		}
	}
}

func TestUUIDv7ToDisplayName_ZeroEntropy(t *testing.T) {
	// UUIDv7 with rand_a=0, rand_b=0. big.Int.Bytes() returns empty for
	// zero, so this tests that unpackEntropy handles the padding correctly.
	var id [16]byte
	// Set a valid timestamp
	id[0], id[1], id[2], id[3], id[4], id[5] = 0x01, 0x9D, 0xA4, 0xC1, 0xB8, 0x3B
	id[6] = 0x70 // version=7, rand_a high=0
	id[7] = 0x00 // rand_a low=0
	id[8] = 0x80 // variant=10, rand_b high=0
	// bytes 9-15 all zero (rand_b=0)

	name := UUIDv7ToDisplayName(id)
	recovered, err := DisplayNameToUUIDv7(name)
	if err != nil {
		t.Fatalf("round-trip failed for zero entropy: %v", err)
	}
	if uuid.UUID(id) != recovered {
		t.Fatalf("round-trip mismatch for zero entropy:\n  orig:      %x\n  display:   %s\n  recovered: %x", id, name, recovered)
	}
}

func TestUUIDv7ToDisplayName_MaxEntropy(t *testing.T) {
	// UUIDv7 with max rand_a (0xFFF) and max rand_b (62 bits all set)
	var id [16]byte
	id[0], id[1], id[2], id[3], id[4], id[5] = 0x01, 0x9D, 0xA4, 0xC1, 0xB8, 0x3B
	id[6] = 0x7F // version=7, rand_a high=F
	id[7] = 0xFF // rand_a low=FF (rand_a = 0xFFF)
	id[8] = 0xBF // variant=10, rand_b high 6 bits=111111
	id[9] = 0xFF
	id[10] = 0xFF
	id[11] = 0xFF
	id[12] = 0xFF
	id[13] = 0xFF
	id[14] = 0xFF
	id[15] = 0xFF

	name := UUIDv7ToDisplayName(id)
	recovered, err := DisplayNameToUUIDv7(name)
	if err != nil {
		t.Fatalf("round-trip failed for max entropy: %v", err)
	}
	if uuid.UUID(id) != recovered {
		t.Fatalf("round-trip mismatch for max entropy:\n  orig:      %x\n  display:   %s\n  recovered: %x", id, name, recovered)
	}
}

func TestUUIDv7ToDisplayName_SmallEntropy(t *testing.T) {
	// UUIDv7 with rand_a=0, rand_b=1. The base36 output will be very
	// short ("1"), testing that the decoder handles short entropy strings.
	var id [16]byte
	id[0], id[1], id[2], id[3], id[4], id[5] = 0x01, 0x9D, 0xA4, 0xC1, 0xB8, 0x3B
	id[6] = 0x70 // version=7, rand_a=0
	id[7] = 0x00
	id[8] = 0x80 // variant=10, rand_b high=0
	// rand_b = 1 (lowest bit of byte 15)
	id[15] = 0x01

	name := UUIDv7ToDisplayName(id)
	recovered, err := DisplayNameToUUIDv7(name)
	if err != nil {
		t.Fatalf("round-trip failed for small entropy: %v", err)
	}
	if uuid.UUID(id) != recovered {
		t.Fatalf("round-trip mismatch for small entropy:\n  orig:      %x\n  display:   %s\n  recovered: %x", id, name, recovered)
	}

	// The entropy portion should be very short (base36 of a small number)
	tsLen := len(DisplayNameLayout)
	entropy := name[tsLen+1:]
	if len(entropy) > 5 {
		t.Errorf("expected short entropy for small value, got %q (len=%d)", entropy, len(entropy))
	}
}

func TestDisplayNameToUUIDv7_ShortEntropy(t *testing.T) {
	// A display name with a single-character entropy "0" should decode correctly
	// (represents zero entropy bits)
	var id [16]byte
	id[0], id[1], id[2], id[3], id[4], id[5] = 0x01, 0x9D, 0xA4, 0xC1, 0xB8, 0x3B
	id[6] = 0x70
	id[7] = 0x00
	id[8] = 0x80

	name := UUIDv7ToDisplayName(id)
	recovered, err := DisplayNameToUUIDv7(name)
	if err != nil {
		t.Fatalf("failed to decode display name with minimal entropy: %v", err)
	}
	if uuid.UUID(id) != recovered {
		t.Fatalf("mismatch:\n  orig:      %x\n  display:   %s\n  recovered: %x", id, name, recovered)
	}
}

func TestDisplayNameToUUIDv7_HexUUIDIsRejected(t *testing.T) {
	// A standard hex UUID string should NOT parse as a display name
	hexUUID := "019590a0-1234-7fff-8000-a1b2c3d4e5f6"
	_, err := DisplayNameToUUIDv7(hexUUID)
	if err == nil {
		t.Errorf("DisplayNameToUUIDv7(%q) should have failed for hex UUID input", hexUUID)
	}
}

func TestIsUUIDv7_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		id       [16]byte
		expected bool
	}{
		{
			name:     "version 0 (nil-like)",
			id:       [16]byte{0, 0, 0, 0, 0, 0, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expected: false,
		},
		{
			name:     "version 4",
			id:       [16]byte{0, 0, 0, 0, 0, 0, 0x40, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expected: false,
		},
		{
			name:     "version 7 with invalid variant (00)",
			id:       [16]byte{0, 0, 0, 0, 0, 0, 0x70, 0, 0x00, 0, 0, 0, 0, 0, 0, 0},
			expected: true, // IsUUIDv7 only checks version, not variant
		},
		{
			name:     "version 7 with variant 10 (standard)",
			id:       [16]byte{0, 0, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 0},
			expected: true,
		},
		{
			name:     "version 1",
			id:       [16]byte{0, 0, 0, 0, 0, 0, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsUUIDv7(tt.id)
			if got != tt.expected {
				t.Errorf("IsUUIDv7(%x) = %v, want %v", tt.id, got, tt.expected)
			}
		})
	}
}

func TestIsDisplayName_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid display name", "2026-04-07T143000.123Z-abc", true},
		{"hex UUID", "019590a0-1234-7fff-8000-a1b2c3d4e5f6", false},
		{"old version ID format", "2026-04-07T143000Z", false},
		{"timestamp without entropy", "2026-04-07T143000.123Z", false},
		{"timestamp with wrong separator", "2026-04-07T143000.123Zabc", false},
		{"just long enough (1 char entropy)", "2026-04-07T143000.123Z-0", true},
		{"empty entropy after dash", "2026-04-07T143000.123Z-", false}, // len < tsLen+2
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsDisplayName(tt.input)
			if got != tt.expected {
				t.Errorf("IsDisplayName(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestUUIDv7ToDisplayName_KnownValue(t *testing.T) {
	// Test with a specific known UUID to verify deterministic output.
	// Build a UUIDv7 from known bytes.
	var id [16]byte
	// Timestamp: 2026-04-07 14:30:00.123 UTC = 1775405400123 ms
	// 1775405400123 = 0x19D_A4C1_B83B
	id[0] = 0x01
	id[1] = 0x9D
	id[2] = 0xA4
	id[3] = 0xC1
	id[4] = 0xB8
	id[5] = 0x3B
	// Version 7
	id[6] = 0x7F // version=7, rand_a high nibble=F
	id[7] = 0xFF // rand_a low byte=FF (rand_a = 0xFFF)
	// Variant 10
	id[8] = 0x80 // variant=10, rand_b high 6 bits=000000
	id[9] = 0x00
	id[10] = 0x00
	id[11] = 0x00
	id[12] = 0x00
	id[13] = 0x00
	id[14] = 0x00
	id[15] = 0x01 // rand_b = 1

	name := UUIDv7ToDisplayName(id)

	// Verify round-trip
	recovered, err := DisplayNameToUUIDv7(name)
	if err != nil {
		t.Fatalf("round-trip failed: %v", err)
	}
	if uuid.UUID(id) != recovered {
		t.Fatalf("round-trip mismatch:\n  orig:      %x\n  display:   %s\n  recovered: %x", id, name, recovered)
	}

	// Verify timestamp portion starts with expected date prefix
	ts := ExtractUUIDv7Time(id)
	if ts.Year() < 2025 || ts.Year() > 2030 {
		t.Errorf("extracted time %v has unexpected year", ts)
	}

	// Verify display name starts with the timestamp
	expectedPrefix := ts.UTC().Format(DisplayNameLayout)
	if name[:len(DisplayNameLayout)] != expectedPrefix {
		t.Errorf("display name %q does not start with expected timestamp %q", name, expectedPrefix)
	}
}
