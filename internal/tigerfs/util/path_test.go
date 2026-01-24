package util

import "testing"

func TestParseRowFilename(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedPK     string
		expectedFormat string
	}{
		{
			name:           "no extension",
			input:          "123",
			expectedPK:     "123",
			expectedFormat: "tsv",
		},
		{
			name:           "csv extension",
			input:          "123.csv",
			expectedPK:     "123",
			expectedFormat: "csv",
		},
		{
			name:           "json extension",
			input:          "123.json",
			expectedPK:     "123",
			expectedFormat: "json",
		},
		{
			name:           "tsv extension explicit",
			input:          "123.tsv",
			expectedPK:     "123",
			expectedFormat: "tsv",
		},
		{
			name:           "email as pk",
			input:          "foo@example.com",
			expectedPK:     "foo@example.com",
			expectedFormat: "tsv",
		},
		{
			name:           "email with extension",
			input:          "foo@example.com.csv",
			expectedPK:     "foo@example.com",
			expectedFormat: "csv",
		},
		{
			name:           "uuid",
			input:          "550e8400-e29b-41d4-a716-446655440000",
			expectedPK:     "550e8400-e29b-41d4-a716-446655440000",
			expectedFormat: "tsv",
		},
		{
			name:           "uuid with extension",
			input:          "550e8400-e29b-41d4-a716-446655440000.json",
			expectedPK:     "550e8400-e29b-41d4-a716-446655440000",
			expectedFormat: "json",
		},
		{
			name:           "string with dots",
			input:          "file.name.txt",
			expectedPK:     "file.name.txt",
			expectedFormat: "tsv", // .txt is not recognized, kept as part of PK
		},
		{
			name:           "unknown extension",
			input:          "123.unknown",
			expectedPK:     "123.unknown",
			expectedFormat: "tsv", // .unknown not recognized, kept as part of PK
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkValue, format := ParseRowFilename(tt.input)

			if pkValue != tt.expectedPK {
				t.Errorf("ParseRowFilename(%q) pkValue = %q, expected %q", tt.input, pkValue, tt.expectedPK)
			}

			if format != tt.expectedFormat {
				t.Errorf("ParseRowFilename(%q) format = %q, expected %q", tt.input, format, tt.expectedFormat)
			}
		})
	}
}
