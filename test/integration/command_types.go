package integration

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CommandTestCase represents a single input/output test.
type CommandTestCase struct {
	Name     string         // Test name for t.Run()
	Category string         // Grouping: "read", "write", "ddl", etc.
	Input    CommandInput   // The operation to perform
	Expected ExpectedOutput // What we expect
	Skip     string         // Skip reason if not empty
}

// CommandInput describes a filesystem operation to perform.
type CommandInput struct {
	Op      string   // "ls", "cat", "echo", "mkdir", "touch", "rm"
	Path    string   // Filesystem path (relative to mountpoint)
	Content string   // For echo/write operations
	Args    []string // Additional args (e.g., "-r" for rm)
}

// ExpectedOutput describes what we expect from a command.
type ExpectedOutput struct {
	// For exact match (after normalization)
	Exact string

	// For flexible matching
	Contains    []string // Output must contain all these
	NotContains []string // Output must not contain these
	LineCount   int      // Expected number of lines (-1 to skip check)

	// For directory listings (always sorted before comparison)
	Entries    []string // Expected entry names
	EntryCount int      // Expected count (-1 to skip check)

	// For JSON output
	JSONFields map[string]any // Expected JSON fields (partial match)
	JSONArray  bool           // Expect JSON array
	JSONLength int            // Expected array length (-1 to skip check)

	// For error cases
	Error     bool   // Expect an error
	ErrorType string // "ENOENT", "EACCES", "EIO", etc.

	// Normalization options
	TrimWhitespace bool // Trim whitespace from output (default behavior)
}

// verifyOutput checks if the actual output matches expectations.
func verifyOutput(t *testing.T, output string, err error, expected ExpectedOutput) {
	t.Helper()

	// Handle error expectations
	if expected.Error {
		require.Error(t, err, "Expected an error but got none")
		if expected.ErrorType != "" {
			assert.Contains(t, err.Error(), expected.ErrorType,
				"Error should contain %s", expected.ErrorType)
		}
		return
	}
	require.NoError(t, err)

	// Default: trim whitespace
	output = strings.TrimSpace(output)

	// Check exact match
	if expected.Exact != "" {
		assert.Equal(t, expected.Exact, output)
		return
	}

	// Check contains
	for _, s := range expected.Contains {
		assert.Contains(t, output, s, "Output should contain %q", s)
	}
	for _, s := range expected.NotContains {
		assert.NotContains(t, output, s, "Output should not contain %q", s)
	}

	// Check line count (only when > 0; use Entries for exact matching)
	if expected.LineCount > 0 {
		lines := strings.Split(output, "\n")
		if output == "" {
			lines = []string{}
		}
		assert.Len(t, lines, expected.LineCount, "Line count mismatch")
	}

	// Check entries (directory listing) - always sorted
	if len(expected.Entries) > 0 {
		actualEntries := strings.Split(output, "\n")
		if output == "" {
			actualEntries = []string{}
		}
		sort.Strings(actualEntries)
		expectedSorted := make([]string, len(expected.Entries))
		copy(expectedSorted, expected.Entries)
		sort.Strings(expectedSorted)
		assert.Equal(t, expectedSorted, actualEntries, "Directory entries mismatch")
	}

	// Check entry count (only when > 0)
	if expected.EntryCount > 0 {
		actualEntries := strings.Split(output, "\n")
		if output == "" {
			actualEntries = []string{}
		}
		assert.Len(t, actualEntries, expected.EntryCount, "Entry count mismatch")
	}

	// Check JSON array
	if expected.JSONArray {
		var arr []map[string]any
		require.NoError(t, json.Unmarshal([]byte(output), &arr), "Expected valid JSON array")
		if expected.JSONLength > 0 {
			assert.Len(t, arr, expected.JSONLength, "JSON array length mismatch")
		}
	}

	// Check JSON fields
	if len(expected.JSONFields) > 0 {
		var obj map[string]any
		require.NoError(t, json.Unmarshal([]byte(output), &obj), "Expected valid JSON object")
		for k, v := range expected.JSONFields {
			assert.Equal(t, v, obj[k], "JSON field %q mismatch", k)
		}
	}
}

// normalizeFloats normalizes float representations (11.50 -> 11.5).
func normalizeFloats(s string) string {
	// Remove trailing zeros after decimal point
	re := regexp.MustCompile(`(\d+\.\d*?)0+([,\s\n\]}]|$)`)
	return re.ReplaceAllString(s, "${1}${2}")
}

// sortLines splits output by newlines, sorts, and rejoins.
func sortLines(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}
