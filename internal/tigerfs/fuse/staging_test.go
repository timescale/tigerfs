package fuse

import (
	"testing"
	"time"
)

func TestStagingTracker_GetOrCreate(t *testing.T) {
	tracker := NewStagingTracker()

	// First call creates entry
	entry1 := tracker.GetOrCreate("test/path")
	if entry1 == nil {
		t.Fatal("GetOrCreate returned nil")
	}
	if entry1.Content != "" {
		t.Errorf("Expected empty content, got %q", entry1.Content)
	}
	if entry1.CreatedAt.IsZero() {
		t.Error("Expected CreatedAt to be set")
	}

	// Second call returns same entry
	entry2 := tracker.GetOrCreate("test/path")
	if entry1 != entry2 {
		t.Error("Expected same entry on second call")
	}

	// Different path creates different entry
	entry3 := tracker.GetOrCreate("other/path")
	if entry1 == entry3 {
		t.Error("Expected different entry for different path")
	}
}

func TestStagingTracker_Get(t *testing.T) {
	tracker := NewStagingTracker()

	// Non-existent returns nil
	if tracker.Get("nonexistent") != nil {
		t.Error("Expected nil for non-existent path")
	}

	// After GetOrCreate, Get returns the entry
	tracker.GetOrCreate("test/path")
	entry := tracker.Get("test/path")
	if entry == nil {
		t.Error("Expected entry after GetOrCreate")
	}
}

func TestStagingTracker_Set(t *testing.T) {
	tracker := NewStagingTracker()

	// Set creates entry if needed
	tracker.Set("test/path", "CREATE TABLE foo (id int);")

	entry := tracker.Get("test/path")
	if entry == nil {
		t.Fatal("Expected entry after Set")
	}
	if entry.Content != "CREATE TABLE foo (id int);" {
		t.Errorf("Expected content to be set, got %q", entry.Content)
	}

	// Set updates existing entry
	tracker.Set("test/path", "DROP TABLE foo;")
	entry = tracker.Get("test/path")
	if entry.Content != "DROP TABLE foo;" {
		t.Errorf("Expected updated content, got %q", entry.Content)
	}
}

func TestStagingTracker_Delete(t *testing.T) {
	tracker := NewStagingTracker()

	tracker.Set("test/path", "content")
	if tracker.Get("test/path") == nil {
		t.Fatal("Expected entry to exist")
	}

	tracker.Delete("test/path")
	if tracker.Get("test/path") != nil {
		t.Error("Expected entry to be deleted")
	}

	// Delete non-existent path doesn't panic
	tracker.Delete("nonexistent")
}

func TestStagingTracker_HasContent(t *testing.T) {
	tracker := NewStagingTracker()

	// Non-existent path
	if tracker.HasContent("nonexistent") {
		t.Error("Expected HasContent to return false for non-existent path")
	}

	// Empty content
	tracker.Set("empty", "")
	if tracker.HasContent("empty") {
		t.Error("Expected HasContent to return false for empty content")
	}

	// Only comments
	tracker.Set("comments", "-- just a comment\n/* block comment */")
	if tracker.HasContent("comments") {
		t.Error("Expected HasContent to return false for comment-only content")
	}

	// Actual SQL
	tracker.Set("sql", "CREATE TABLE foo (id int);")
	if !tracker.HasContent("sql") {
		t.Error("Expected HasContent to return true for SQL content")
	}
}

func TestStagingTracker_SetTestResult(t *testing.T) {
	tracker := NewStagingTracker()

	// Create entry first
	tracker.GetOrCreate("test/path")

	tracker.SetTestResult("test/path", "OK: DDL validated successfully.\n")

	result := tracker.GetTestResult("test/path")
	if result != "OK: DDL validated successfully.\n" {
		t.Errorf("Expected test result, got %q", result)
	}

	// SetTestResult on non-existent path is a no-op
	tracker.SetTestResult("nonexistent", "result")
	if tracker.GetTestResult("nonexistent") != "" {
		t.Error("Expected empty result for non-existent path")
	}
}

func TestStagingTracker_ListPending(t *testing.T) {
	tracker := NewStagingTracker()

	// Empty tracker
	if len(tracker.ListPending(DirCreate)) != 0 {
		t.Error("Expected empty list for empty tracker")
	}

	// Add some entries
	tracker.Set(".create/orders", "CREATE TABLE orders...")
	tracker.Set(".create/users", "CREATE TABLE users...")
	tracker.Set(".modify/products", "ALTER TABLE products...")

	// List .create entries
	createEntries := tracker.ListPending(DirCreate)
	if len(createEntries) != 2 {
		t.Errorf("Expected 2 create entries, got %d", len(createEntries))
	}

	// Verify entries contain expected names
	found := make(map[string]bool)
	for _, name := range createEntries {
		found[name] = true
	}
	if !found["orders"] || !found["users"] {
		t.Errorf("Expected orders and users, got %v", createEntries)
	}

	// List .modify entries
	modifyEntries := tracker.ListPending(DirModify)
	if len(modifyEntries) != 1 || modifyEntries[0] != "products" {
		t.Errorf("Expected [products], got %v", modifyEntries)
	}
}

func TestStagingTracker_Concurrency(t *testing.T) {
	tracker := NewStagingTracker()

	// Run concurrent operations
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(n int) {
			path := "path/" + string(rune('a'+n))
			tracker.Set(path, "content")
			tracker.Get(path)
			tracker.HasContent(path)
			tracker.Delete(path)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for goroutines")
		}
	}
}

func TestIsEmptyOrCommented(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected bool
	}{
		{"empty string", "", true},
		{"whitespace only", "   \n\t\n  ", true},
		{"single line comment", "-- this is a comment", true},
		{"multiple line comments", "-- comment 1\n-- comment 2", true},
		{"block comment", "/* this is a block comment */", true},
		{"mixed comments", "-- line\n/* block */\n-- another", true},
		{"simple SQL", "SELECT 1;", false},
		{"SQL with leading comment", "-- comment\nSELECT 1;", false},
		{"SQL with trailing comment", "SELECT 1; -- comment", false},
		{"CREATE TABLE", "CREATE TABLE foo (id int);", false},
		{"CREATE TABLE with comments", "-- Create table\nCREATE TABLE foo (\n  -- id column\n  id int\n);", false},
		{"commented out SQL", "-- CREATE TABLE foo (id int);", true},
		{"block comment around SQL", "/* CREATE TABLE foo (id int); */", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsEmptyOrCommented(tt.content)
			if result != tt.expected {
				t.Errorf("IsEmptyOrCommented(%q) = %v, want %v", tt.content, result, tt.expected)
			}
		})
	}
}

func TestExtractSQL(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected string
	}{
		{"no comments", "SELECT 1;", "SELECT 1;"},
		{"leading comment", "-- comment\nSELECT 1;", "SELECT 1;"},
		{"trailing comment", "SELECT 1; -- comment", "SELECT 1;"},
		{"block comment", "SELECT /* inline */ 1;", "SELECT  1;"},
		{"multiline with comments", "-- header\nCREATE TABLE foo (\n  -- id column\n  id int\n);", "CREATE TABLE foo (\n\n  id int\n);"},
		{"empty after stripping", "-- just comments", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractSQL(tt.content)
			if result != tt.expected {
				t.Errorf("ExtractSQL(%q) = %q, want %q", tt.content, result, tt.expected)
			}
		})
	}
}
