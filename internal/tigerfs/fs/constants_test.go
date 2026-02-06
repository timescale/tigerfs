// Package fs tests for constants.go
package fs

import (
	"strings"
	"testing"
)

// TestCapabilityDirectoryConstants verifies capability directory names start with dot.
func TestCapabilityDirectoryConstants(t *testing.T) {
	dirs := []string{
		DirInfo, DirBy, DirFirst, DirLast, DirSample, DirAll,
		DirOrder, DirExport, DirImport, DirFilter,
	}
	for _, dir := range dirs {
		if !strings.HasPrefix(dir, ".") {
			t.Errorf("capability directory %q should start with '.'", dir)
		}
	}
}

// TestFormatExtensions verifies format extensions start with dot.
func TestFormatExtensions(t *testing.T) {
	exts := []string{ExtJSON, ExtCSV, ExtTSV, ExtYAML}
	for _, ext := range exts {
		if !strings.HasPrefix(ext, ".") {
			t.Errorf("extension %q should start with '.'", ext)
		}
	}
}

// TestFormatNames verifies format names don't have dot.
func TestFormatNames(t *testing.T) {
	names := []string{FmtJSON, FmtCSV, FmtTSV, FmtYAML}
	for _, name := range names {
		if strings.HasPrefix(name, ".") {
			t.Errorf("format name %q should not start with '.'", name)
		}
	}
}

// TestIsCapabilityDirectory verifies the lookup function.
func TestIsCapabilityDirectory(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{DirBy, true},
		{DirFilter, true},
		{DirFirst, true},
		{DirExport, true},
		{"users", false},
		{"123", false},
		{".hidden", false}, // not a capability
	}

	for _, tt := range tests {
		if got := IsCapabilityDirectory(tt.name); got != tt.want {
			t.Errorf("IsCapabilityDirectory(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestDDLControlFiles verifies DDL file constants.
func TestDDLControlFiles(t *testing.T) {
	// Content files should not start with dot (visible)
	if strings.HasPrefix(FileSQL, ".") {
		t.Errorf("FileSQL %q should not start with '.' (content file)", FileSQL)
	}
	if strings.HasPrefix(FileTestLog, ".") {
		t.Errorf("FileTestLog %q should not start with '.' (content file)", FileTestLog)
	}

	// Trigger files should start with dot (hidden)
	if !strings.HasPrefix(FileTest, ".") {
		t.Errorf("FileTest %q should start with '.' (trigger file)", FileTest)
	}
	if !strings.HasPrefix(FileCommit, ".") {
		t.Errorf("FileCommit %q should start with '.' (trigger file)", FileCommit)
	}
	if !strings.HasPrefix(FileAbort, ".") {
		t.Errorf("FileAbort %q should start with '.' (trigger file)", FileAbort)
	}
}
