package integration

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestCommandCases_Read runs all read-only command tests.
// These tests don't modify data and can run in any order.
func TestCommandCases_Read(t *testing.T) {
	// Setup database with demo data
	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Seed demo data
	ctx := context.Background()
	cfg := DefaultDemoConfig()
	if err := seedDemoData(ctx, dbResult.ConnStr, cfg); err != nil {
		t.Fatalf("Failed to seed demo data: %v", err)
	}

	// Mount filesystem
	fsCfg := commandTestConfig()
	mountpoint := t.TempDir()
	fs := mountWithTimeout(t, fsCfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if fs == nil {
		return
	}
	defer func() { _ = fs.Close() }()
	time.Sleep(500 * time.Millisecond)

	t.Logf("Running %d read test cases", len(ReadTestCases))

	// Run all read tests
	for _, tc := range ReadTestCases {
		tc := tc // capture
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Skip != "" {
				t.Skip(tc.Skip)
			}
			runTestCase(t, mountpoint, tc)
		})
	}
}

// TestCommandCases_Write runs write command tests.
// These tests modify data and stop on first failure.
func TestCommandCases_Write(t *testing.T) {
	// Setup database with demo data
	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Seed demo data
	ctx := context.Background()
	cfg := DefaultDemoConfig()
	if err := seedDemoData(ctx, dbResult.ConnStr, cfg); err != nil {
		t.Fatalf("Failed to seed demo data: %v", err)
	}

	// Mount filesystem
	fsCfg := commandTestConfig()
	mountpoint := t.TempDir()
	fs := mountWithTimeout(t, fsCfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if fs == nil {
		return
	}
	defer func() { _ = fs.Close() }()
	time.Sleep(500 * time.Millisecond)

	t.Logf("Running %d write test cases (stops on first failure)", len(WriteTestCases))

	// Run write tests - stop on first failure
	for _, tc := range WriteTestCases {
		tc := tc // capture
		success := t.Run(tc.Name, func(t *testing.T) {
			if tc.Skip != "" {
				t.Skip(tc.Skip)
			}
			runTestCase(t, mountpoint, tc)
		})
		if !success {
			t.Fatalf("Write test %q failed - stopping remaining write tests", tc.Name)
		}
	}
}

// TestCommandCases_DDL runs DDL command tests.
func TestCommandCases_DDL(t *testing.T) {

	// Setup database with demo data
	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		return
	}
	defer dbResult.Cleanup()

	// Seed demo data
	ctx := context.Background()
	cfg := DefaultDemoConfig()
	if err := seedDemoData(ctx, dbResult.ConnStr, cfg); err != nil {
		t.Fatalf("Failed to seed demo data: %v", err)
	}

	// Mount filesystem
	fsCfg := commandTestConfig()
	mountpoint := t.TempDir()
	fs := mountWithTimeout(t, fsCfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if fs == nil {
		return
	}
	defer func() { _ = fs.Close() }()
	time.Sleep(500 * time.Millisecond)

	t.Logf("Running %d DDL test cases (stops on first failure)", len(DDLTestCases))

	// Run DDL tests - stop on first failure
	for _, tc := range DDLTestCases {
		tc := tc // capture
		success := t.Run(tc.Name, func(t *testing.T) {
			if tc.Skip != "" {
				t.Skip(tc.Skip)
			}
			runTestCase(t, mountpoint, tc)
		})
		if !success {
			t.Fatalf("DDL test %q failed - stopping remaining DDL tests", tc.Name)
		}
	}
}

// runTestCase executes a single test case.
func runTestCase(t *testing.T, mountpoint string, tc CommandTestCase) {
	t.Helper()

	output, err := executeCommand(t, mountpoint, tc.Input)
	verifyOutput(t, output, err, tc.Expected)
}

// executeCommand runs a filesystem command and returns the output.
func executeCommand(t *testing.T, mountpoint string, cmd CommandInput) (string, error) {
	t.Helper()

	fullPath := filepath.Join(mountpoint, cmd.Path)

	switch cmd.Op {
	case "ls":
		entries, err := os.ReadDir(fullPath)
		if err != nil {
			return "", err
		}
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		// Always sort for consistency across FUSE/NFS
		sort.Strings(names)
		return strings.Join(names, "\n"), nil

	case "cat":
		data, err := os.ReadFile(fullPath)
		if err != nil {
			return "", err
		}
		return strings.TrimSuffix(string(data), "\n"), nil

	case "echo":
		content := cmd.Content
		if !strings.HasSuffix(content, "\n") {
			content += "\n"
		}
		err := os.WriteFile(fullPath, []byte(content), 0644)
		return "", err

	case "mkdir":
		err := os.MkdirAll(fullPath, 0755)
		return "", err

	case "touch":
		f, err := os.Create(fullPath)
		if err == nil {
			f.Close()
		}
		return "", err

	case "rm":
		if containsArg(cmd.Args, "-r") {
			return "", os.RemoveAll(fullPath)
		}
		return "", os.Remove(fullPath)

	default:
		t.Fatalf("Unknown command: %s", cmd.Op)
		return "", nil
	}
}

// containsArg checks if args contains a specific argument.
func containsArg(args []string, arg string) bool {
	for _, a := range args {
		if a == arg {
			return true
		}
	}
	return false
}

// commandTestConfig returns a config suitable for integration tests.
func commandTestConfig() *config.Config {
	return &config.Config{
		PoolSize:                5,
		PoolMaxIdle:             2,
		DefaultSchema:           "public",
		DirListingLimit:         10000,
		AttrTimeout:             1 * time.Second,
		EntryTimeout:            1 * time.Second,
		MetadataRefreshInterval: 30 * time.Second,
		LogLevel:                "warn",
	}
}
