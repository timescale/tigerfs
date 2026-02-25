package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// TestBuildRootCmd tests that the root command is built correctly
func TestBuildRootCmd(t *testing.T) {
	ctx := context.Background()
	cmd := buildRootCmd(ctx)

	if cmd == nil {
		t.Fatal("buildRootCmd returned nil")
	}

	if cmd.Use != "tigerfs" {
		t.Errorf("Expected Use='tigerfs', got %q", cmd.Use)
	}

	if !strings.Contains(cmd.Short, "TigerFS") {
		t.Errorf("Expected Short to contain 'TigerFS', got %q", cmd.Short)
	}

	if !strings.Contains(cmd.Long, "PostgreSQL") {
		t.Errorf("Expected Long to contain 'PostgreSQL', got %q", cmd.Long)
	}
}

// TestBuildRootCmd_HasSubcommands tests that all subcommands are present
func TestBuildRootCmd_HasSubcommands(t *testing.T) {
	ctx := context.Background()
	cmd := buildRootCmd(ctx)

	expectedSubcommands := []string{
		"mount",
		"unmount",
		"status",
		"list",
		"test-connection",
		"version",
		"config",
	}

	subcommandNames := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		subcommandNames[sub.Name()] = true
	}

	for _, expected := range expectedSubcommands {
		if !subcommandNames[expected] {
			t.Errorf("Expected subcommand %q not found", expected)
		}
	}
}

// TestBuildRootCmd_PersistentFlags tests that persistent flags are registered
func TestBuildRootCmd_PersistentFlags(t *testing.T) {
	ctx := context.Background()
	cmd := buildRootCmd(ctx)

	expectedFlags := []string{
		"config-dir",
		"debug",
		"log-level",
		"log-file",
		"log-format",
	}

	for _, flagName := range expectedFlags {
		flag := cmd.PersistentFlags().Lookup(flagName)
		if flag == nil {
			t.Errorf("Expected persistent flag %q not found", flagName)
		}
	}
}

// TestBuildVersionCmd tests that the version command is built correctly
func TestBuildVersionCmd(t *testing.T) {
	cmd := buildVersionCmd()

	if cmd == nil {
		t.Fatal("buildVersionCmd returned nil")
	}

	if cmd.Use != "version" {
		t.Errorf("Expected Use='version', got %q", cmd.Use)
	}

	if cmd.Short == "" {
		t.Error("Expected Short to be non-empty")
	}
}

// TestBuildVersionCmd_Run tests that the version command runs without error
func TestBuildVersionCmd_Run(t *testing.T) {
	cmd := buildVersionCmd()

	// Note: The version command uses fmt.Printf which writes to stdout,
	// not to cmd.OutOrStdout(). We just verify it runs without error.
	err := cmd.Execute()
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}
}

// TestBuildMountCmd tests that the mount command is built correctly
func TestBuildMountCmd(t *testing.T) {
	ctx := context.Background()
	cmd := buildMountCmd(ctx)

	if cmd == nil {
		t.Fatal("buildMountCmd returned nil")
	}

	if cmd.Use != "mount [CONNECTION] [MOUNTPOINT]" {
		t.Errorf("Expected Use='mount [CONNECTION] [MOUNTPOINT]', got %q", cmd.Use)
	}
}

// TestBuildMountCmd_Flags tests that mount command flags are registered
func TestBuildMountCmd_Flags(t *testing.T) {
	ctx := context.Background()
	cmd := buildMountCmd(ctx)

	expectedFlags := []struct {
		name      string
		shorthand string
	}{
		{"schema", ""},
		{"read-only", ""},
		{"max-ls-rows", ""},
		{"foreground", ""},
		{"no-filename-extensions", ""},
		{"query-timeout", ""},
		{"dir-filter-limit", ""},
		{"legacy-fuse", ""},
	}

	for _, expected := range expectedFlags {
		flag := cmd.Flags().Lookup(expected.name)
		if flag == nil {
			t.Errorf("Expected flag %q not found", expected.name)
			continue
		}

		if expected.shorthand != "" && flag.Shorthand != expected.shorthand {
			t.Errorf("Expected flag %q shorthand=%q, got %q", expected.name, expected.shorthand, flag.Shorthand)
		}
	}
}

// TestBuildMountCmd_FlagDefaults tests mount command flag defaults
func TestBuildMountCmd_FlagDefaults(t *testing.T) {
	ctx := context.Background()
	cmd := buildMountCmd(ctx)

	testCases := []struct {
		name     string
		expected string
	}{
		{"max-ls-rows", "10000"},
		{"read-only", "false"},
		{"foreground", "false"},
		{"legacy-fuse", "false"},
	}

	for _, tc := range testCases {
		flag := cmd.Flags().Lookup(tc.name)
		if flag == nil {
			t.Errorf("Flag %q not found", tc.name)
			continue
		}

		if flag.DefValue != tc.expected {
			t.Errorf("Flag %q default: expected %q, got %q", tc.name, tc.expected, flag.DefValue)
		}
	}
}

// TestBuildUnmountCmd tests that the unmount command is built correctly
func TestBuildUnmountCmd(t *testing.T) {
	cmd := buildUnmountCmd()

	if cmd == nil {
		t.Fatal("buildUnmountCmd returned nil")
	}

	if cmd.Use != "unmount MOUNTPOINT" {
		t.Errorf("Expected Use='unmount MOUNTPOINT', got %q", cmd.Use)
	}
}

// TestBuildStatusCmd tests that the status command is built correctly
func TestBuildStatusCmd(t *testing.T) {
	cmd := buildStatusCmd()

	if cmd == nil {
		t.Fatal("buildStatusCmd returned nil")
	}

	if cmd.Use != "status [MOUNTPOINT]" {
		t.Errorf("Expected Use='status [MOUNTPOINT]', got %q", cmd.Use)
	}
}

// TestBuildListCmd tests that the list command is built correctly
func TestBuildListCmd(t *testing.T) {
	cmd := buildListCmd()

	if cmd == nil {
		t.Fatal("buildListCmd returned nil")
	}

	if cmd.Use != "list" {
		t.Errorf("Expected Use='list', got %q", cmd.Use)
	}
}

// TestBuildTestConnectionCmd tests that the test-connection command is built correctly
func TestBuildTestConnectionCmd(t *testing.T) {
	cmd := buildTestConnectionCmd()

	if cmd == nil {
		t.Fatal("buildTestConnectionCmd returned nil")
	}

	if cmd.Use != "test-connection [CONNECTION]" {
		t.Errorf("Expected Use='test-connection [CONNECTION]', got %q", cmd.Use)
	}
}

// TestBuildConfigCmd tests that the config command is built correctly
func TestBuildConfigCmd(t *testing.T) {
	cmd := buildConfigCmd()

	if cmd == nil {
		t.Fatal("buildConfigCmd returned nil")
	}

	if cmd.Use != "config" {
		t.Errorf("Expected Use='config', got %q", cmd.Use)
	}
}

// TestBuildConfigCmd_HasSubcommands tests that config command has subcommands
func TestBuildConfigCmd_HasSubcommands(t *testing.T) {
	cmd := buildConfigCmd()

	expectedSubcommands := []string{
		"show",
		"validate",
		"path",
	}

	subcommandNames := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		subcommandNames[sub.Name()] = true
	}

	for _, expected := range expectedSubcommands {
		if !subcommandNames[expected] {
			t.Errorf("Expected config subcommand %q not found", expected)
		}
	}
}

// TestBuildConfigShowCmd tests that the config show command is built correctly
func TestBuildConfigShowCmd(t *testing.T) {
	cmd := buildConfigShowCmd()

	if cmd == nil {
		t.Fatal("buildConfigShowCmd returned nil")
	}

	if cmd.Use != "show" {
		t.Errorf("Expected Use='show', got %q", cmd.Use)
	}
}

// TestBuildConfigValidateCmd tests that the config validate command is built correctly
func TestBuildConfigValidateCmd(t *testing.T) {
	cmd := buildConfigValidateCmd()

	if cmd == nil {
		t.Fatal("buildConfigValidateCmd returned nil")
	}

	if cmd.Use != "validate" {
		t.Errorf("Expected Use='validate', got %q", cmd.Use)
	}
}

// TestBuildConfigPathCmd tests that the config path command is built correctly
func TestBuildConfigPathCmd(t *testing.T) {
	cmd := buildConfigPathCmd()

	if cmd == nil {
		t.Fatal("buildConfigPathCmd returned nil")
	}

	if cmd.Use != "path" {
		t.Errorf("Expected Use='path', got %q", cmd.Use)
	}
}

// TestRootCmd_Help tests that help is displayed correctly
func TestRootCmd_Help(t *testing.T) {
	ctx := context.Background()
	cmd := buildRootCmd(ctx)

	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("help command failed: %v", err)
	}

	output := buf.String()

	// Check output contains expected sections
	if !strings.Contains(output, "TigerFS") {
		t.Errorf("Expected help to contain 'TigerFS'")
	}

	if !strings.Contains(output, "Available Commands") {
		t.Errorf("Expected help to contain 'Available Commands'")
	}

	if !strings.Contains(output, "Flags") {
		t.Errorf("Expected help to contain 'Flags'")
	}
}

// TestMountCmd_Help tests that mount help is displayed correctly
func TestMountCmd_Help(t *testing.T) {
	ctx := context.Background()
	cmd := buildMountCmd(ctx)

	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("mount help command failed: %v", err)
	}

	output := buf.String()

	// Check output contains expected sections
	if !strings.Contains(output, "mount") {
		t.Errorf("Expected mount help to contain 'mount'")
	}

	if !strings.Contains(output, "Examples") {
		t.Errorf("Expected mount help to contain 'Examples'")
	}

	if !strings.Contains(output, "tiger:") {
		t.Errorf("Expected mount help to contain 'tiger:'")
	}
}

// TestVersionVariables tests that version variables exist
func TestVersionVariables(t *testing.T) {
	// These are set via ldflags in production builds
	// In tests, they should have default values

	if Version == "" {
		t.Error("Version should not be empty")
	}

	if BuildTime == "" {
		t.Error("BuildTime should not be empty")
	}

	if GitCommit == "" {
		t.Error("GitCommit should not be empty")
	}
}

// TestCommandNaming tests that commands follow naming conventions
func TestCommandNaming(t *testing.T) {
	ctx := context.Background()
	root := buildRootCmd(ctx)

	// Check all commands use lowercase with hyphens
	for _, sub := range root.Commands() {
		name := sub.Name()

		// Check for spaces (not allowed)
		if strings.Contains(name, " ") {
			t.Errorf("Command name %q contains spaces", name)
		}

		// Check for uppercase (should be lowercase)
		if name != strings.ToLower(name) {
			t.Errorf("Command name %q should be lowercase", name)
		}

		// Check for underscores (should use hyphens)
		if strings.Contains(name, "_") {
			t.Errorf("Command name %q uses underscores, should use hyphens", name)
		}
	}
}

// TestCommandShortDescriptions tests that all commands have short descriptions
func TestCommandShortDescriptions(t *testing.T) {
	ctx := context.Background()
	root := buildRootCmd(ctx)

	if root.Short == "" {
		t.Error("Root command should have a Short description")
	}

	for _, sub := range root.Commands() {
		if sub.Short == "" {
			t.Errorf("Command %q should have a Short description", sub.Name())
		}
	}
}
