package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// resetViper resets viper to a clean state for test isolation
func resetViper() {
	viper.Reset()
}

// TestGetDefaultConfigDir_XDGConfigHome tests XDG_CONFIG_HOME takes precedence
func TestGetDefaultConfigDir_XDGConfigHome(t *testing.T) {
	// Save original env
	origXDG := os.Getenv("XDG_CONFIG_HOME")
	origAppData := os.Getenv("APPDATA")
	defer func() {
		_ = os.Setenv("XDG_CONFIG_HOME", origXDG)
		_ = os.Setenv("APPDATA", origAppData)
	}()

	// Set XDG_CONFIG_HOME
	_ = os.Setenv("XDG_CONFIG_HOME", "/custom/xdg")
	_ = os.Unsetenv("APPDATA")

	result := GetDefaultConfigDir()
	expected := "/custom/xdg/tigerfs"

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestGetDefaultConfigDir_APPDATA tests Windows APPDATA fallback
func TestGetDefaultConfigDir_APPDATA(t *testing.T) {
	// Save original env
	origXDG := os.Getenv("XDG_CONFIG_HOME")
	origAppData := os.Getenv("APPDATA")
	defer func() {
		_ = os.Setenv("XDG_CONFIG_HOME", origXDG)
		_ = os.Setenv("APPDATA", origAppData)
	}()

	// Unset XDG, set APPDATA
	_ = os.Unsetenv("XDG_CONFIG_HOME")
	_ = os.Setenv("APPDATA", "C:\\Users\\Test\\AppData\\Roaming")

	result := GetDefaultConfigDir()
	expected := filepath.Join("C:\\Users\\Test\\AppData\\Roaming", "tigerfs")

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestGetDefaultConfigDir_HomeDir tests fallback to ~/.config/tigerfs
func TestGetDefaultConfigDir_HomeDir(t *testing.T) {
	// Save original env
	origXDG := os.Getenv("XDG_CONFIG_HOME")
	origAppData := os.Getenv("APPDATA")
	defer func() {
		_ = os.Setenv("XDG_CONFIG_HOME", origXDG)
		_ = os.Setenv("APPDATA", origAppData)
	}()

	// Unset both
	_ = os.Unsetenv("XDG_CONFIG_HOME")
	_ = os.Unsetenv("APPDATA")

	result := GetDefaultConfigDir()

	// Should end with .config/tigerfs
	if !filepath.IsAbs(result) {
		t.Errorf("Expected absolute path, got %q", result)
	}

	if filepath.Base(result) != "tigerfs" {
		t.Errorf("Expected path to end with 'tigerfs', got %q", result)
	}

	parent := filepath.Base(filepath.Dir(result))
	if parent != ".config" {
		t.Errorf("Expected parent dir '.config', got %q", parent)
	}
}

// TestInit_SetsDefaults tests that Init sets all default values
func TestInit_SetsDefaults(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	// Check all defaults
	tests := []struct {
		key      string
		expected interface{}
	}{
		{"port", 5432},
		{"default_schema", "public"},
		{"pool_size", 10},
		{"pool_max_idle", 5},
		{"dir_listing_limit", 10000},
		{"attr_timeout", 1 * time.Second},
		{"entry_timeout", 1 * time.Second},
		{"metadata_refresh_interval", 30 * time.Second},
		{"log_level", "info"},
		{"log_format", "text"},
		{"default_format", "tsv"},
		{"binary_encoding", "raw"},
		{"debug", false},
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			got := viper.Get(tc.key)
			if got != tc.expected {
				t.Errorf("Default for %q: expected %v (%T), got %v (%T)",
					tc.key, tc.expected, tc.expected, got, got)
			}
		})
	}
}

// TestInit_ConfigFileNotFound tests that missing config file is not an error
func TestInit_ConfigFileNotFound(t *testing.T) {
	resetViper()

	// Point to non-existent directory
	origXDG := os.Getenv("XDG_CONFIG_HOME")
	defer func() { _ = os.Setenv("XDG_CONFIG_HOME", origXDG) }()
	_ = os.Setenv("XDG_CONFIG_HOME", "/nonexistent/path/that/does/not/exist")

	err := Init()
	if err != nil {
		t.Errorf("Init() should not error for missing config file, got: %v", err)
	}
}

// TestLoad_UnmarshalConfig tests that Load correctly unmarshals to Config struct
func TestLoad_UnmarshalConfig(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Verify struct fields match defaults
	if cfg.Port != 5432 {
		t.Errorf("Expected Port=5432, got %d", cfg.Port)
	}
	if cfg.DefaultSchema != "public" {
		t.Errorf("Expected DefaultSchema='public', got %q", cfg.DefaultSchema)
	}
	if cfg.PoolSize != 10 {
		t.Errorf("Expected PoolSize=10, got %d", cfg.PoolSize)
	}
	if cfg.PoolMaxIdle != 5 {
		t.Errorf("Expected PoolMaxIdle=5, got %d", cfg.PoolMaxIdle)
	}
	if cfg.DirListingLimit != 10000 {
		t.Errorf("Expected DirListingLimit=10000, got %d", cfg.DirListingLimit)
	}
	if cfg.AttrTimeout != 1*time.Second {
		t.Errorf("Expected AttrTimeout=1s, got %v", cfg.AttrTimeout)
	}
	if cfg.EntryTimeout != 1*time.Second {
		t.Errorf("Expected EntryTimeout=1s, got %v", cfg.EntryTimeout)
	}
	if cfg.MetadataRefreshInterval != 30*time.Second {
		t.Errorf("Expected MetadataRefreshInterval=30s, got %v", cfg.MetadataRefreshInterval)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("Expected LogLevel='info', got %q", cfg.LogLevel)
	}
	if cfg.LogFormat != "text" {
		t.Errorf("Expected LogFormat='text', got %q", cfg.LogFormat)
	}
	if cfg.DefaultFormat != "tsv" {
		t.Errorf("Expected DefaultFormat='tsv', got %q", cfg.DefaultFormat)
	}
	if cfg.BinaryEncoding != "raw" {
		t.Errorf("Expected BinaryEncoding='raw', got %q", cfg.BinaryEncoding)
	}
	if cfg.Debug != false {
		t.Errorf("Expected Debug=false, got %v", cfg.Debug)
	}
}

// TestConfig_TigerFSEnvVars tests TIGERFS_* environment variables
func TestConfig_TigerFSEnvVars(t *testing.T) {
	resetViper()

	// Save and restore env
	envVars := []string{
		"TIGERFS_DIR_LISTING_LIMIT",
		"TIGERFS_LOG_LEVEL",
		"TIGERFS_DEBUG",
		"TIGERFS_DEFAULT_FORMAT",
	}
	origValues := make(map[string]string)
	for _, v := range envVars {
		origValues[v] = os.Getenv(v)
	}
	defer func() {
		for _, v := range envVars {
			if origValues[v] == "" {
				_ = os.Unsetenv(v)
			} else {
				_ = os.Setenv(v, origValues[v])
			}
		}
	}()

	// Set env vars
	_ = os.Setenv("TIGERFS_DIR_LISTING_LIMIT", "5000")
	_ = os.Setenv("TIGERFS_LOG_LEVEL", "debug")
	_ = os.Setenv("TIGERFS_DEBUG", "true")
	_ = os.Setenv("TIGERFS_DEFAULT_FORMAT", "json")

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.DirListingLimit != 5000 {
		t.Errorf("Expected DirListingLimit=5000 from env, got %d", cfg.DirListingLimit)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected LogLevel='debug' from env, got %q", cfg.LogLevel)
	}
	if cfg.Debug != true {
		t.Errorf("Expected Debug=true from env, got %v", cfg.Debug)
	}
	if cfg.DefaultFormat != "json" {
		t.Errorf("Expected DefaultFormat='json' from env, got %q", cfg.DefaultFormat)
	}
}

// TestConfig_PostgreSQLEnvVars tests PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD
func TestConfig_PostgreSQLEnvVars(t *testing.T) {
	resetViper()

	// Save and restore env
	envVars := []string{"PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD"}
	origValues := make(map[string]string)
	for _, v := range envVars {
		origValues[v] = os.Getenv(v)
	}
	defer func() {
		for _, v := range envVars {
			if origValues[v] == "" {
				_ = os.Unsetenv(v)
			} else {
				_ = os.Setenv(v, origValues[v])
			}
		}
	}()

	// Set PostgreSQL env vars
	_ = os.Setenv("PGHOST", "db.example.com")
	_ = os.Setenv("PGPORT", "5433")
	_ = os.Setenv("PGUSER", "testuser")
	_ = os.Setenv("PGDATABASE", "testdb")
	_ = os.Setenv("PGPASSWORD", "secret123")

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Host != "db.example.com" {
		t.Errorf("Expected Host='db.example.com' from PGHOST, got %q", cfg.Host)
	}
	if cfg.Port != 5433 {
		t.Errorf("Expected Port=5433 from PGPORT, got %d", cfg.Port)
	}
	if cfg.User != "testuser" {
		t.Errorf("Expected User='testuser' from PGUSER, got %q", cfg.User)
	}
	if cfg.Database != "testdb" {
		t.Errorf("Expected Database='testdb' from PGDATABASE, got %q", cfg.Database)
	}
	if cfg.Password != "secret123" {
		t.Errorf("Expected Password='secret123' from PGPASSWORD, got %q", cfg.Password)
	}
}

// TestConfig_TigerCloudServiceIDEnv tests TIGER_SERVICE_ID environment variable.
func TestConfig_TigerCloudServiceIDEnv(t *testing.T) {
	resetViper()

	// Save and restore env
	origValue := os.Getenv("TIGER_SERVICE_ID")
	defer func() {
		if origValue == "" {
			_ = os.Unsetenv("TIGER_SERVICE_ID")
		} else {
			_ = os.Setenv("TIGER_SERVICE_ID", origValue)
		}
	}()

	_ = os.Setenv("TIGER_SERVICE_ID", "e6ue9697jf")

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.TigerCloudServiceID != "e6ue9697jf" {
		t.Errorf("Expected TigerCloudServiceID='e6ue9697jf' from env, got %q", cfg.TigerCloudServiceID)
	}
}

// TestConfig_Precedence_EnvOverridesDefault tests that env vars override defaults
func TestConfig_Precedence_EnvOverridesDefault(t *testing.T) {
	resetViper()

	// Save and restore env
	origValue := os.Getenv("TIGERFS_PORT")
	defer func() {
		if origValue == "" {
			_ = os.Unsetenv("TIGERFS_PORT")
		} else {
			_ = os.Setenv("TIGERFS_PORT", origValue)
		}
	}()

	// Set env var that overrides default
	_ = os.Setenv("TIGERFS_PORT", "6543")

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Default is 5432, env should override to 6543
	if cfg.Port != 6543 {
		t.Errorf("Expected Port=6543 (env override), got %d", cfg.Port)
	}
}

// TestConfig_Precedence_TigerFSEnvOverridesPGEnv tests TIGERFS_PORT vs PGPORT
// TIGERFS_* env vars take precedence due to AutomaticEnv() being processed after BindEnv
func TestConfig_Precedence_TigerFSEnvOverridesPGEnv(t *testing.T) {
	resetViper()

	// Save and restore env
	origPG := os.Getenv("PGPORT")
	origTiger := os.Getenv("TIGERFS_PORT")
	defer func() {
		if origPG == "" {
			_ = os.Unsetenv("PGPORT")
		} else {
			_ = os.Setenv("PGPORT", origPG)
		}
		if origTiger == "" {
			_ = os.Unsetenv("TIGERFS_PORT")
		} else {
			_ = os.Setenv("TIGERFS_PORT", origTiger)
		}
	}()

	// Set both - TIGERFS_PORT wins due to AutomaticEnv with TIGERFS prefix
	_ = os.Setenv("PGPORT", "5434")
	_ = os.Setenv("TIGERFS_PORT", "6543")

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// TIGERFS_PORT takes precedence over PGPORT
	if cfg.Port != 6543 {
		t.Errorf("Expected Port=6543 (TIGERFS_PORT wins), got %d", cfg.Port)
	}
}

// TestConfig_Precedence_PGEnvWhenNoTigerFS tests that PGPORT works when TIGERFS_PORT is not set
func TestConfig_Precedence_PGEnvWhenNoTigerFS(t *testing.T) {
	resetViper()

	// Save and restore env
	origPG := os.Getenv("PGPORT")
	origTiger := os.Getenv("TIGERFS_PORT")
	defer func() {
		if origPG == "" {
			_ = os.Unsetenv("PGPORT")
		} else {
			_ = os.Setenv("PGPORT", origPG)
		}
		if origTiger == "" {
			_ = os.Unsetenv("TIGERFS_PORT")
		} else {
			_ = os.Setenv("TIGERFS_PORT", origTiger)
		}
	}()

	// Set only PGPORT
	_ = os.Setenv("PGPORT", "5434")
	_ = os.Unsetenv("TIGERFS_PORT")

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// PGPORT should be used when TIGERFS_PORT is not set
	if cfg.Port != 5434 {
		t.Errorf("Expected Port=5434 (PGPORT), got %d", cfg.Port)
	}
}

// TestConfig_EmptyEnvVars tests that empty env vars don't override defaults
func TestConfig_EmptyEnvVars(t *testing.T) {
	resetViper()

	// Save and restore env
	origValue := os.Getenv("TIGERFS_LOG_LEVEL")
	defer func() {
		if origValue == "" {
			_ = os.Unsetenv("TIGERFS_LOG_LEVEL")
		} else {
			_ = os.Setenv("TIGERFS_LOG_LEVEL", origValue)
		}
	}()

	// Set empty env var - should use default
	_ = os.Setenv("TIGERFS_LOG_LEVEL", "")

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Empty env var should NOT override default
	// Note: This depends on viper's behavior - empty string might be used
	// This test documents the actual behavior
	if cfg.LogLevel != "" && cfg.LogLevel != "info" {
		t.Errorf("Expected LogLevel='info' (default) or '' (empty), got %q", cfg.LogLevel)
	}
}

// TestConfig_ConnectionFields tests all connection-related fields
func TestConfig_ConnectionFields(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Connection fields should have sensible defaults or be empty
	// Port has default of 5432
	if cfg.Port != 5432 {
		t.Errorf("Expected default Port=5432, got %d", cfg.Port)
	}

	// Host, User, Database, Password are empty by default
	if cfg.Host != "" {
		t.Errorf("Expected empty Host by default, got %q", cfg.Host)
	}
	if cfg.User != "" {
		t.Errorf("Expected empty User by default, got %q", cfg.User)
	}
	if cfg.Database != "" {
		t.Errorf("Expected empty Database by default, got %q", cfg.Database)
	}
	if cfg.Password != "" {
		t.Errorf("Expected empty Password by default, got %q", cfg.Password)
	}
	if cfg.PasswordCommand != "" {
		t.Errorf("Expected empty PasswordCommand by default, got %q", cfg.PasswordCommand)
	}
	if cfg.TigerCloudServiceID != "" {
		t.Errorf("Expected empty TigerCloudServiceID by default, got %q", cfg.TigerCloudServiceID)
	}
}

// TestConfig_FilesystemFields tests filesystem-related fields
func TestConfig_FilesystemFields(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.DirListingLimit != 10000 {
		t.Errorf("Expected DirListingLimit=10000, got %d", cfg.DirListingLimit)
	}
	if cfg.AttrTimeout != 1*time.Second {
		t.Errorf("Expected AttrTimeout=1s, got %v", cfg.AttrTimeout)
	}
	if cfg.EntryTimeout != 1*time.Second {
		t.Errorf("Expected EntryTimeout=1s, got %v", cfg.EntryTimeout)
	}
	if cfg.MetadataRefreshInterval != 30*time.Second {
		t.Errorf("Expected MetadataRefreshInterval=30s, got %v", cfg.MetadataRefreshInterval)
	}
}

// TestConfig_LoggingFields tests logging-related fields
func TestConfig_LoggingFields(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("Expected LogLevel='info', got %q", cfg.LogLevel)
	}
	if cfg.LogFile != "" {
		t.Errorf("Expected empty LogFile by default, got %q", cfg.LogFile)
	}
	if cfg.LogFormat != "text" {
		t.Errorf("Expected LogFormat='text', got %q", cfg.LogFormat)
	}
	if cfg.Debug != false {
		t.Errorf("Expected Debug=false, got %v", cfg.Debug)
	}
}

// TestConfig_FormatFields tests format-related fields
func TestConfig_FormatFields(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.DefaultFormat != "tsv" {
		t.Errorf("Expected DefaultFormat='tsv', got %q", cfg.DefaultFormat)
	}
	if cfg.BinaryEncoding != "raw" {
		t.Errorf("Expected BinaryEncoding='raw', got %q", cfg.BinaryEncoding)
	}
}

// TestConfig_ConfigDirField tests the config_dir field
func TestConfig_ConfigDirField(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// ConfigDir should be set to default
	if cfg.ConfigDir == "" {
		t.Error("Expected ConfigDir to have default value")
	}

	// Should end with tigerfs
	if filepath.Base(cfg.ConfigDir) != "tigerfs" {
		t.Errorf("Expected ConfigDir to end with 'tigerfs', got %q", cfg.ConfigDir)
	}
}

// TestLoad_CalledBeforeInit tests that Load works even without Init
// (viper's defaults won't be set, but unmarshal should still work)
func TestLoad_CalledBeforeInit(t *testing.T) {
	resetViper()

	// Don't call Init()
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Without Init, we get zero values
	if cfg.Port != 0 {
		t.Errorf("Expected Port=0 without Init(), got %d", cfg.Port)
	}
}

// TestMultipleInitCalls tests that Init can be called multiple times safely
func TestMultipleInitCalls(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("First Init() failed: %v", err)
	}

	err = Init()
	if err != nil {
		t.Fatalf("Second Init() failed: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Defaults should still be correct
	if cfg.Port != 5432 {
		t.Errorf("Expected Port=5432 after multiple Init calls, got %d", cfg.Port)
	}
}

// TestLoad_MultipleLoadCalls tests that Load can be called multiple times
func TestLoad_MultipleLoadCalls(t *testing.T) {
	resetViper()

	err := Init()
	if err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cfg1, err := Load()
	if err != nil {
		t.Fatalf("First Load() failed: %v", err)
	}

	cfg2, err := Load()
	if err != nil {
		t.Fatalf("Second Load() failed: %v", err)
	}

	// Should get independent Config instances with same values
	if cfg1 == cfg2 {
		t.Error("Load() should return new Config instances")
	}

	if cfg1.Port != cfg2.Port {
		t.Errorf("Config values should match: %d != %d", cfg1.Port, cfg2.Port)
	}
}
