package cmd

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestShowConfig verifies that showConfig outputs all fields in grouped YAML sections.
func TestShowConfig(t *testing.T) {
	cfg := &config.Config{
		Host:                              "localhost",
		Port:                              5432,
		User:                              "postgres",
		Database:                          "testdb",
		Password:                          "secret",
		DefaultSchema:                     "public",
		PasswordCommand:                   "pass show db",
		PoolSize:                          10,
		PoolMaxIdle:                       5,
		TigerCloudServiceID:               "svc123",
		TigerCloudPublicKey:               "pub-should-not-appear",
		TigerCloudSecretKey:               "sec-should-not-appear",
		TigerCloudProjectID:               "proj456",
		DefaultBackend:                    "tiger",
		DefaultMountDir:                   "/tmp",
		DirListingLimit:                   10000,
		DirWritingLimit:                   100000,
		TrailingNewlines:                  true,
		NoFilenameExtensions:              false,
		AttrTimeout:                       1 * time.Second,
		EntryTimeout:                      1 * time.Second,
		QueryTimeout:                      30 * time.Second,
		DirFilterLimit:                    100000,
		MetadataRefreshInterval:           10 * time.Second,
		StructuralMetadataRefreshInterval: 5 * time.Minute,
		NFSStreamingThreshold:             10485760,
		NFSMaxRandomWriteSize:             104857600,
		NFSCacheReaperInterval:            30 * time.Second,
		NFSCacheIdleTimeout:               5 * time.Minute,
		DDLGracePeriod:                    30 * time.Second,
		LogLevel:                          "warn",
		LogFile:                           "/tmp/tigerfs.log",
		LogFormat:                         "text",
		LogSQLParams:                      false,
		DefaultFormat:                     "tsv",
		BinaryEncoding:                    "raw",
		LegacyFuse:                        false,
		ConfigDir:                         "/home/user/.config/tigerfs",
	}

	var buf bytes.Buffer
	if err := showConfig(&buf, cfg); err != nil {
		t.Fatalf("showConfig() error: %v", err)
	}
	output := buf.String()

	// Verify section headers
	for _, section := range []string{
		"connection:", "tiger_cloud:", "backend:", "filesystem:",
		"query:", "metadata:", "nfs:", "ddl:", "logging:", "advanced:",
	} {
		if !strings.Contains(output, section) {
			t.Errorf("missing section %q in output", section)
		}
	}

	// Verify sensitive fields are NOT present
	for _, sensitive := range []string{"secret", "pub-should-not-appear", "sec-should-not-appear"} {
		if strings.Contains(output, sensitive) {
			t.Errorf("sensitive value %q should not appear in output", sensitive)
		}
	}

	// Verify key fields are present
	for _, field := range []string{
		"host: localhost", "port: 5432", "user: postgres", "database: testdb",
		"service_id: svc123", "project_id: proj456",
		"default_backend: tiger", "default_mount_dir: /tmp",
		"dir_listing_limit: 10000", "dir_writing_limit: 100000",
		"trailing_newlines: true", "no_filename_extensions: false",
		"query_timeout: 30s", "dir_filter_limit: 100000",
		"structural_metadata_refresh_interval: 5m0s",
		"streaming_threshold: 10485760", "max_random_write_size: 104857600",
		"grace_period: 30s",
		"log_level: warn", "log_format: text",
		"legacy_fuse: false", "config_dir: /home/user/.config/tigerfs",
	} {
		if !strings.Contains(output, field) {
			t.Errorf("missing field %q in output", field)
		}
	}
}

// TestValidateConfigValues verifies configuration value validation.
func TestValidateConfigValues(t *testing.T) {
	// Helper to create a valid base config
	validConfig := func() *config.Config {
		return &config.Config{
			Port:            5432,
			PoolSize:        10,
			PoolMaxIdle:     5,
			DirListingLimit: 10000,
			DefaultFormat:   "tsv",
			BinaryEncoding:  "raw",
			LogLevel:        "info",
		}
	}

	tests := []struct {
		name    string
		modify  func(*config.Config)
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid config",
			modify:  func(c *config.Config) {},
			wantErr: false,
		},
		{
			name:    "port too low",
			modify:  func(c *config.Config) { c.Port = 0 },
			wantErr: true,
			errMsg:  "invalid port",
		},
		{
			name:    "port too high",
			modify:  func(c *config.Config) { c.Port = 65536 },
			wantErr: true,
			errMsg:  "invalid port",
		},
		{
			name:    "pool_size zero",
			modify:  func(c *config.Config) { c.PoolSize = 0 },
			wantErr: true,
			errMsg:  "invalid pool_size",
		},
		{
			name:    "pool_max_idle negative",
			modify:  func(c *config.Config) { c.PoolMaxIdle = -1 },
			wantErr: true,
			errMsg:  "invalid pool_max_idle",
		},
		{
			name:    "dir_listing_limit zero",
			modify:  func(c *config.Config) { c.DirListingLimit = 0 },
			wantErr: true,
			errMsg:  "invalid dir_listing_limit",
		},
		{
			name:    "invalid default_format",
			modify:  func(c *config.Config) { c.DefaultFormat = "xml" },
			wantErr: true,
			errMsg:  "invalid default_format",
		},
		{
			name:    "invalid binary_encoding",
			modify:  func(c *config.Config) { c.BinaryEncoding = "uuencode" },
			wantErr: true,
			errMsg:  "invalid binary_encoding",
		},
		{
			name:    "invalid log_level",
			modify:  func(c *config.Config) { c.LogLevel = "verbose" },
			wantErr: true,
			errMsg:  "invalid log_level",
		},
		{
			name:    "valid csv format",
			modify:  func(c *config.Config) { c.DefaultFormat = "csv" },
			wantErr: false,
		},
		{
			name:    "valid json format",
			modify:  func(c *config.Config) { c.DefaultFormat = "json" },
			wantErr: false,
		},
		{
			name:    "valid base64 encoding",
			modify:  func(c *config.Config) { c.BinaryEncoding = "base64" },
			wantErr: false,
		},
		{
			name:    "valid hex encoding",
			modify:  func(c *config.Config) { c.BinaryEncoding = "hex" },
			wantErr: false,
		},
		{
			name:    "valid debug log level",
			modify:  func(c *config.Config) { c.LogLevel = "debug" },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.modify(cfg)

			err := validateConfigValues(cfg)

			if tt.wantErr {
				if err == nil {
					t.Errorf("validateConfigValues() expected error containing %q, got nil", tt.errMsg)
				} else if tt.errMsg != "" && !containsString(err.Error(), tt.errMsg) {
					t.Errorf("validateConfigValues() error = %q, want error containing %q", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateConfigValues() unexpected error: %v", err)
				}
			}
		})
	}
}

// containsString checks if s contains substr.
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && searchString(s, substr)))
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
