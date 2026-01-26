package cmd

import (
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// TestMaskPassword verifies password masking for display.
func TestMaskPassword(t *testing.T) {
	tests := []struct {
		name     string
		password string
		want     string
	}{
		{
			name:     "empty password",
			password: "",
			want:     "",
		},
		{
			name:     "short password",
			password: "abc",
			want:     "********",
		},
		{
			name:     "long password",
			password: "verylongsecretpassword123!@#",
			want:     "********",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskPassword(tt.password)
			if got != tt.want {
				t.Errorf("maskPassword(%q) = %q, want %q", tt.password, got, tt.want)
			}
		})
	}
}

// TestValidateConfigValues verifies configuration value validation.
func TestValidateConfigValues(t *testing.T) {
	// Helper to create a valid base config
	validConfig := func() *config.Config {
		return &config.Config{
			Port:           5432,
			PoolSize:       10,
			PoolMaxIdle:    5,
			MaxLsRows:      10000,
			DefaultFormat:  "tsv",
			BinaryEncoding: "raw",
			LogLevel:       "info",
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
			name:    "max_ls_rows zero",
			modify:  func(c *config.Config) { c.MaxLsRows = 0 },
			wantErr: true,
			errMsg:  "invalid max_ls_rows",
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
