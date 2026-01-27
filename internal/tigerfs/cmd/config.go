// Package cmd provides CLI commands for TigerFS.
//
// This file implements the config command group for configuration management:
//   - config show: Display current merged configuration
//   - config validate: Validate configuration file syntax
//   - config path: Show configuration file location
package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"gopkg.in/yaml.v3"
)

// buildConfigCmd creates the config command group.
//
// The config command provides subcommands for managing TigerFS configuration:
//   - show: Display the current merged configuration
//   - validate: Check config file for syntax errors
//   - path: Show the config file location
func buildConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Configuration management",
		Long:  `Manage TigerFS configuration.`,
	}

	cmd.AddCommand(buildConfigShowCmd())
	cmd.AddCommand(buildConfigValidateCmd())
	cmd.AddCommand(buildConfigPathCmd())

	return cmd
}

// buildConfigShowCmd creates the config show subcommand.
//
// Displays the current merged configuration from all sources (defaults,
// config file, environment variables). Output is YAML format.
func buildConfigShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Show current configuration",
		Long: `Display merged configuration from all sources.

Shows the effective configuration after merging:
  1. Built-in defaults
  2. Config file (~/.config/tigerfs/config.yaml)
  3. Environment variables (TIGERFS_*, PGHOST, etc.)

Output is YAML format. Sensitive values (passwords) are masked.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			return showConfig(cmd.OutOrStdout(), cfg)
		},
	}
}

// showConfig displays the configuration in YAML format.
//
// Parameters:
//   - w: Writer for output
//   - cfg: Configuration to display
//
// Sensitive fields (password) are masked with asterisks.
func showConfig(w io.Writer, cfg *config.Config) error {
	// Create a display-safe copy with masked password
	display := struct {
		Host                    string `yaml:"host,omitempty"`
		Port                    int    `yaml:"port"`
		User                    string `yaml:"user,omitempty"`
		Database                string `yaml:"database,omitempty"`
		Password                string `yaml:"password,omitempty"`
		DefaultSchema           string `yaml:"default_schema"`
		PoolSize                int    `yaml:"pool_size"`
		PoolMaxIdle             int    `yaml:"pool_max_idle"`
		PasswordCommand         string `yaml:"password_command,omitempty"`
		TigerCloudServiceID     string `yaml:"tiger_service_id,omitempty"`
		TigerCloudProjectID     string `yaml:"tiger_project_id,omitempty"`
		DirListingLimit         int    `yaml:"dir_listing_limit"`
		TrailingNewlines        bool   `yaml:"trailing_newlines"`
		AttrTimeout             string `yaml:"attr_timeout"`
		EntryTimeout            string `yaml:"entry_timeout"`
		MetadataRefreshInterval string `yaml:"metadata_refresh_interval"`
		LogLevel                string `yaml:"log_level"`
		LogFile                 string `yaml:"log_file,omitempty"`
		LogFormat               string `yaml:"log_format"`
		DefaultFormat           string `yaml:"default_format"`
		BinaryEncoding          string `yaml:"binary_encoding"`
		Debug                   bool   `yaml:"debug"`
		ConfigDir               string `yaml:"config_dir"`
	}{
		Host:                    cfg.Host,
		Port:                    cfg.Port,
		User:                    cfg.User,
		Database:                cfg.Database,
		Password:                maskPassword(cfg.Password),
		DefaultSchema:           cfg.DefaultSchema,
		PoolSize:                cfg.PoolSize,
		PoolMaxIdle:             cfg.PoolMaxIdle,
		PasswordCommand:         cfg.PasswordCommand,
		TigerCloudServiceID:     cfg.TigerCloudServiceID,
		TigerCloudProjectID:     cfg.TigerCloudProjectID,
		DirListingLimit:         cfg.DirListingLimit,
		TrailingNewlines:        cfg.TrailingNewlines,
		AttrTimeout:             cfg.AttrTimeout.String(),
		EntryTimeout:            cfg.EntryTimeout.String(),
		MetadataRefreshInterval: cfg.MetadataRefreshInterval.String(),
		LogLevel:                cfg.LogLevel,
		LogFile:                 cfg.LogFile,
		LogFormat:               cfg.LogFormat,
		DefaultFormat:           cfg.DefaultFormat,
		BinaryEncoding:          cfg.BinaryEncoding,
		Debug:                   cfg.Debug,
		ConfigDir:               cfg.ConfigDir,
	}

	encoder := yaml.NewEncoder(w)
	encoder.SetIndent(2)
	if err := encoder.Encode(display); err != nil {
		return fmt.Errorf("failed to encode config: %w", err)
	}

	return nil
}

// maskPassword returns a masked version of the password for display.
// Returns empty string if password is empty, otherwise returns asterisks.
func maskPassword(password string) string {
	if password == "" {
		return ""
	}
	return "********"
}

// buildConfigValidateCmd creates the config validate subcommand.
//
// Validates the configuration file for syntax errors and invalid values.
func buildConfigValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate configuration file",
		Long: `Validate configuration file syntax and values.

Checks the config file at ~/.config/tigerfs/config.yaml for:
  - YAML syntax errors
  - Unknown configuration keys
  - Invalid value types`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			return validateConfig(cmd.OutOrStdout())
		},
	}
}

// validateConfig checks the configuration file for errors.
//
// Parameters:
//   - w: Writer for output messages
//
// Returns an error if:
//   - Config file doesn't exist (informational, not an error)
//   - Config file has syntax errors
//   - Config file has invalid values
func validateConfig(w io.Writer) error {
	configPath := filepath.Join(config.GetDefaultConfigDir(), "config.yaml")

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Fprintf(w, "No configuration file found at %s\n", configPath)
		fmt.Fprintln(w, "Using default configuration.")
		return nil
	}

	// Try to read and parse the config file
	v := viper.New()
	v.SetConfigFile(configPath)

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("configuration file error: %w", err)
	}

	// Try to unmarshal to catch type errors
	var cfg config.Config
	if err := v.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("configuration value error: %w", err)
	}

	// Validate specific values
	if err := validateConfigValues(&cfg); err != nil {
		return err
	}

	fmt.Fprintf(w, "Configuration file is valid: %s\n", configPath)
	return nil
}

// validateConfigValues checks configuration values for validity.
//
// Parameters:
//   - cfg: Configuration to validate
//
// Returns an error if any values are invalid (out of range, etc.)
func validateConfigValues(cfg *config.Config) error {
	if cfg.Port < 1 || cfg.Port > 65535 {
		return fmt.Errorf("invalid port: %d (must be 1-65535)", cfg.Port)
	}

	if cfg.PoolSize < 1 {
		return fmt.Errorf("invalid pool_size: %d (must be at least 1)", cfg.PoolSize)
	}

	if cfg.PoolMaxIdle < 0 {
		return fmt.Errorf("invalid pool_max_idle: %d (must be non-negative)", cfg.PoolMaxIdle)
	}

	if cfg.DirListingLimit < 1 {
		return fmt.Errorf("invalid dir_listing_limit: %d (must be at least 1)", cfg.DirListingLimit)
	}

	validFormats := map[string]bool{"tsv": true, "csv": true, "json": true}
	if !validFormats[cfg.DefaultFormat] {
		return fmt.Errorf("invalid default_format: %q (must be tsv, csv, or json)", cfg.DefaultFormat)
	}

	validEncodings := map[string]bool{"raw": true, "base64": true, "hex": true}
	if !validEncodings[cfg.BinaryEncoding] {
		return fmt.Errorf("invalid binary_encoding: %q (must be raw, base64, or hex)", cfg.BinaryEncoding)
	}

	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[cfg.LogLevel] {
		return fmt.Errorf("invalid log_level: %q (must be debug, info, warn, or error)", cfg.LogLevel)
	}

	return nil
}

// buildConfigPathCmd creates the config path subcommand.
//
// Displays the path to the configuration file and whether it exists.
func buildConfigPathCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "path",
		Short: "Show configuration file path",
		Long:  `Display the path to the configuration file.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			return showConfigPath(cmd.OutOrStdout())
		},
	}
}

// showConfigPath displays the configuration file path and status.
//
// Parameters:
//   - w: Writer for output
//
// Shows the path and whether the file exists.
func showConfigPath(w io.Writer) error {
	configPath := filepath.Join(config.GetDefaultConfigDir(), "config.yaml")

	fmt.Fprintln(w, configPath)

	// Check if file exists and show status
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Fprintln(w, "(file does not exist)")
	}

	return nil
}
