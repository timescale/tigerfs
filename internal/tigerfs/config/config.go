package config

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	// Connection
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	User            string        `mapstructure:"user"`
	Database        string        `mapstructure:"database"`
	Password        string        `mapstructure:"password"`
	DefaultSchema   string        `mapstructure:"default_schema"`
	PoolSize        int           `mapstructure:"pool_size"`
	PoolMaxIdle     int           `mapstructure:"pool_max_idle"`
	PasswordCommand string        `mapstructure:"password_command"`
	TigerServiceID  string        `mapstructure:"tiger_service_id"`

	// Filesystem
	MaxLsRows        int           `mapstructure:"max_ls_rows"`
	TrailingNewlines bool          `mapstructure:"trailing_newlines"`
	AttrTimeout      time.Duration `mapstructure:"attr_timeout"`
	EntryTimeout time.Duration `mapstructure:"entry_timeout"`

	// Metadata
	MetadataRefreshInterval time.Duration `mapstructure:"metadata_refresh_interval"`

	// Logging
	LogLevel  string `mapstructure:"log_level"`
	LogFile   string `mapstructure:"log_file"`
	LogFormat string `mapstructure:"log_format"`

	// Formats
	DefaultFormat  string `mapstructure:"default_format"`
	BinaryEncoding string `mapstructure:"binary_encoding"`

	// Debug
	Debug bool `mapstructure:"debug"`

	// Config directory
	ConfigDir string `mapstructure:"config_dir"`
}

func Init() error {
	// Set defaults
	viper.SetDefault("port", 5432)
	viper.SetDefault("default_schema", "public")
	viper.SetDefault("pool_size", 10)
	viper.SetDefault("pool_max_idle", 5)
	viper.SetDefault("max_ls_rows", 10000)
	viper.SetDefault("trailing_newlines", true)
	viper.SetDefault("attr_timeout", 1*time.Second)
	viper.SetDefault("entry_timeout", 1*time.Second)
	viper.SetDefault("metadata_refresh_interval", 30*time.Second)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("log_format", "text")
	viper.SetDefault("default_format", "tsv")
	viper.SetDefault("binary_encoding", "raw")
	viper.SetDefault("debug", false)
	viper.SetDefault("config_dir", GetDefaultConfigDir())

	// Setup config file
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(GetDefaultConfigDir())

	// Environment variables
	viper.SetEnvPrefix("TIGERFS")
	viper.AutomaticEnv()

	// PostgreSQL standard env vars
	if err := errors.Join(
		viper.BindEnv("host", "PGHOST"),
		viper.BindEnv("port", "PGPORT"),
		viper.BindEnv("user", "PGUSER"),
		viper.BindEnv("database", "PGDATABASE"),
		viper.BindEnv("password", "PGPASSWORD"),
		// Tiger Cloud env var
		viper.BindEnv("tiger_service_id", "TIGER_SERVICE_ID"),
	); err != nil {
		return err
	}

	// Read config file (ignore if doesn't exist)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	return nil
}

func Load() (*Config, error) {
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func GetDefaultConfigDir() string {
	// ~/.config/tigerfs on Unix, %APPDATA%/tigerfs on Windows
	if dir := os.Getenv("XDG_CONFIG_HOME"); dir != "" {
		return filepath.Join(dir, "tigerfs")
	}

	if dir := os.Getenv("APPDATA"); dir != "" {
		// Windows
		return filepath.Join(dir, "tigerfs")
	}

	// Fallback to home directory
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "tigerfs")
}
