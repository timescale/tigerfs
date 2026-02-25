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
	Host                string `mapstructure:"host"`
	Port                int    `mapstructure:"port"`
	User                string `mapstructure:"user"`
	Database            string `mapstructure:"database"`
	Password            string `mapstructure:"password"`
	DefaultSchema       string `mapstructure:"default_schema"`
	PoolSize            int    `mapstructure:"pool_size"`
	PoolMaxIdle         int    `mapstructure:"pool_max_idle"`
	PasswordCommand     string `mapstructure:"password_command"`
	TigerCloudServiceID string `mapstructure:"tiger_service_id"`
	TigerCloudPublicKey string `mapstructure:"tiger_public_key"`
	TigerCloudSecretKey string `mapstructure:"tiger_secret_key"`
	TigerCloudProjectID string `mapstructure:"tiger_project_id"`

	// Backend
	DefaultBackend  string `mapstructure:"default_backend"`   // "tiger", "ghost", or "" (none)
	DefaultMountDir string `mapstructure:"default_mount_dir"` // Base directory for auto-generated mountpoints (default: /tmp)

	// Filesystem
	DirListingLimit      int           `mapstructure:"dir_listing_limit"`
	DirWritingLimit      int           `mapstructure:"dir_writing_limit"`
	TrailingNewlines     bool          `mapstructure:"trailing_newlines"`
	NoFilenameExtensions bool          `mapstructure:"no_filename_extensions"`
	AttrTimeout          time.Duration `mapstructure:"attr_timeout"`
	EntryTimeout         time.Duration `mapstructure:"entry_timeout"`

	// Query Safety
	QueryTimeout   time.Duration `mapstructure:"query_timeout"`    // Global statement timeout for all queries (default: 30s)
	DirFilterLimit int           `mapstructure:"dir_filter_limit"` // Row count threshold for .filter/ value listing (default: 100000)

	// Metadata
	MetadataRefreshInterval time.Duration `mapstructure:"metadata_refresh_interval"`

	// NFS file cache settings (ADR-010)
	NFSStreamingThreshold  int64         `mapstructure:"nfs_streaming_threshold"`   // Buffer size triggering streaming commit (default: 10MB)
	NFSMaxRandomWriteSize  int64         `mapstructure:"nfs_max_random_write_size"` // Max buffer for random writes (default: 100MB)
	NFSCacheReaperInterval time.Duration `mapstructure:"nfs_cache_reaper_interval"` // How often reaper checks for stale entries (default: 30s)
	NFSCacheIdleTimeout    time.Duration `mapstructure:"nfs_cache_idle_timeout"`    // Idle time before force-commit (default: 5m)

	// DDL
	DDLGracePeriod time.Duration `mapstructure:"ddl_grace_period"` // How long completed DDL sessions stay visible (default: 30s)

	// Logging
	LogLevel  string `mapstructure:"log_level"`
	LogFile   string `mapstructure:"log_file"`
	LogFormat string `mapstructure:"log_format"`

	// Formats
	DefaultFormat  string `mapstructure:"default_format"`
	BinaryEncoding string `mapstructure:"binary_encoding"`

	// Debug
	Debug bool `mapstructure:"debug"`

	// FUSE backend selection (Linux only)
	LegacyFuse bool `mapstructure:"legacy_fuse"` // Use legacy specialized FUSE nodes instead of shared Operations

	// Config directory
	ConfigDir string `mapstructure:"config_dir"`
}

func Init() error {
	// Set defaults
	viper.SetDefault("port", 5432)
	viper.SetDefault("default_schema", "") // Empty = inherit from PostgreSQL's current_schema()
	viper.SetDefault("pool_size", 10)
	viper.SetDefault("pool_max_idle", 5)
	viper.SetDefault("dir_listing_limit", 10000)
	viper.SetDefault("dir_writing_limit", 100000)
	viper.SetDefault("trailing_newlines", true)
	viper.SetDefault("no_filename_extensions", false)
	viper.SetDefault("attr_timeout", 1*time.Second)
	viper.SetDefault("entry_timeout", 1*time.Second)
	viper.SetDefault("query_timeout", 30*time.Second)
	viper.SetDefault("dir_filter_limit", 100000)
	viper.SetDefault("metadata_refresh_interval", 30*time.Second)
	viper.SetDefault("nfs_streaming_threshold", int64(10*1024*1024))
	viper.SetDefault("nfs_max_random_write_size", int64(100*1024*1024))
	viper.SetDefault("nfs_cache_reaper_interval", 30*time.Second)
	viper.SetDefault("nfs_cache_idle_timeout", 5*time.Minute)
	viper.SetDefault("ddl_grace_period", 30*time.Second)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("log_format", "text")
	viper.SetDefault("default_format", "tsv")
	viper.SetDefault("binary_encoding", "raw")
	viper.SetDefault("debug", false)
	viper.SetDefault("legacy_fuse", false)
	viper.SetDefault("default_backend", "")
	viper.SetDefault("default_mount_dir", "/tmp")
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
		// Tiger Cloud env vars (for headless/Docker authentication)
		// Use with: tiger auth login (reads these automatically)
		viper.BindEnv("tiger_service_id", "TIGER_SERVICE_ID"),
		viper.BindEnv("tiger_public_key", "TIGER_PUBLIC_KEY"),
		viper.BindEnv("tiger_secret_key", "TIGER_SECRET_KEY"),
		viper.BindEnv("tiger_project_id", "TIGER_PROJECT_ID"),
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
