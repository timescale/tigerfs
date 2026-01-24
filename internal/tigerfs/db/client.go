package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// Client represents a PostgreSQL database client with connection pooling
type Client struct {
	pool *pgxpool.Pool
	cfg  *config.Config
}

// NewClient creates a new database client
func NewClient(ctx context.Context, cfg *config.Config, connStr string) (*Client, error) {
	logging.Debug("Creating database client", zap.String("connection", connStr))

	// Resolve password from multiple sources (env vars, password_command)
	// Note: .pgpass file is handled automatically by pgx
	password, err := resolvePassword(ctx, cfg, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve password: %w", err)
	}

	// Inject password into connection string if found
	if password != "" {
		connStr, err = injectPasswordIntoConnStr(connStr, password)
		if err != nil {
			return nil, fmt.Errorf("failed to inject password: %w", err)
		}
	}

	// Parse connection string
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Configure connection pool from config
	poolConfig.MaxConns = int32(cfg.PoolSize)
	poolConfig.MinConns = int32(cfg.PoolMaxIdle)

	logging.Debug("Configuring connection pool",
		zap.Int("max_conns", cfg.PoolSize),
		zap.Int("min_conns", cfg.PoolMaxIdle),
	)

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection with ping
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logging.Info("Database connection established",
		zap.String("host", poolConfig.ConnConfig.Host),
		zap.Uint16("port", poolConfig.ConnConfig.Port),
		zap.String("database", poolConfig.ConnConfig.Database),
		zap.String("user", poolConfig.ConnConfig.User),
	)

	return &Client{
		pool: pool,
		cfg:  cfg,
	}, nil
}

// Close closes the database connection pool
func (c *Client) Close() error {
	if c.pool != nil {
		logging.Debug("Closing database connection pool")
		c.pool.Close()
	}
	return nil
}

// Query executes a SQL query and returns results
func (c *Client) Query(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	logging.Debug("Executing query", zap.String("sql", sql))

	// TODO: Execute query using pgx
	// rows, err := c.pool.Query(ctx, sql, args...)
	// if err != nil {
	// 	return nil, err
	// }
	// defer rows.Close()

	return nil, fmt.Errorf("query execution not yet implemented")
}

// Exec executes a SQL statement (INSERT, UPDATE, DELETE)
func (c *Client) Exec(ctx context.Context, sql string, args ...interface{}) error {
	logging.Debug("Executing statement", zap.String("sql", sql))

	// TODO: Execute statement using pgx
	// _, err := c.pool.Exec(ctx, sql, args...)
	// return err

	return fmt.Errorf("statement execution not yet implemented")
}
