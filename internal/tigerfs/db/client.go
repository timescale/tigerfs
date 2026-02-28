package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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

// WithQueryTimeout returns a context with the configured query timeout applied.
// If the config has no timeout or timeout is zero, returns the original context.
// The returned cancel function should be called when the query completes.
func (c *Client) WithQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.cfg == nil || c.cfg.QueryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.cfg.QueryTimeout)
}

// QueryTimeout returns the configured query timeout duration.
func (c *Client) QueryTimeout() time.Duration {
	if c.cfg == nil {
		return 0
	}
	return c.cfg.QueryTimeout
}

// IsTimeoutError checks if an error is a query timeout error.
// Returns true for context deadline exceeded or PostgreSQL statement_timeout errors.
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for context deadline exceeded
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for PostgreSQL statement_timeout error (SQLSTATE 57014)
	// The error message contains "canceling statement due to statement timeout"
	errStr := err.Error()
	if strings.Contains(errStr, "statement timeout") ||
		strings.Contains(errStr, "57014") ||
		strings.Contains(errStr, "canceling statement") {
		return true
	}

	return false
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

	// Enable SQL query tracing (logs SQL text, timing, and backend PID at debug level)
	poolConfig.ConnConfig.Tracer = &dbTracer{logParams: cfg.LogSQLParams}

	// Log when a new TCP connection is created and set statement_timeout
	timeoutMs := 0
	if cfg.QueryTimeout > 0 {
		timeoutMs = int(cfg.QueryTimeout.Milliseconds())
	}
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		logging.Debug("pool: new connection created",
			zap.Uint32("pg_pid", conn.PgConn().PID()),
		)
		if timeoutMs > 0 {
			// SET statement_timeout in milliseconds
			_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = %d", timeoutMs))
			if err != nil {
				logging.Warn("Failed to set statement_timeout on connection",
					zap.Int("timeout_ms", timeoutMs),
					zap.Error(err))
				// Don't fail the connection, just warn
			}
		}
		return nil
	}

	logging.Debug("Configuring connection pool",
		zap.Int("max_conns", cfg.PoolSize),
		zap.Int("min_conns", cfg.PoolMaxIdle),
		zap.Duration("query_timeout", cfg.QueryTimeout),
	)

	// Create connection pool.
	// Use context.Background() for pool creation so the pool's background goroutines
	// (idle connection creation, health checks) don't depend on the caller's context.
	// The caller's ctx may have a short timeout (e.g., mount timeout) that would disrupt
	// the pool's connection management after creation.
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
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

// Pool returns the underlying connection pool
func (c *Client) Pool() *pgxpool.Pool {
	return c.pool
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

// Exec executes a SQL statement (INSERT, UPDATE, DELETE, DDL)
func (c *Client) Exec(ctx context.Context, sql string, args ...interface{}) error {
	logging.Debug("Executing statement", zap.String("sql", sql))

	_, err := c.pool.Exec(ctx, sql, args...)
	if err != nil {
		logging.Error("Statement execution failed",
			zap.String("sql", sql),
			zap.Error(err))
		return err
	}

	logging.Debug("Statement executed successfully", zap.String("sql", sql))
	return nil
}

// ExecInTransaction executes a SQL statement within a transaction that is
// always rolled back. Used to validate DDL without persisting changes.
//
// Returns nil if the statement would succeed, or an error describing the failure.
func (c *Client) ExecInTransaction(ctx context.Context, sql string, args ...interface{}) error {
	logging.Debug("Testing statement in transaction", zap.String("sql", sql))

	// Start transaction
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Always rollback - we're just testing
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			// Rollback failure after successful test is not critical
			logging.Debug("Rollback after test", zap.Error(rbErr))
		}
	}()

	// Execute the statement
	_, err = tx.Exec(ctx, sql, args...)
	if err != nil {
		logging.Debug("Statement test failed",
			zap.String("sql", sql),
			zap.Error(err))
		return err
	}

	logging.Debug("Statement test passed", zap.String("sql", sql))
	return nil
}
