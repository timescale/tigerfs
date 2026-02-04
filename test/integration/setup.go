package integration

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestDBSource indicates where the test database is running
type TestDBSource int

const (
	SourceLocal  TestDBSource = iota // Local PostgreSQL
	SourceDocker                     // Docker container via testcontainers
)

// MountMethod indicates the filesystem mount method to use for tests
type MountMethod string

const (
	MountMethodFUSE   MountMethod = "fuse"   // Linux FUSE (native)
	MountMethodNFS    MountMethod = "nfs"    // macOS NFS (native)
	MountMethodDocker MountMethod = "docker" // Docker container with Linux FUSE
)

// TestDBResult contains the connection info and cleanup function
type TestDBResult struct {
	ConnStr string
	Source  TestDBSource
	Cleanup func()
}

var (
	// Cached results from probing local PostgreSQL
	localPGProbeOnce   sync.Once
	localPGAvailable   bool
	localPGConnStr     string
	localPGProbeReason string
)

// probeLocalPostgreSQL checks if local PostgreSQL is available (one-time check)
func probeLocalPostgreSQL() {
	localPGProbeOnce.Do(func() {
		connStr := buildLocalConnStr()
		if connStr == "" {
			localPGAvailable = false
			localPGProbeReason = "no connection string could be built"
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		pool, err := pgxpool.New(ctx, connStr)
		if err != nil {
			localPGAvailable = false
			localPGProbeReason = fmt.Sprintf("connection failed: %v", err)
			return
		}
		defer pool.Close()

		if err := pool.Ping(ctx); err != nil {
			localPGAvailable = false
			localPGProbeReason = fmt.Sprintf("ping failed: %v", err)
			return
		}

		localPGAvailable = true
		localPGConnStr = connStr
		localPGProbeReason = ""
	})
}

// buildLocalConnStr builds a connection string from environment variables
func buildLocalConnStr() string {
	// Check for explicit test connection string
	if connStr := os.Getenv("TEST_DATABASE_URL"); connStr != "" {
		return connStr
	}

	// Build from PG environment variables
	host := os.Getenv("PGHOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("PGPORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("PGUSER")
	if user == "" {
		user = os.Getenv("USER")
	}

	database := os.Getenv("PGDATABASE")
	if database == "" {
		database = "postgres"
	}

	password := os.Getenv("PGPASSWORD")
	if password != "" {
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, database)
	}

	return fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=disable", user, host, port, database)
}

// isDockerAvailable checks if Docker is available on the system
func isDockerAvailable() bool {
	// Check if Docker socket exists
	if _, err := os.Stat("/var/run/docker.sock"); err == nil {
		return true
	}

	// Check if DOCKER_HOST is set
	if os.Getenv("DOCKER_HOST") != "" {
		return true
	}

	return false
}

// GetTestDB returns a test database connection, trying local first then Docker
// This is the primary function all integration tests should use
func GetTestDB(t *testing.T) *TestDBResult {
	t.Helper()

	// Try local PostgreSQL first
	probeLocalPostgreSQL()
	if localPGAvailable {
		return setupLocalTestDB(t, localPGConnStr)
	}

	t.Logf("Local PostgreSQL not available (%s), trying Docker...", localPGProbeReason)

	// Fall back to Docker
	if !isDockerAvailable() {
		t.Skip("Neither local PostgreSQL nor Docker available, skipping test")
		return nil
	}

	return setupDockerTestDB(t)
}

// setupLocalTestDB sets up test tables in local PostgreSQL
func setupLocalTestDB(t *testing.T, connStr string) *TestDBResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect to local PostgreSQL: %v", err)
	}

	// Create a unique schema for this test to avoid conflicts
	schemaName := fmt.Sprintf("tigerfs_test_%d", time.Now().UnixNano())

	_, err = pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	if err != nil {
		pool.Close()
		t.Fatalf("Failed to create test schema: %v", err)
	}

	// Set search path to our test schema
	schemaConnStr := connStr + fmt.Sprintf("&search_path=%s", schemaName)

	// Seed test data in the new schema
	if err := seedTestDataInSchema(ctx, connStr, schemaName); err != nil {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
		pool.Close()
		t.Fatalf("Failed to seed test data: %v", err)
	}

	pool.Close()

	t.Logf("Using local PostgreSQL with schema %s", schemaName)

	cleanup := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		cleanupPool, err := pgxpool.New(cleanupCtx, connStr)
		if err != nil {
			t.Logf("Failed to connect for cleanup: %v", err)
			return
		}
		defer cleanupPool.Close()

		_, err = cleanupPool.Exec(cleanupCtx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
		if err != nil {
			t.Logf("Failed to drop test schema: %v", err)
		}
	}

	return &TestDBResult{
		ConnStr: schemaConnStr,
		Source:  SourceLocal,
		Cleanup: cleanup,
	}
}

// setupDockerTestDB starts a PostgreSQL container and seeds test data
func setupDockerTestDB(t *testing.T) *TestDBResult {
	t.Helper()

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
	)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL container: %v", err)
	}

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Wait for PostgreSQL to be ready
	time.Sleep(2 * time.Second)

	// Seed test data
	if err := seedTestData(ctx, connStr); err != nil {
		_ = pgContainer.Terminate(ctx)
		t.Fatalf("Failed to seed test data: %v", err)
	}

	t.Log("Using Docker PostgreSQL container")

	cleanup := func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate PostgreSQL container: %v", err)
		}
	}

	return &TestDBResult{
		ConnStr: connStr,
		Source:  SourceDocker,
		Cleanup: cleanup,
	}
}

// seedTestDataInSchema creates test tables in a specific schema
func seedTestDataInSchema(ctx context.Context, connStr, schemaName string) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Create users table
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.users (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text NOT NULL,
			age int,
			active boolean DEFAULT true,
			created_at timestamp DEFAULT NOW()
		)
	`, schemaName))
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Insert test users
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.users (name, email, age, active) VALUES
		('Alice', 'alice@example.com', 30, true),
		('Bob', 'bob@example.com', 25, true),
		('Charlie', 'charlie@example.com', 35, false)
	`, schemaName))
	if err != nil {
		return fmt.Errorf("failed to insert test users: %w", err)
	}

	// Create products table
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.products (
			id serial PRIMARY KEY,
			name text NOT NULL,
			price numeric(10,2) NOT NULL,
			in_stock boolean DEFAULT true
		)
	`, schemaName))
	if err != nil {
		return fmt.Errorf("failed to create products table: %w", err)
	}

	// Insert test products
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.products (name, price, in_stock) VALUES
		('Widget', 19.99, true),
		('Gadget', 29.99, true),
		('Doohickey', 39.99, false)
	`, schemaName))
	if err != nil {
		return fmt.Errorf("failed to insert test products: %w", err)
	}

	return nil
}

// seedTestData creates test tables and inserts test data (for Docker - uses public schema)
func seedTestData(ctx context.Context, connStr string) error {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Create users table
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id serial PRIMARY KEY,
			name text NOT NULL,
			email text NOT NULL,
			age int,
			active boolean DEFAULT true,
			created_at timestamp DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Insert test users
	_, err = pool.Exec(ctx, `
		INSERT INTO users (name, email, age, active) VALUES
		('Alice', 'alice@example.com', 30, true),
		('Bob', 'bob@example.com', 25, true),
		('Charlie', 'charlie@example.com', 35, false)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert test users: %w", err)
	}

	// Create products table
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS products (
			id serial PRIMARY KEY,
			name text NOT NULL,
			price numeric(10,2) NOT NULL,
			in_stock boolean DEFAULT true
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create products table: %w", err)
	}

	// Insert test products
	_, err = pool.Exec(ctx, `
		INSERT INTO products (name, price, in_stock) VALUES
		('Widget', 19.99, true),
		('Gadget', 29.99, true),
		('Doohickey', 39.99, false)
	`)
	if err != nil {
		return fmt.Errorf("failed to insert test products: %w", err)
	}

	return nil
}

// SetupTestDB is kept for backward compatibility - wraps GetTestDB
// Deprecated: Use GetTestDB instead
func SetupTestDB(t *testing.T) (string, func()) {
	t.Helper()
	result := GetTestDB(t)
	if result == nil {
		return "", func() {}
	}
	return result.ConnStr, result.Cleanup
}

// isFUSEAvailable checks if FUSE is available on the system.
// On macOS, FUSE (macFUSE) is no longer supported; use NFS instead.
// On Linux, checks for /dev/fuse.
func isFUSEAvailable() bool {
	// On Linux, check for /dev/fuse
	if _, err := os.Stat("/dev/fuse"); err == nil {
		return true
	}

	// macFUSE is no longer available/maintained on macOS
	// Tests should use NFS on macOS instead
	return false
}

// isNFSAvailable checks if NFS mounting is available on the system.
// On macOS, NFS is built into the OS and always available.
// On Linux, NFS is not used for integration tests (use FUSE instead).
func isNFSAvailable() bool {
	// Check if we're on macOS by looking for mount_nfs
	if _, err := os.Stat("/sbin/mount_nfs"); err == nil {
		return true
	}

	return false
}

// isMountAvailable checks if any filesystem mount method is available.
// Returns true if FUSE (Linux) or NFS (macOS) is available.
func isMountAvailable() (available bool, method string) {
	if isFUSEAvailable() {
		return true, "fuse"
	}
	if isNFSAvailable() {
		return true, "nfs"
	}
	return false, ""
}

// getMountMethod determines the mount method to use based on platform and environment.
//
// On Linux: Always uses FUSE (native).
// On macOS: Uses TEST_MOUNT_METHOD environment variable:
//   - "nfs" (default): Use native macOS NFS
//   - "docker": Run tests in a Docker container with Linux FUSE
//
// Returns the mount method and a reason string if skipping.
func getMountMethod(t *testing.T) (MountMethod, string) {
	t.Helper()

	if runtime.GOOS == "linux" {
		if !isFUSEAvailable() {
			return "", "FUSE not available on Linux (/dev/fuse not found)"
		}
		return MountMethodFUSE, ""
	}

	if runtime.GOOS == "darwin" {
		method := os.Getenv("TEST_MOUNT_METHOD")
		switch method {
		case "", "nfs":
			// Default to NFS on macOS
			if !isNFSAvailable() {
				return "", "NFS not available on macOS (/sbin/mount_nfs not found)"
			}
			return MountMethodNFS, ""
		case "docker":
			if !isDockerAvailable() {
				return "", "Docker not available for TEST_MOUNT_METHOD=docker"
			}
			return MountMethodDocker, ""
		default:
			return "", fmt.Sprintf("invalid TEST_MOUNT_METHOD=%q (use 'nfs' or 'docker')", method)
		}
	}

	return "", fmt.Sprintf("unsupported platform: %s", runtime.GOOS)
}
