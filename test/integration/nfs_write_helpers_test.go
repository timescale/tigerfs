package integration

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Register pgx driver for database/sql
)

// nfsWriteTestContext holds the test context for NFS write tests.
// It provides helpers for database queries and cleanup.
type nfsWriteTestContext struct {
	t          *testing.T
	mountPoint string
	connStr    string
	db         *sql.DB
	cleanup    func()
}

// setupNFSWriteTestContext creates a test context with a mounted NFS filesystem
// and a test table with id (int), name (text), and data (text) columns.
func setupNFSWriteTestContext(t *testing.T) *nfsWriteTestContext {
	t.Helper()

	checkFUSEMountCapability(t)

	dbResult := GetTestDBEmpty(t)
	if dbResult == nil {
		t.Skip("database not available")
		return nil
	}

	cfg := defaultTestConfig()
	mountpoint := t.TempDir()

	filesystem := mountWithTimeout(t, cfg, dbResult.ConnStr, mountpoint, 10*time.Second)
	if filesystem == nil {
		dbResult.Cleanup()
		t.Skip("mount not available")
		return nil
	}

	time.Sleep(500 * time.Millisecond)

	// Create test table via DDL
	tableName := "write_test"
	createDir := filepath.Join(mountpoint, ".create", tableName)
	if err := os.MkdirAll(createDir, 0755); err != nil {
		filesystem.Close()
		dbResult.Cleanup()
		t.Fatalf("Failed to mkdir: %v", err)
	}

	sqlPath := filepath.Join(createDir, "sql")
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INT PRIMARY KEY,
		name TEXT,
		data TEXT
	);
	INSERT INTO %s (id, name, data) VALUES (1, 'initial_name', 'initial_data')
	ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, data = EXCLUDED.data;`,
		tableName, tableName)

	if err := os.WriteFile(sqlPath, []byte(ddl), 0644); err != nil {
		filesystem.Close()
		dbResult.Cleanup()
		t.Fatalf("Failed to write DDL: %v", err)
	}

	commitPath := filepath.Join(createDir, ".commit")
	if err := touchTriggerFile(t, commitPath); err != nil {
		filesystem.Close()
		dbResult.Cleanup()
		t.Fatalf("Failed to commit: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Open database connection for verification queries
	db, err := sql.Open("pgx", dbResult.ConnStr)
	if err != nil {
		filesystem.Close()
		dbResult.Cleanup()
		t.Fatalf("Failed to open database: %v", err)
	}

	return &nfsWriteTestContext{
		t:          t,
		mountPoint: mountpoint,
		connStr:    dbResult.ConnStr,
		db:         db,
		cleanup: func() {
			db.Close()
			filesystem.Close()
			dbResult.Cleanup()
		},
	}
}

// queryColumn returns the value of a column for a given table and row ID.
func (ctx *nfsWriteTestContext) queryColumn(table string, id int, column string) string {
	ctx.t.Helper()

	query := fmt.Sprintf("SELECT %s FROM %s WHERE id = $1", column, table)
	var value sql.NullString
	err := ctx.db.QueryRow(query, id).Scan(&value)
	if err != nil {
		ctx.t.Fatalf("Failed to query column %s.%s for id=%d: %v", table, column, id, err)
	}
	if !value.Valid {
		return "" // Return empty string for NULL
	}
	return value.String
}

// execSQL executes a SQL statement.
func (ctx *nfsWriteTestContext) execSQL(query string, args ...interface{}) {
	ctx.t.Helper()

	_, err := ctx.db.Exec(query, args...)
	if err != nil {
		ctx.t.Fatalf("Failed to execute SQL: %v", err)
	}
}
