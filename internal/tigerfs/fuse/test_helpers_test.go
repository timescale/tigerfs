package fuse

import (
	"fmt"
	"net/url"
	"os"
	"testing"
)

// getTestConnectionString returns a Postgres connection string built from the
// PG* environment variables. If TEST_DATABASE_URL is set, returns it verbatim.
// Returns "" if no PG endpoint is reachable.
//
// We build a fully-populated postgres://user:pass@host:port/db URL (not a bare
// postgres://host/db URL) because db.NewClient's injectPasswordIntoConnStr
// requires user@host in the URL to inject the resolved password; a bare URL
// errors out with "connection string has no user@host format" whenever
// PGPASSWORD is set in the environment (e.g., in CI). Mirrors the same fix
// applied to db/client_test.go's getTestConnectionString helper.
func getTestConnectionString(t *testing.T) string {
	t.Helper()

	if connStr := os.Getenv("TEST_DATABASE_URL"); connStr != "" {
		return connStr
	}

	host := os.Getenv("PGHOST")
	if host == "" {
		return ""
	}

	port := os.Getenv("PGPORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("PGUSER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("PGPASSWORD")
	dbname := os.Getenv("PGDATABASE")
	if dbname == "" {
		dbname = "postgres"
	}

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		url.QueryEscape(user),
		url.QueryEscape(password),
		host, port, dbname,
	)
}
