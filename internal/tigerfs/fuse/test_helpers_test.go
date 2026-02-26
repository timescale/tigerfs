package fuse

import (
	"os"
	"testing"
)

func getTestConnectionString(t *testing.T) string {
	t.Helper()

	if host := os.Getenv("PGHOST"); host != "" {
		return "postgres://localhost/postgres?sslmode=disable"
	}

	if connStr := os.Getenv("TEST_DATABASE_URL"); connStr != "" {
		return connStr
	}

	return ""
}
