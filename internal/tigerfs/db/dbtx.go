package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBTX is the common query interface satisfied by *pgxpool.Pool and pgx.Tx.
// Package-level functions accept DBTX so they can operate against either
// a raw pool connection or a transaction with SET LOCAL session variables.
//
// This interface intentionally excludes SendBatch, CopyFrom, and Begin.
// Operations that need those capabilities (bulk import, DDL validation)
// manage their own pgx.Tx directly and inject session vars via
// applySessionVars.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Compile-time verification that both pool and transaction satisfy DBTX.
var (
	_ DBTX = (*pgxpool.Pool)(nil)
	_ DBTX = (pgx.Tx)(nil)
)

// applySessionVars executes SET LOCAL for each session variable within an
// open transaction. Uses set_config($1, $2, true) which is the parameterized
// equivalent of SET LOCAL — safe against injection, transaction-scoped,
// and compatible with PgBouncer transaction mode and RDS Proxy.
//
// Keys are iterated in sorted order (pre-sorted at SessionVars construction)
// for deterministic execution with zero per-query allocation.
func applySessionVars(ctx context.Context, tx pgx.Tx, vars SessionVars) error {
	var applyErr error
	vars.Range(func(key, value string) {
		if applyErr != nil {
			return
		}
		if _, err := tx.Exec(ctx, "SELECT set_config($1, $2, true)", key, value); err != nil {
			applyErr = fmt.Errorf("set session var %q: %w", key, err)
		}
	})
	return applyErr
}
