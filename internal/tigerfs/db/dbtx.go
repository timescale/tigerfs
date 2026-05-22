package db

import (
	"context"

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
// manage their own pgx.Tx directly.
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
