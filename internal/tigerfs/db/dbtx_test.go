package db

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestDBTX_PoolSatisfiesInterface verifies at compile time that
// *pgxpool.Pool satisfies the DBTX interface.
func TestDBTX_PoolSatisfiesInterface(t *testing.T) {
	var _ DBTX = (*pgxpool.Pool)(nil)
}

// TestDBTX_TxSatisfiesInterface verifies at compile time that
// pgx.Tx satisfies the DBTX interface.
func TestDBTX_TxSatisfiesInterface(t *testing.T) {
	var _ DBTX = (pgx.Tx)(nil)
}
