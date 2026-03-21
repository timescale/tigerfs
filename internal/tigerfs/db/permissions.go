package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// TablePermissions represents the PostgreSQL privileges a user has on a table.
// These permissions determine what filesystem operations are allowed:
//   - SELECT → read files (cat, less)
//   - INSERT → create files (write new rows)
//   - UPDATE → modify files (edit existing rows)
//   - DELETE → remove files (rm rows)
type TablePermissions struct {
	// CanSelect indicates whether the user can read rows from the table.
	// Maps to read permission on row files.
	CanSelect bool

	// CanInsert indicates whether the user can insert new rows.
	// Maps to write permission for creating new row files.
	CanInsert bool

	// CanUpdate indicates whether the user can modify existing rows.
	// Maps to write permission on existing row files.
	CanUpdate bool

	// CanDelete indicates whether the user can delete rows.
	// Maps to the ability to rm row files.
	CanDelete bool
}

// IsReadOnly returns true if the user only has SELECT privilege.
// Useful for determining if a table should be mounted read-only.
func (p *TablePermissions) IsReadOnly() bool {
	return p.CanSelect && !p.CanInsert && !p.CanUpdate && !p.CanDelete
}

// HasAnyPermission returns true if the user has at least one privilege.
// Tables with no permissions should not be visible in the filesystem.
func (p *TablePermissions) HasAnyPermission() bool {
	return p.CanSelect || p.CanInsert || p.CanUpdate || p.CanDelete
}

// GetTablePermissions queries PostgreSQL to determine what privileges
// the current user has on a specific table.
//
// Uses PostgreSQL's has_table_privilege() function which checks the
// actual effective permissions, including those granted through roles.
//
// Parameters:
//   - ctx: Context for cancellation
//   - pool: PostgreSQL connection pool
//   - schema: PostgreSQL schema name
//   - table: Table name to check permissions for
//
// Returns the permissions struct, or error on database failure.
func GetTablePermissions(ctx context.Context, pool *pgxpool.Pool, schema, table string) (*TablePermissions, error) {
	logging.Debug("Getting table permissions",
		zap.String("schema", schema),
		zap.String("table", table))

	// Use schema-qualified table name for accurate privilege check.
	// The has_table_privilege function accepts various formats; we use
	// the fully qualified name to avoid ambiguity.
	qualifiedName := qt(schema, table)

	query := `
		SELECT
			has_table_privilege($1, 'SELECT') as can_select,
			has_table_privilege($1, 'INSERT') as can_insert,
			has_table_privilege($1, 'UPDATE') as can_update,
			has_table_privilege($1, 'DELETE') as can_delete
	`

	var perms TablePermissions
	err := pool.QueryRow(ctx, query, qualifiedName).Scan(
		&perms.CanSelect,
		&perms.CanInsert,
		&perms.CanUpdate,
		&perms.CanDelete,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get table permissions: %w", err)
	}

	logging.Debug("Got table permissions",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Bool("select", perms.CanSelect),
		zap.Bool("insert", perms.CanInsert),
		zap.Bool("update", perms.CanUpdate),
		zap.Bool("delete", perms.CanDelete))

	return &perms, nil
}

// GetTablePermissions is a convenience wrapper for Client.
//
// Parameters:
//   - ctx: Context for cancellation
//   - schema: PostgreSQL schema name
//   - table: Table name to check permissions for
//
// Returns the permissions struct, or error if the database connection
// is not initialized or the query fails.
func (c *Client) GetTablePermissions(ctx context.Context, schema, table string) (*TablePermissions, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetTablePermissions(ctx, c.pool, schema, table)
}

// GetTablePermissionsBatch queries PostgreSQL to determine privileges
// for the current user on multiple tables in a single query.
// This eliminates N serial round-trips when refreshing schema metadata.
//
// Parameters:
//   - ctx: Context for cancellation
//   - pool: PostgreSQL connection pool
//   - schema: PostgreSQL schema name
//   - tables: Table names to check permissions for
//
// Returns a map of table name to permissions, or error on database failure.
func GetTablePermissionsBatch(ctx context.Context, pool *pgxpool.Pool, schema string, tables []string) (map[string]*TablePermissions, error) {
	if len(tables) == 0 {
		return make(map[string]*TablePermissions), nil
	}

	logging.Debug("Getting batch table permissions",
		zap.String("schema", schema),
		zap.Int("table_count", len(tables)))

	query := `
		SELECT t.table_name,
		       has_table_privilege(format('"%s"."%s"', $1::text, t.table_name), 'SELECT'),
		       has_table_privilege(format('"%s"."%s"', $1::text, t.table_name), 'INSERT'),
		       has_table_privilege(format('"%s"."%s"', $1::text, t.table_name), 'UPDATE'),
		       has_table_privilege(format('"%s"."%s"', $1::text, t.table_name), 'DELETE')
		FROM unnest($2::text[]) AS t(table_name)
	`

	rows, err := pool.Query(ctx, query, schema, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch table permissions: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*TablePermissions, len(tables))
	for rows.Next() {
		var tableName string
		var perms TablePermissions
		if err := rows.Scan(&tableName, &perms.CanSelect, &perms.CanInsert, &perms.CanUpdate, &perms.CanDelete); err != nil {
			return nil, fmt.Errorf("failed to scan batch permissions: %w", err)
		}
		result[tableName] = &perms
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating batch permissions: %w", err)
	}

	logging.Debug("Got batch table permissions",
		zap.String("schema", schema),
		zap.Int("table_count", len(result)))

	return result, nil
}

// GetTablePermissionsBatch is a convenience wrapper for Client.
func (c *Client) GetTablePermissionsBatch(ctx context.Context, schema string, tables []string) (map[string]*TablePermissions, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return GetTablePermissionsBatch(ctx, c.pool, schema, tables)
}
