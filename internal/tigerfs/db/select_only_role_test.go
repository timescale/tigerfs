package db

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// setupRoleParityFixture provisions a dedicated test schema, a SELECT-only role,
// and fixture objects (tables + view) shared by all TestRoleParity_* tests.
//
// Returns:
//   - owner: a client connected with the env-default credentials. Owns the schema.
//   - selectOnly: a client connected as a freshly-created role with USAGE on the
//     schema and SELECT on every fixture object (no other privileges).
//   - schema: the unique schema name. Use as the schema argument to all functions
//     under test so they observe only the fixture, not other objects in public.
//
// Cleanup (drop schema, revoke, drop role, close clients) is registered with
// t.Cleanup, so it runs after the test even on failure.
func setupRoleParityFixture(t *testing.T) (owner, selectOnly *Client, schema string) {
	t.Helper()

	if getTestConnectionString(t) == "" {
		t.Skip("No PostgreSQL connection available (set PGHOST or skip)")
	}

	// Build a fully-populated connstr from env vars. We can't reuse the bare
	// "postgres://localhost/postgres" from getTestConnectionString because
	// NewClient's password injection requires user@host syntax in the URL.
	host := os.Getenv("PGHOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("PGPORT")
	if port == "" {
		port = "5432"
	}
	dbname := os.Getenv("PGDATABASE")
	if dbname == "" {
		dbname = "postgres"
	}
	ownerUser := os.Getenv("PGUSER")
	if ownerUser == "" {
		ownerUser = "postgres"
	}
	ownerPassword := os.Getenv("PGPASSWORD")

	ownerConnStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		url.QueryEscape(ownerUser),
		url.QueryEscape(ownerPassword),
		host, port, dbname,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	cfg := &config.Config{PoolSize: 5, PoolMaxIdle: 2}

	ownerClient, err := NewClient(ctx, cfg, ownerConnStr)
	if err != nil {
		t.Fatalf("connect as owner: %v", err)
	}

	// Unique-per-test identifiers. Sanitize t.Name() so it's a legal SQL identifier
	// and short enough to avoid PG's 63-char limit.
	stamp := time.Now().UnixNano()
	sanitized := sanitizeIdent(t.Name())
	if len(sanitized) > 20 {
		sanitized = sanitized[:20]
	}
	schema = fmt.Sprintf("tigerfs_test_%s_%d", sanitized, stamp)
	roleName := fmt.Sprintf("tigerfs_role_%s_%d", sanitized, stamp)
	rolePassword := fmt.Sprintf("p_%d", stamp)

	// Provision schema, role, fixture objects, and grants atomically.
	// Use individual statements rather than a single multi-statement command so
	// errors point at the failing step.
	setup := []string{
		fmt.Sprintf(`CREATE SCHEMA %s`, QuoteIdent(schema)),
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD %s`, QuoteIdent(roleName), pgQuoteString(rolePassword)),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, QuoteIdent(schema), QuoteIdent(roleName)),

		// Fixture: single-column PK.
		fmt.Sprintf(`CREATE TABLE %s.single_pk (id int PRIMARY KEY, name text NOT NULL)`, QuoteIdent(schema)),
		fmt.Sprintf(`INSERT INTO %s.single_pk SELECT g, 'row_'||g FROM generate_series(1, 5) g`, QuoteIdent(schema)),

		// Fixture: composite PK in non-alphabetical order to catch ordering bugs.
		fmt.Sprintf(`CREATE TABLE %s.composite_pk (a int, b text, c date, val text, PRIMARY KEY (b, a, c))`, QuoteIdent(schema)),

		// Fixture: table with no primary key.
		fmt.Sprintf(`CREATE TABLE %s.no_pk (id int, name text)`, QuoteIdent(schema)),

		// Fixture: table with both PK and a UNIQUE constraint.
		fmt.Sprintf(`CREATE TABLE %s.with_unique (id int PRIMARY KEY, code text NOT NULL, CONSTRAINT with_unique_code_key UNIQUE (code))`, QuoteIdent(schema)),

		// Fixture: simple, updatable view over single_pk.
		fmt.Sprintf(`CREATE VIEW %s.single_pk_view AS SELECT id, name FROM %s.single_pk`, QuoteIdent(schema), QuoteIdent(schema)),

		// Grant SELECT on every fixture object to the SELECT-only role.
		// Intentionally NOT using "GRANT SELECT ON ALL TABLES" so the helper stays
		// explicit about what's reachable.
		fmt.Sprintf(`GRANT SELECT ON %s.single_pk TO %s`, QuoteIdent(schema), QuoteIdent(roleName)),
		fmt.Sprintf(`GRANT SELECT ON %s.composite_pk TO %s`, QuoteIdent(schema), QuoteIdent(roleName)),
		fmt.Sprintf(`GRANT SELECT ON %s.no_pk TO %s`, QuoteIdent(schema), QuoteIdent(roleName)),
		fmt.Sprintf(`GRANT SELECT ON %s.with_unique TO %s`, QuoteIdent(schema), QuoteIdent(roleName)),
		fmt.Sprintf(`GRANT SELECT ON %s.single_pk_view TO %s`, QuoteIdent(schema), QuoteIdent(roleName)),
	}
	for _, stmt := range setup {
		if _, err := ownerClient.pool.Exec(ctx, stmt); err != nil {
			_ = ownerClient.Close()
			t.Fatalf("setup statement failed: %v\nSQL: %s", err, stmt)
		}
	}

	// Build a connection string for the SELECT-only role; reuse host/port/dbname
	// from the owner connstr above.
	roleConnStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		url.QueryEscape(roleName),
		url.QueryEscape(rolePassword),
		host, port, dbname,
	)

	// Build the SELECT-only pool directly via pgxpool. We can't go through
	// NewClient because TigerFS's resolvePassword would inject PGPASSWORD
	// (the owner's password) and overwrite the role's password in the URL.
	rolePoolCfg, err := pgxpool.ParseConfig(roleConnStr)
	if err != nil {
		_ = ownerClient.Close()
		t.Fatalf("parse select-only connstr: %v", err)
	}
	rolePoolCfg.MaxConns = int32(cfg.PoolSize)
	rolePoolCfg.MinConns = int32(cfg.PoolMaxIdle)
	rolePool, err := pgxpool.NewWithConfig(context.Background(), rolePoolCfg)
	if err != nil {
		// Best-effort cleanup before failing so we don't leak the role/schema.
		_, _ = ownerClient.pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, QuoteIdent(schema)))
		_, _ = ownerClient.pool.Exec(context.Background(), fmt.Sprintf(`DROP ROLE %s`, QuoteIdent(roleName)))
		_ = ownerClient.Close()
		t.Fatalf("connect as select-only role: %v", err)
	}
	if err := rolePool.Ping(ctx); err != nil {
		rolePool.Close()
		_, _ = ownerClient.pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, QuoteIdent(schema)))
		_, _ = ownerClient.pool.Exec(context.Background(), fmt.Sprintf(`DROP ROLE %s`, QuoteIdent(roleName)))
		_ = ownerClient.Close()
		t.Fatalf("ping as select-only role: %v", err)
	}
	selectOnlyClient := &Client{pool: rolePool, cfg: cfg}

	t.Cleanup(func() {
		_ = selectOnlyClient.Close()
		// Use a fresh background context: the test's context may already be cancelled.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		// CASCADE drops the dependent view, tables, and grants automatically.
		if _, err := ownerClient.pool.Exec(cleanupCtx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, QuoteIdent(schema))); err != nil {
			t.Logf("cleanup: drop schema %q failed: %v", schema, err)
		}
		// Role grants on dropped objects vanish with the drop, but a role can still
		// hold default privileges or other grants. REVOKE everything to be safe.
		if _, err := ownerClient.pool.Exec(cleanupCtx, fmt.Sprintf(`DROP OWNED BY %s`, QuoteIdent(roleName))); err != nil {
			t.Logf("cleanup: drop owned by %q failed: %v", roleName, err)
		}
		if _, err := ownerClient.pool.Exec(cleanupCtx, fmt.Sprintf(`DROP ROLE %s`, QuoteIdent(roleName))); err != nil {
			t.Logf("cleanup: drop role %q failed: %v", roleName, err)
		}
		_ = ownerClient.Close()
	})

	return ownerClient, selectOnlyClient, schema
}

// sanitizeIdent converts an arbitrary string (e.g. t.Name(), which contains "/")
// into a snake_case identifier suitable for use in a SQL identifier suffix.
func sanitizeIdent(s string) string {
	s = strings.ToLower(s)
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// pgQuoteString quotes a string literal for use in DDL (e.g. CREATE ROLE ... PASSWORD '...').
// Use only for values that must appear literally in the SQL text (PASSWORD doesn't accept
// a parameter placeholder). For all other values, use $N parameters.
func pgQuoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// roleParityCase is one (subtest name, client) pair for table-driven role-parity tests.
type roleParityCase struct {
	name   string
	client *Client
}

// roleParityCases returns the standard two-role test matrix.
func roleParityCases(owner, selectOnly *Client) []roleParityCase {
	return []roleParityCase{
		{"owner", owner},
		{"select_only", selectOnly},
	}
}

// TestRoleParity_GetPrimaryKey_Single verifies that GetPrimaryKey returns the
// single-column PK consistently for both the owner role and a SELECT-only role.
//
// On `main` (before fix), the select_only subtest fails because
// information_schema.table_constraints is privilege-filtered to non-SELECT
// privileges. After switching to pg_constraint, both subtests pass.
func TestRoleParity_GetPrimaryKey_Single(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			pk, err := tc.client.GetPrimaryKey(ctx, schema, "single_pk")
			if err != nil {
				t.Fatalf("GetPrimaryKey: %v", err)
			}
			if len(pk.Columns) != 1 || pk.Columns[0] != "id" {
				t.Errorf("expected PK columns [id], got %v", pk.Columns)
			}
		})
	}
}

// TestRoleParity_GetPrimaryKey_Composite verifies composite PK column ordering
// for both roles. The fixture declares PRIMARY KEY (b, a, c) in non-alphabetical
// order to catch ordering bugs in the array_position(conkey, attnum) clause.
func TestRoleParity_GetPrimaryKey_Composite(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			pk, err := tc.client.GetPrimaryKey(ctx, schema, "composite_pk")
			if err != nil {
				t.Fatalf("GetPrimaryKey: %v", err)
			}
			want := []string{"b", "a", "c"}
			if len(pk.Columns) != len(want) {
				t.Fatalf("expected %d PK columns, got %d: %v", len(want), len(pk.Columns), pk.Columns)
			}
			for i, c := range want {
				if pk.Columns[i] != c {
					t.Errorf("PK column %d: expected %q, got %q (full: %v)", i, c, pk.Columns[i], pk.Columns)
				}
			}
		})
	}
}

// TestRoleParity_GetUniqueConstraints verifies that the internal
// getUniqueConstraints helper sees both the PRIMARY KEY and the UNIQUE
// constraint on the fixture's with_unique table for both roles. Before the
// fix the select_only subtest returns zero constraints (same root cause as
// GetPrimaryKey).
//
// Even though getUniqueConstraints is only reached on write paths (which
// SELECT-only roles can't exercise), fixing it eliminates the privilege-filter
// footgun globally and keeps constraint discovery consistent across the package.
func TestRoleParity_GetUniqueConstraints(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			constraints, err := getUniqueConstraints(ctx, tc.client.pool, schema, "with_unique")
			if err != nil {
				t.Fatalf("getUniqueConstraints: %v", err)
			}
			// Expect 2 constraints: with_unique_pkey (PRIMARY KEY on id) and
			// with_unique_code_key (UNIQUE on code).
			if len(constraints) != 2 {
				t.Fatalf("expected 2 constraints, got %d: %+v", len(constraints), constraints)
			}
			// Build a map for assertion order-independence.
			byName := map[string][]string{}
			for _, c := range constraints {
				byName[c.Name] = c.Columns
			}
			if cols, ok := byName["with_unique_pkey"]; !ok || len(cols) != 1 || cols[0] != "id" {
				t.Errorf("expected with_unique_pkey [id], got %v", cols)
			}
			if cols, ok := byName["with_unique_code_key"]; !ok || len(cols) != 1 || cols[0] != "code" {
				t.Errorf("expected with_unique_code_key [code], got %v", cols)
			}
		})
	}
}

// TestRoleParity_GetTableDDL verifies that GetTableDDL produces identical
// output for both roles, including the PRIMARY KEY clause. Before the fix,
// the select_only subtest sees DDL missing the "PRIMARY KEY (id)" clause
// because the PK-discovery sub-query in GetTableDDL also hits
// information_schema.table_constraints.
func TestRoleParity_GetTableDDL(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			ddl, err := tc.client.GetTableDDL(ctx, schema, "single_pk")
			if err != nil {
				t.Fatalf("GetTableDDL: %v", err)
			}
			if !strings.Contains(ddl, "PRIMARY KEY") {
				t.Errorf("expected DDL to contain PRIMARY KEY clause, got:\n%s", ddl)
			}
			if !strings.Contains(ddl, "CREATE TABLE") {
				t.Errorf("expected DDL to contain CREATE TABLE, got:\n%s", ddl)
			}
		})
	}
}

// TestRoleParity_GetPrimaryKey_NoPK verifies the "no primary key" error path
// returns identically for both roles. Before the fix, the select_only subtest
// would also error out, but for the wrong reason (privilege filter rather than
// genuinely missing PK). After the fix, both roles correctly identify the
// no-PK case.
func TestRoleParity_GetPrimaryKey_NoPK(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.client.GetPrimaryKey(ctx, schema, "no_pk")
			if err == nil {
				t.Fatal("expected error for table without PK, got nil")
			}
			if !strings.Contains(err.Error(), "no primary key") {
				t.Errorf("expected error to mention 'no primary key', got: %v", err)
			}
		})
	}
}

// TestRoleParity_GetSchemas verifies both roles see the dedicated test schema.
// information_schema.schemata is filtered by USAGE privilege; the SELECT-only
// role has USAGE granted in the fixture.
func TestRoleParity_GetSchemas(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			schemas, err := GetSchemas(ctx, tc.client.pool)
			if err != nil {
				t.Fatalf("GetSchemas: %v", err)
			}
			if !containsString(schemas, schema) {
				t.Errorf("expected schemas to include %q, got %v", schema, schemas)
			}
		})
	}
}

// TestRoleParity_GetTables verifies both roles see the fixture tables.
// information_schema.tables is filtered to tables on which the role has any
// privilege; SELECT is sufficient.
func TestRoleParity_GetTables(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			tables, err := GetTables(ctx, tc.client.pool, schema)
			if err != nil {
				t.Fatalf("GetTables: %v", err)
			}
			for _, want := range []string{"single_pk", "composite_pk", "no_pk", "with_unique"} {
				if !containsString(tables, want) {
					t.Errorf("expected tables to include %q, got %v", want, tables)
				}
			}
		})
	}
}

// TestRoleParity_GetViews verifies both roles see the fixture view.
func TestRoleParity_GetViews(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			views, err := GetViews(ctx, tc.client.pool, schema)
			if err != nil {
				t.Fatalf("GetViews: %v", err)
			}
			if !containsString(views, "single_pk_view") {
				t.Errorf("expected views to include single_pk_view, got %v", views)
			}
		})
	}
}

// TestRoleParity_IsViewUpdatable verifies both roles get the same updatability
// answer for a simple view. The fixture view is a non-aggregated SELECT on a
// single table, so PG reports it as updatable.
func TestRoleParity_IsViewUpdatable(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			updatable, err := IsViewUpdatable(ctx, tc.client.pool, schema, "single_pk_view")
			if err != nil {
				t.Fatalf("IsViewUpdatable: %v", err)
			}
			if !updatable {
				t.Errorf("expected single_pk_view to be updatable, got false")
			}
		})
	}
}

// TestRoleParity_GetColumns verifies column-listing parity for both roles.
func TestRoleParity_GetColumns(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			cols, err := GetColumns(ctx, tc.client.pool, schema, "single_pk")
			if err != nil {
				t.Fatalf("GetColumns: %v", err)
			}
			if len(cols) != 2 {
				t.Fatalf("expected 2 columns, got %d: %+v", len(cols), cols)
			}
			if cols[0].Name != "id" || cols[1].Name != "name" {
				t.Errorf("expected columns [id, name], got [%s, %s]", cols[0].Name, cols[1].Name)
			}
		})
	}
}

// TestRoleParity_GetColumnsForConstraintCheck verifies the internal
// constraint-check column helper sees columns for both roles.
func TestRoleParity_GetColumnsForConstraintCheck(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			cols, err := getColumnsForConstraintCheck(ctx, tc.client.pool, schema, "single_pk")
			if err != nil {
				t.Fatalf("getColumnsForConstraintCheck: %v", err)
			}
			if len(cols) != 2 {
				t.Errorf("expected 2 columns, got %d: %+v", len(cols), cols)
			}
		})
	}
}

// TestRoleParity_TableExists verifies TableExists returns true for granted
// tables and false for missing tables, identically for both roles.
func TestRoleParity_TableExists(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range roleParityCases(owner, selectOnly) {
		t.Run(tc.name, func(t *testing.T) {
			exists, err := tc.client.TableExists(ctx, schema, "single_pk")
			if err != nil {
				t.Fatalf("TableExists(single_pk): %v", err)
			}
			if !exists {
				t.Errorf("expected single_pk to exist, got false")
			}

			exists, err = tc.client.TableExists(ctx, schema, "nonexistent_table")
			if err != nil {
				t.Fatalf("TableExists(nonexistent): %v", err)
			}
			if exists {
				t.Errorf("expected nonexistent_table to not exist, got true")
			}
		})
	}
}

// TestRoleParity_TableWithoutGrant_NotVisible is a negative test verifying
// the asymmetry between roles: a table created without a SELECT grant should
// be visible to the owner but not to the SELECT-only role. Catches accidental
// over-granting in the fixture helper.
func TestRoleParity_TableWithoutGrant_NotVisible(t *testing.T) {
	owner, selectOnly, schema := setupRoleParityFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create an extra table in the test schema but DON'T grant it to the
	// SELECT-only role.
	hiddenTable := "hidden_no_grant"
	stmt := fmt.Sprintf(`CREATE TABLE %s.%s (id int PRIMARY KEY)`, QuoteIdent(schema), QuoteIdent(hiddenTable))
	if _, err := owner.pool.Exec(ctx, stmt); err != nil {
		t.Fatalf("create hidden table: %v", err)
	}

	ownerTables, err := GetTables(ctx, owner.pool, schema)
	if err != nil {
		t.Fatalf("GetTables(owner): %v", err)
	}
	if !containsString(ownerTables, hiddenTable) {
		t.Errorf("owner should see %q, got %v", hiddenTable, ownerTables)
	}

	selectOnlyTables, err := GetTables(ctx, selectOnly.pool, schema)
	if err != nil {
		t.Fatalf("GetTables(select_only): %v", err)
	}
	if containsString(selectOnlyTables, hiddenTable) {
		t.Errorf("select_only role should NOT see %q (no grant), got %v", hiddenTable, selectOnlyTables)
	}
}

// containsString reports whether the slice contains target.
func containsString(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}
