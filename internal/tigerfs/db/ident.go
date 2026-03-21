package db

import "github.com/jackc/pgx/v5"

// QuoteIdent safely quotes a single SQL identifier (column, table, or schema
// name) for interpolation into SQL strings. It wraps
// pgx.Identifier{}.Sanitize(), which double-quotes the name and escapes any
// embedded double-quote characters by doubling them (e.g., my"col becomes
// "my""col"). This prevents SQL injection via crafted identifier names.
//
// Use QuoteIdent() for ANY identifier interpolated into SQL via fmt.Sprintf.
// Never use fmt.Sprintf(`"%s"`, name) directly -- that fails to escape
// embedded quotes.
//
// Do NOT use QuoteIdent() for:
//   - SQL keywords or direction strings ("ASC", "DESC")
//   - Pre-built clause strings already assembled from quoted parts
//   - Parameterized values ($1, $2) -- those are handled by pgx
//   - Hardcoded column name literals in SQL strings (e.g., "id", "filename")
//
// Examples:
//
//	QuoteIdent("id")       -> `"id"`
//	QuoteIdent(`my"col`)   -> `"my""col"`
//	fmt.Sprintf(`SELECT %s FROM %s WHERE %s = $1`, qi(col), qt(schema, table), qi(pk))
func QuoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// QuoteTable safely quotes a schema-qualified table reference for
// interpolation into SQL strings. It wraps
// pgx.Identifier{schema, table}.Sanitize(), producing output like
// "public"."users" with proper escaping of embedded double quotes in either
// the schema or table name.
//
// Use QuoteTable() whenever building a "schema"."table" reference in SQL.
// Never use fmt.Sprintf(`"%s"."%s"`, schema, table) directly.
//
// Examples:
//
//	QuoteTable("public", "users")       -> `"public"."users"`
//	QuoteTable("public", `my"table`)    -> `"public"."my""table"`
//	fmt.Sprintf(`SELECT * FROM %s`, qt(schema, table))
func QuoteTable(schema, table string) string {
	return pgx.Identifier{schema, table}.Sanitize()
}

// qi is an unexported shorthand for QuoteIdent, used within the db package
// where the SQL-building context makes the abbreviation obvious.
func qi(name string) string {
	return QuoteIdent(name)
}

// qt is an unexported shorthand for QuoteTable, used within the db package
// where the SQL-building context makes the abbreviation obvious.
func qt(schema, table string) string {
	return QuoteTable(schema, table)
}
