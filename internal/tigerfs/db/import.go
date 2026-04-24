package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// ImportOverwrite replaces all table data with new rows.
// Executes TRUNCATE followed by bulk INSERT in a single transaction.
// All-or-nothing: rolls back on any error.
func (c *Client) ImportOverwrite(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error {
	logging.Debug("ImportOverwrite starting",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("columns", len(columns)),
		zap.Int("rows", len(rows)))

	if len(rows) == 0 {
		// Just truncate if no rows
		return c.truncateTable(ctx, schema, table)
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Apply session variables (SET LOCAL) if configured
	if vars := c.effectiveSessionVars(ctx); len(vars) > 0 {
		if err := applySessionVars(ctx, tx, vars); err != nil {
			return fmt.Errorf("failed to apply session variables: %w", err)
		}
	}

	// Truncate table
	truncateSQL := fmt.Sprintf("TRUNCATE %s",
		qt(schema, table))
	if _, err := tx.Exec(ctx, truncateSQL); err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	// Bulk insert
	if err := c.bulkInsert(ctx, tx, schema, table, columns, rows); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logging.Debug("ImportOverwrite completed",
		zap.String("table", table),
		zap.Int("rows_inserted", len(rows)))

	return nil
}

// ImportSync performs upsert operations for all rows.
// Uses INSERT ... ON CONFLICT DO UPDATE to insert new rows and update existing ones.
// Requires the table to have a primary key.
// All-or-nothing: rolls back on any error.
func (c *Client) ImportSync(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error {
	logging.Debug("ImportSync starting",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("columns", len(columns)),
		zap.Int("rows", len(rows)))

	if len(rows) == 0 {
		return nil // Nothing to sync
	}

	// Get primary key for upsert
	pk, err := c.GetPrimaryKey(ctx, schema, table)
	if err != nil {
		return fmt.Errorf("failed to get primary key (required for sync): %w", err)
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Apply session variables (SET LOCAL) if configured
	if vars := c.effectiveSessionVars(ctx); len(vars) > 0 {
		if err := applySessionVars(ctx, tx, vars); err != nil {
			return fmt.Errorf("failed to apply session variables: %w", err)
		}
	}

	// Build upsert statement
	// INSERT INTO table (col1, col2) VALUES ($1, $2)
	// ON CONFLICT (pk_col) DO UPDATE SET col1 = EXCLUDED.col1, col2 = EXCLUDED.col2
	if err := c.bulkUpsert(ctx, tx, schema, table, pk.Columns, columns, rows); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logging.Debug("ImportSync completed",
		zap.String("table", table),
		zap.Int("rows_synced", len(rows)))

	return nil
}

// ImportAppend inserts new rows without modifying existing data.
// Fails on primary key conflicts (duplicate key errors).
// All-or-nothing: rolls back on any error.
func (c *Client) ImportAppend(ctx context.Context, schema, table string, columns []string, rows [][]interface{}) error {
	logging.Debug("ImportAppend starting",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("columns", len(columns)),
		zap.Int("rows", len(rows)))

	if len(rows) == 0 {
		return nil // Nothing to append
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Apply session variables (SET LOCAL) if configured
	if vars := c.effectiveSessionVars(ctx); len(vars) > 0 {
		if err := applySessionVars(ctx, tx, vars); err != nil {
			return fmt.Errorf("failed to apply session variables: %w", err)
		}
	}

	// Bulk insert (will fail on conflicts)
	if err := c.bulkInsert(ctx, tx, schema, table, columns, rows); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logging.Debug("ImportAppend completed",
		zap.String("table", table),
		zap.Int("rows_inserted", len(rows)))

	return nil
}

// truncateTable truncates a table in its own transaction.
func (c *Client) truncateTable(ctx context.Context, schema, table string) (retErr error) {
	q, done, err := c.acquireDBTX(ctx)
	if err != nil {
		return err
	}
	defer func() { done(retErr) }()

	truncateSQL := fmt.Sprintf("TRUNCATE %s",
		qt(schema, table))
	_, err = q.Exec(ctx, truncateSQL)
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}
	return nil
}

// bulkInsert performs a bulk INSERT using COPY with text format for efficiency.
// Text format allows PostgreSQL to handle type conversions server-side,
// avoiding issues with binary encoding of complex types like timestamps.
//
// This requires pgx.Tx (not DBTX) because it uses tx.Conn().PgConn().CopyFrom()
// which is not part of the DBTX interface. Session vars are applied by the
// caller (ImportOverwrite/Sync/Append) at the start of the transaction.
func (c *Client) bulkInsert(ctx context.Context, tx pgx.Tx, schema, table string, columns []string, rows [][]interface{}) error {
	// Build COPY command with text format
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = qi(col)
	}
	columnList := strings.Join(quotedColumns, ", ")

	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN WITH (FORMAT text, NULL '')",
		qt(schema, table),
		columnList)

	// Build text data for COPY
	var buf strings.Builder
	for _, row := range rows {
		for i, val := range row {
			if i > 0 {
				buf.WriteByte('\t')
			}
			if val == nil {
				// NULL is represented as empty with NULL '' option
			} else {
				// Escape special characters for COPY text format
				buf.WriteString(escapeCopyValue(val))
			}
		}
		buf.WriteByte('\n')
	}

	// Execute COPY using pgconn
	conn := tx.Conn().PgConn()
	tag, err := conn.CopyFrom(ctx, strings.NewReader(buf.String()), copySQL)
	if err != nil {
		return fmt.Errorf("failed to insert rows: %w", err)
	}

	logging.Debug("Bulk insert completed",
		zap.Int64("rows_copied", tag.RowsAffected()))

	return nil
}

// escapeCopyValue escapes a value for COPY text format.
// Backslash, newline, carriage return, and tab need escaping.
func escapeCopyValue(val interface{}) string {
	s := fmt.Sprintf("%v", val)

	// Escape special characters for COPY text format
	var buf strings.Builder
	for _, r := range s {
		switch r {
		case '\\':
			buf.WriteString("\\\\")
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\t':
			buf.WriteString("\\t")
		default:
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

// bulkUpsert performs bulk upsert using individual INSERT ... ON CONFLICT statements.
// This is less efficient than CopyFrom but supports upsert semantics.
func (c *Client) bulkUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pkColumns, columns []string, rows [][]interface{}) error {
	// Build column list
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = qi(col)
	}
	columnList := strings.Join(quotedColumns, ", ")

	// Build placeholders
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	placeholderList := strings.Join(placeholders, ", ")

	// Build conflict target (primary key columns)
	quotedPKColumns := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		quotedPKColumns[i] = qi(col)
	}
	conflictTarget := strings.Join(quotedPKColumns, ", ")

	// Build UPDATE SET clause (all non-PK columns)
	updateClauses := make([]string, 0, len(columns))
	for _, col := range columns {
		// Skip PK columns in update
		isPK := false
		for _, pkCol := range pkColumns {
			if col == pkCol {
				isPK = true
				break
			}
		}
		if !isPK {
			quotedCol := qi(col)
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", quotedCol, quotedCol))
		}
	}

	var upsertSQL string
	if len(updateClauses) > 0 {
		updateSet := strings.Join(updateClauses, ", ")
		upsertSQL = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			qt(schema, table),
			columnList,
			placeholderList,
			conflictTarget,
			updateSet,
		)
	} else {
		// All columns are PK columns - just do nothing on conflict
		upsertSQL = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
			qt(schema, table),
			columnList,
			placeholderList,
			conflictTarget,
		)
	}

	// Use batch for efficiency
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(upsertSQL, row...)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	// Execute all batched statements
	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to upsert row: %w", err)
		}
	}

	return nil
}
