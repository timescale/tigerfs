// Package db provides PostgreSQL database operations for TigerFS.
//
// This file implements pipeline query functions that support composable
// capability chaining with filters, ordering, and limits.

package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// LimitType represents the type of row limiting operation.
type LimitType int

const (
	// LimitNone indicates no limit has been applied.
	LimitNone LimitType = iota
	// LimitFirst limits to the first N rows (ORDER BY pk ASC).
	LimitFirst
	// LimitLast limits to the last N rows (ORDER BY pk DESC).
	LimitLast
	// LimitSample randomly samples N rows.
	LimitSample
)

// String returns a human-readable name for the LimitType.
func (lt LimitType) String() string {
	switch lt {
	case LimitFirst:
		return "first"
	case LimitLast:
		return "last"
	case LimitSample:
		return "sample"
	default:
		return "none"
	}
}

// FilterCondition represents a column equality filter.
// Multiple filters are combined with AND.
type FilterCondition struct {
	// Column is the column name to filter on.
	Column string
	// Value is the value to match (equality comparison).
	Value string
	// Indexed indicates whether this filter came from .by/ (true) or .filter/ (false).
	// This affects query planning but not semantics.
	Indexed bool
}

// QueryParams contains all parameters needed to build a pipeline query.
type QueryParams struct {
	Schema   string
	Table    string
	PKColumn string

	// Filters (from .by and .filter), AND-combined
	Filters []FilterCondition

	// Column projection (from .columns/col1,col2,col3/)
	// Empty means SELECT * (all columns).
	Columns []string

	// Ordering
	OrderBy   string
	OrderDesc bool

	// Current limit
	Limit     int
	LimitType LimitType

	// Previous limit for nested pagination (requires subquery)
	PreviousLimit     int
	PreviousLimitType LimitType
}

// NeedsSubquery returns true if the query requires a subquery.
// This is needed for nested limits like .first/100/.last/50/.
func (p *QueryParams) NeedsSubquery() bool {
	return p.PreviousLimitType != LimitNone
}

// HasFilters returns true if any filters are present.
func (p *QueryParams) HasFilters() bool {
	return len(p.Filters) > 0
}

// HasLimit returns true if a limit is set.
func (p *QueryParams) HasLimit() bool {
	return p.LimitType != LimitNone
}

// HasOrder returns true if ordering is set.
func (p *QueryParams) HasOrder() bool {
	return p.OrderBy != ""
}

// QueryRowsPipeline executes a pipeline query and returns primary key values.
//
// This function handles all combinations of filters, ordering, and limits,
// including nested limits that require subqueries.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: PostgreSQL connection pool
//   - params: Query parameters from pipeline context
//
// Returns primary key values as strings, or error on database failure.
func QueryRowsPipeline(ctx context.Context, pool *pgxpool.Pool, params QueryParams) ([]string, error) {
	logging.Debug("Executing pipeline query",
		zap.String("schema", params.Schema),
		zap.String("table", params.Table),
		zap.Int("filter_count", len(params.Filters)),
		zap.String("order_by", params.OrderBy),
		zap.String("limit_type", params.LimitType.String()),
		zap.Int("limit", params.Limit))

	query, queryParams := buildPipelineSQL(params, true) // selectPKOnly=true

	logging.Debug("Built pipeline SQL",
		zap.String("sql", query),
		zap.Int("param_count", len(queryParams)))

	rows, err := pool.Query(ctx, query, queryParams...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute pipeline query: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk interface{}
		if err := rows.Scan(&pk); err != nil {
			return nil, fmt.Errorf("failed to scan primary key: %w", err)
		}
		pkStr, err := format.ConvertValueToText(pk)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary key value: %w", err)
		}
		pks = append(pks, pkStr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pipeline results: %w", err)
	}

	logging.Debug("Pipeline query completed",
		zap.String("table", params.Table),
		zap.Int("result_count", len(pks)))

	return pks, nil
}

// QueryRowsPipeline is a convenience wrapper for Client.
func (c *Client) QueryRowsPipeline(ctx context.Context, params QueryParams) ([]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	return QueryRowsPipeline(ctx, c.pool, params)
}

// QueryRowsWithDataPipeline executes a pipeline query and returns full row data.
// Used for .export/ operations that need all columns.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - pool: PostgreSQL connection pool
//   - params: Query parameters from pipeline context
//
// Returns column names and row data, or error on database failure.
func QueryRowsWithDataPipeline(ctx context.Context, pool *pgxpool.Pool, params QueryParams) ([]string, [][]interface{}, error) {
	logging.Debug("Executing pipeline query with full data",
		zap.String("schema", params.Schema),
		zap.String("table", params.Table),
		zap.Int("filter_count", len(params.Filters)),
		zap.String("order_by", params.OrderBy),
		zap.String("limit_type", params.LimitType.String()),
		zap.Int("limit", params.Limit))

	query, queryParams := buildPipelineSQL(params, false) // selectPKOnly=false

	logging.Debug("Built pipeline SQL for full data",
		zap.String("sql", query),
		zap.Int("param_count", len(queryParams)))

	rows, err := pool.Query(ctx, query, queryParams...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute pipeline query: %w", err)
	}
	defer rows.Close()

	// Get column names from field descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	var results [][]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan row values: %w", err)
		}
		results = append(results, values)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating pipeline results: %w", err)
	}

	logging.Debug("Pipeline query with data completed",
		zap.String("table", params.Table),
		zap.Int("column_count", len(columns)),
		zap.Int("row_count", len(results)))

	return columns, results, nil
}

// QueryRowsWithDataPipeline is a convenience wrapper for Client.
func (c *Client) QueryRowsWithDataPipeline(ctx context.Context, params QueryParams) ([]string, [][]interface{}, error) {
	if c.pool == nil {
		return nil, nil, fmt.Errorf("database connection not initialized")
	}
	return QueryRowsWithDataPipeline(ctx, c.pool, params)
}

// buildPipelineSQL constructs the SQL query for a pipeline operation.
// If selectPKOnly is true, only the primary key column is selected.
// Otherwise, columns from params.Columns are selected (or * if empty).
//
// Returns the SQL string and parameter values.
func buildPipelineSQL(params QueryParams, selectPKOnly bool) (string, []interface{}) {
	// Determine what to select
	selectClause := buildSelectClause(params, selectPKOnly)

	// Base table reference
	tableRef := fmt.Sprintf(`"%s"."%s"`, params.Schema, params.Table)

	// Check if we need subqueries for nested operations
	if params.NeedsSubquery() {
		return buildNestedPipelineSQL(params, selectPKOnly)
	}

	// Simple case: no nested limits
	return buildSimplePipelineSQL(params, selectClause, tableRef)
}

// buildSelectClause constructs the SELECT column list.
// Priority: selectPKOnly > params.Columns > *.
func buildSelectClause(params QueryParams, selectPKOnly bool) string {
	if selectPKOnly {
		return fmt.Sprintf(`"%s"`, params.PKColumn)
	}
	if len(params.Columns) > 0 {
		quoted := make([]string, len(params.Columns))
		for i, col := range params.Columns {
			quoted[i] = fmt.Sprintf(`"%s"`, col)
		}
		return strings.Join(quoted, ", ")
	}
	return "*"
}

// buildSimplePipelineSQL builds SQL for non-nested pipelines.
func buildSimplePipelineSQL(params QueryParams, selectClause, tableRef string) (string, []interface{}) {
	var queryParams []interface{}
	paramIndex := 1

	// Build WHERE clause from filters
	var whereClause string
	if len(params.Filters) > 0 {
		whereParts := make([]string, len(params.Filters))
		for i, f := range params.Filters {
			whereParts[i] = fmt.Sprintf(`"%s" = $%d`, f.Column, paramIndex)
			queryParams = append(queryParams, f.Value)
			paramIndex++
		}
		whereClause = " WHERE " + strings.Join(whereParts, " AND ")
	}

	// Build ORDER BY clause
	orderClause := buildOrderClause(params)

	// Build LIMIT clause
	limitClause := ""
	if params.HasLimit() && params.Limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT $%d", paramIndex)
		queryParams = append(queryParams, params.Limit)
	}

	query := fmt.Sprintf("SELECT %s FROM %s%s%s%s",
		selectClause, tableRef, whereClause, orderClause, limitClause)

	return query, queryParams
}

// buildNestedPipelineSQL builds SQL for pipelines with nested limits.
// This creates subqueries to properly apply limits in sequence.
//
// Examples:
//
//   - .first/100/.last/50/ becomes:
//     SELECT pk FROM (SELECT * FROM t ORDER BY pk ASC LIMIT 100) sub ORDER BY pk DESC LIMIT 50
//
//   - .first/100/.filter/status/active/.sample/50/ becomes:
//     SELECT pk FROM (SELECT * FROM (SELECT * FROM t ORDER BY pk LIMIT 100) sub1 WHERE status='active') sub2 ORDER BY RANDOM() LIMIT 50
func buildNestedPipelineSQL(params QueryParams, selectPKOnly bool) (string, []interface{}) {
	var queryParams []interface{}
	paramIndex := 1

	// Determine outer select (applies column projection)
	outerSelect := buildSelectClause(params, selectPKOnly)

	// Build the inner subquery for the previous limit
	innerQuery, innerParams := buildInnerLimitQuery(params, &paramIndex)
	queryParams = append(queryParams, innerParams...)

	// Build WHERE clause for any filters (applied to subquery result)
	var whereClause string
	if len(params.Filters) > 0 {
		whereParts := make([]string, len(params.Filters))
		for i, f := range params.Filters {
			whereParts[i] = fmt.Sprintf(`"%s" = $%d`, f.Column, paramIndex)
			queryParams = append(queryParams, f.Value)
			paramIndex++
		}
		whereClause = " WHERE " + strings.Join(whereParts, " AND ")
	}

	// Build the outer query with current limit
	orderClause := buildOrderClauseForLimitType(params.LimitType, params.PKColumn, params.OrderBy, params.OrderDesc)

	limitClause := ""
	if params.HasLimit() && params.Limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT $%d", paramIndex)
		queryParams = append(queryParams, params.Limit)
	}

	// If we have filters to apply after the inner limit, we need an intermediate subquery
	if len(params.Filters) > 0 {
		query := fmt.Sprintf("SELECT %s FROM (SELECT * FROM (%s) inner_sub%s) filtered_sub%s%s",
			outerSelect, innerQuery, whereClause, orderClause, limitClause)
		return query, queryParams
	}

	// No filters, just nest the limits
	query := fmt.Sprintf("SELECT %s FROM (%s) sub%s%s",
		outerSelect, innerQuery, orderClause, limitClause)
	return query, queryParams
}

// buildInnerLimitQuery builds the innermost subquery for the previous limit.
func buildInnerLimitQuery(params QueryParams, paramIndex *int) (string, []interface{}) {
	var queryParams []interface{}

	tableRef := fmt.Sprintf(`"%s"."%s"`, params.Schema, params.Table)

	// Order clause for the previous limit type
	orderClause := buildOrderClauseForLimitType(params.PreviousLimitType, params.PKColumn, "", false)

	limitClause := ""
	if params.PreviousLimit > 0 {
		limitClause = fmt.Sprintf(" LIMIT $%d", *paramIndex)
		queryParams = append(queryParams, params.PreviousLimit)
		*paramIndex++
	}

	query := fmt.Sprintf("SELECT * FROM %s%s%s", tableRef, orderClause, limitClause)
	return query, queryParams
}

// buildOrderClause builds the ORDER BY clause based on params.
func buildOrderClause(params QueryParams) string {
	if params.HasOrder() {
		// Custom ordering specified
		direction := "ASC"
		if params.OrderDesc {
			direction = "DESC"
		}
		// Add PK as secondary sort for stability
		return fmt.Sprintf(` ORDER BY "%s" %s NULLS LAST, "%s" %s`,
			params.OrderBy, direction, params.PKColumn, direction)
	}

	// Default ordering based on limit type
	return buildOrderClauseForLimitType(params.LimitType, params.PKColumn, "", false)
}

// buildOrderClauseForLimitType builds ORDER BY for a specific limit type.
func buildOrderClauseForLimitType(lt LimitType, pkColumn, orderBy string, orderDesc bool) string {
	// If custom order is specified, use it
	if orderBy != "" {
		direction := "ASC"
		if orderDesc {
			direction = "DESC"
		}
		return fmt.Sprintf(` ORDER BY "%s" %s NULLS LAST, "%s" %s`,
			orderBy, direction, pkColumn, direction)
	}

	// Default ordering based on limit type
	switch lt {
	case LimitFirst:
		return fmt.Sprintf(` ORDER BY "%s" ASC`, pkColumn)
	case LimitLast:
		return fmt.Sprintf(` ORDER BY "%s" DESC`, pkColumn)
	case LimitSample:
		return " ORDER BY RANDOM()"
	default:
		return ""
	}
}

// BuildPipelineSQLForTest exposes buildPipelineSQL for testing.
// This allows unit tests to verify SQL generation without database access.
func BuildPipelineSQLForTest(params QueryParams, selectPKOnly bool) (string, []interface{}) {
	return buildPipelineSQL(params, selectPKOnly)
}
