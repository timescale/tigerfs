// Package fs provides the shared core filesystem logic for TigerFS.
//
// This file implements FSContext, which accumulates query state as users
// navigate through capability paths like .by/, .filter/, .order/,
// .first/, .last/, .sample/, and .export/.

package fs

import (
	"github.com/timescale/tigerfs/internal/tigerfs/db"
)

// Type aliases for db package types to simplify usage.
// These are used throughout the context to track query state.
type (
	// LimitType represents the type of row limiting operation.
	LimitType = db.LimitType
	// FilterCondition represents a column equality filter.
	FilterCondition = db.FilterCondition
	// QueryParams contains the parameters needed to build a pipeline query.
	QueryParams = db.QueryParams
)

// Re-export LimitType constants for convenience.
const (
	LimitNone   = db.LimitNone
	LimitFirst  = db.LimitFirst
	LimitLast   = db.LimitLast
	LimitSample = db.LimitSample
)

// FSContext accumulates query state as users navigate through capability paths.
// It is immutable - all mutation methods return a new instance.
//
// The context tracks:
//   - Table identity (schema, table name, primary key column)
//   - Filters from .by/ and .filter/ (AND-combined)
//   - Ordering from .order/
//   - Limits from .first/, .last/, .sample/
//   - Previous limits for nested pagination (requires subquery)
//   - Whether the pipeline has reached a terminal state (.export/)
type FSContext struct {
	// Schema is the PostgreSQL schema name (e.g., "public").
	Schema string

	// TableName is the table being queried.
	TableName string

	// PKColumn is the primary key column name, used for row identification.
	PKColumn string

	// Filters holds filter conditions from .by/ and .filter/, AND-combined.
	Filters []FilterCondition

	// OrderBy is the column to order results by (from .order/).
	OrderBy string

	// OrderDesc is true for descending order, false for ascending.
	OrderDesc bool

	// Limit is the row limit count (from .first/, .last/, .sample/).
	Limit int

	// LimitType indicates the type of limit applied.
	LimitType LimitType

	// PreviousLimit is the outer limit for nested pagination.
	// E.g., .first/100/.last/50/ needs to select first 100, then last 50 of those.
	PreviousLimit int

	// PreviousLimitType is the type of the outer limit.
	PreviousLimitType LimitType

	// HasOrdered tracks whether .order/ has been applied.
	// After ordering, no more filters or orders are allowed.
	HasOrdered bool

	// Columns lists column names to project (empty means SELECT *).
	// Set via .columns/col1,col2,col3/ pipeline stage.
	Columns []string

	// HasColumns tracks whether .columns/ has been applied.
	// After column projection, only .export/ is available.
	HasColumns bool

	// IsTerminal indicates this context has reached .export/ and no more
	// capabilities can be added.
	IsTerminal bool
}

// NewFSContext creates a new filesystem context for a table.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pkColumn: Primary key column name (used for row identification)
//
// Returns a new FSContext ready for capability accumulation.
func NewFSContext(schema, table, pkColumn string) *FSContext {
	return &FSContext{
		Schema:    schema,
		TableName: table,
		PKColumn:  pkColumn,
		Filters:   nil,
		LimitType: LimitNone,
	}
}

// Clone creates an independent copy of the FSContext.
// This is used internally by mutation methods to preserve immutability.
func (ctx *FSContext) Clone() *FSContext {
	if ctx == nil {
		return nil
	}

	// Deep copy filters slice
	var filters []FilterCondition
	if len(ctx.Filters) > 0 {
		filters = make([]FilterCondition, len(ctx.Filters))
		copy(filters, ctx.Filters)
	}

	// Deep copy columns slice
	var columns []string
	if len(ctx.Columns) > 0 {
		columns = make([]string, len(ctx.Columns))
		copy(columns, ctx.Columns)
	}

	return &FSContext{
		Schema:            ctx.Schema,
		TableName:         ctx.TableName,
		PKColumn:          ctx.PKColumn,
		Filters:           filters,
		OrderBy:           ctx.OrderBy,
		OrderDesc:         ctx.OrderDesc,
		Limit:             ctx.Limit,
		LimitType:         ctx.LimitType,
		PreviousLimit:     ctx.PreviousLimit,
		PreviousLimitType: ctx.PreviousLimitType,
		HasOrdered:        ctx.HasOrdered,
		Columns:           columns,
		HasColumns:        ctx.HasColumns,
		IsTerminal:        ctx.IsTerminal,
	}
}

// WithFilter returns a new FSContext with an additional filter.
// Filters are AND-combined.
//
// Parameters:
//   - col: Column name to filter on
//   - val: Value to match (equality)
//   - indexed: true if this comes from .by/ (indexed column), false for .filter/
//
// Returns a new FSContext with the filter added.
// If CanAddFilter() returns false, this still adds the filter (caller should check first).
func (ctx *FSContext) WithFilter(col, val string, indexed bool) *FSContext {
	clone := ctx.Clone()
	clone.Filters = append(clone.Filters, FilterCondition{
		Column:  col,
		Value:   val,
		Indexed: indexed,
	})
	return clone
}

// WithOrder returns a new FSContext with ordering set.
// Only one order is supported; calling this overwrites any previous order.
//
// Parameters:
//   - col: Column name to order by
//   - desc: true for descending order, false for ascending
//
// Returns a new FSContext with ordering set.
func (ctx *FSContext) WithOrder(col string, desc bool) *FSContext {
	clone := ctx.Clone()
	clone.OrderBy = col
	clone.OrderDesc = desc
	clone.HasOrdered = true
	return clone
}

// WithLimit returns a new FSContext with a limit applied.
// If a limit already exists, the current limit becomes the "previous" limit
// (requiring a subquery for nested pagination).
//
// Parameters:
//   - limit: Number of rows to limit to
//   - limitType: Type of limit (First, Last, Sample)
//
// Returns a new FSContext with the limit applied.
func (ctx *FSContext) WithLimit(limit int, limitType LimitType) *FSContext {
	clone := ctx.Clone()

	// If we already have a limit, push it to previous (nested pagination)
	if clone.LimitType != LimitNone {
		clone.PreviousLimit = clone.Limit
		clone.PreviousLimitType = clone.LimitType
	}

	clone.Limit = limit
	clone.LimitType = limitType
	return clone
}

// WithTerminal returns a new FSContext marked as terminal.
// Used when .export/ is reached.
func (ctx *FSContext) WithTerminal() *FSContext {
	clone := ctx.Clone()
	clone.IsTerminal = true
	return clone
}

// WithColumns returns a new FSContext with column projection set.
// After columns are set, only .export/ is available as a next step.
//
// Parameters:
//   - columns: Column names to project in the query
//
// Returns a new FSContext with columns set and HasColumns=true.
func (ctx *FSContext) WithColumns(columns []string) *FSContext {
	clone := ctx.Clone()
	clone.Columns = make([]string, len(columns))
	copy(clone.Columns, columns)
	clone.HasColumns = true
	return clone
}

// CanAddColumns returns true if .columns/ can be added.
// Columns are disallowed after terminal (.export/) or if already set.
func (ctx *FSContext) CanAddColumns() bool {
	return !ctx.IsTerminal && !ctx.HasColumns
}

// CanAddFilter returns true if filters can still be added.
// Filters are disallowed after .order/ (must filter before ordering).
func (ctx *FSContext) CanAddFilter() bool {
	if ctx.IsTerminal {
		return false
	}
	// Per ADR: after .order/, no more filters allowed
	return !ctx.HasOrdered
}

// CanAddOrder returns true if ordering can be set.
// Ordering is disallowed if already ordered (second order is redundant).
func (ctx *FSContext) CanAddOrder() bool {
	if ctx.IsTerminal {
		return false
	}
	// Per ADR: second order is redundant
	return !ctx.HasOrdered
}

// CanAddLimit returns true if the specified limit type can be added.
// Rules:
//   - No limits after terminal (.export/)
//   - No double-first (.first/.first) - redundant
//   - No double-last (.last/.last) - redundant
//   - No any limit after sample (.sample/.first, .sample/.last, .sample/.sample) - just sample fewer
func (ctx *FSContext) CanAddLimit(limitType LimitType) bool {
	if ctx.IsTerminal {
		return false
	}

	// After sample, no more limits allowed (just sample fewer)
	if ctx.LimitType == LimitSample {
		return false
	}

	// No double-first
	if ctx.LimitType == LimitFirst && limitType == LimitFirst {
		return false
	}

	// No double-last
	if ctx.LimitType == LimitLast && limitType == LimitLast {
		return false
	}

	return true
}

// CanExport returns true if .export/ can be added.
// Export is always available unless already terminal.
func (ctx *FSContext) CanExport() bool {
	return !ctx.IsTerminal
}

// HasFilters returns true if any filters have been applied.
func (ctx *FSContext) HasFilters() bool {
	return len(ctx.Filters) > 0
}

// HasPipelineOperations returns true if any pipeline operations have been applied.
// This includes filters (.by/, .filter/), ordering (.order/), limits (.first/, .last/, .sample/),
// or column projection (.columns/).
// Used to determine whether to use pipeline-aware queries instead of simple table scans.
func (ctx *FSContext) HasPipelineOperations() bool {
	return ctx.HasFilters() || ctx.HasOrdered || ctx.LimitType != LimitNone || ctx.HasColumns
}

// HasLimit returns true if any limit has been applied.
func (ctx *FSContext) HasLimit() bool {
	return ctx.LimitType != LimitNone
}

// HasNestedLimit returns true if there are nested limits (requiring subquery).
func (ctx *FSContext) HasNestedLimit() bool {
	return ctx.PreviousLimitType != LimitNone
}

// NeedsSubquery returns true if the query requires a subquery for proper execution.
// This is needed for nested pagination like .first/100/.last/50/.
func (ctx *FSContext) NeedsSubquery() bool {
	return ctx.HasNestedLimit()
}

// AvailableCapabilities returns the list of capabilities that can be added next.
// This is used to determine what to expose in directory listings.
//
// Returns a slice of capability names (e.g., ".by", ".columns", ".filter", ".order", ".first", ".last", ".sample", ".export").
func (ctx *FSContext) AvailableCapabilities() []string {
	if ctx.IsTerminal {
		return nil
	}

	// After .columns/, only .export/ is available
	if ctx.HasColumns {
		return []string{".export"}
	}

	var caps []string

	// Filters (.by/ and .filter/) available unless after .order/
	if ctx.CanAddFilter() {
		caps = append(caps, ".by", ".filter")
	}

	// Columns available if not yet set
	if ctx.CanAddColumns() {
		caps = append(caps, ".columns")
	}

	// Order available unless already ordered
	if ctx.CanAddOrder() {
		caps = append(caps, ".order")
	}

	// Limits with their specific rules
	if ctx.CanAddLimit(LimitFirst) {
		caps = append(caps, ".first")
	}
	if ctx.CanAddLimit(LimitLast) {
		caps = append(caps, ".last")
	}
	if ctx.CanAddLimit(LimitSample) {
		caps = append(caps, ".sample")
	}

	// Export always available unless terminal
	if ctx.CanExport() {
		caps = append(caps, ".export")
	}

	return caps
}

// ToQueryParams converts the FSContext to QueryParams for the database layer.
func (ctx *FSContext) ToQueryParams() QueryParams {
	// Copy filters to avoid sharing slice
	var filters []FilterCondition
	if len(ctx.Filters) > 0 {
		filters = make([]FilterCondition, len(ctx.Filters))
		copy(filters, ctx.Filters)
	}

	// Copy columns to avoid sharing slice
	var columns []string
	if len(ctx.Columns) > 0 {
		columns = make([]string, len(ctx.Columns))
		copy(columns, ctx.Columns)
	}

	return QueryParams{
		Schema:            ctx.Schema,
		Table:             ctx.TableName,
		PKColumn:          ctx.PKColumn,
		Filters:           filters,
		OrderBy:           ctx.OrderBy,
		OrderDesc:         ctx.OrderDesc,
		Limit:             ctx.Limit,
		LimitType:         ctx.LimitType,
		PreviousLimit:     ctx.PreviousLimit,
		PreviousLimitType: ctx.PreviousLimitType,
		Columns:           columns,
	}
}
