// Package fuse provides FUSE filesystem operations for TigerFS.
//
// This file implements the PipelineContext, which accumulates query state
// as users navigate through capability paths like .by/, .filter/, .order/,
// .first/, .last/, .sample/, and .export/.

package fuse

// LimitType represents the type of row limiting operation.
type LimitType int

const (
	// LimitNone indicates no limit has been applied.
	LimitNone LimitType = iota
	// LimitFirst limits to the first N rows.
	LimitFirst
	// LimitLast limits to the last N rows.
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

// PipelineContext accumulates query state as users navigate through capability paths.
// It is immutable - all mutation methods return a new instance.
//
// The context tracks:
// - Table identity (schema, table name, primary key column)
// - Filters from .by/ and .filter/ (AND-combined)
// - Ordering from .order/
// - Limits from .first/, .last/, .sample/
// - Previous limits for nested pagination (requires subquery)
// - Whether the pipeline has reached a terminal state (.export/)
type PipelineContext struct {
	// Table identity
	Schema    string
	TableName string
	PKColumn  string

	// Filters (from .by and .filter), AND-combined
	Filters []FilterCondition

	// Ordering (from .order)
	OrderBy   string
	OrderDesc bool

	// Current limit (from .first, .last, .sample)
	Limit     int
	LimitType LimitType

	// Previous limit for nested pagination (requires subquery)
	// E.g., .first/100/.last/50/ needs to select first 100, then last 50 of those
	PreviousLimit     int
	PreviousLimitType LimitType

	// HasOrdered tracks whether .order/ has been applied.
	// After ordering, no more filters or orders are allowed.
	HasOrdered bool

	// IsTerminal indicates this pipeline has reached .export/ and no more
	// capabilities can be added.
	IsTerminal bool
}

// NewPipelineContext creates a new pipeline context for a table.
//
// Parameters:
//   - schema: PostgreSQL schema name
//   - table: Table name
//   - pkColumn: Primary key column name (used for row identification)
//
// Returns a new PipelineContext ready for capability accumulation.
func NewPipelineContext(schema, table, pkColumn string) *PipelineContext {
	return &PipelineContext{
		Schema:    schema,
		TableName: table,
		PKColumn:  pkColumn,
		Filters:   nil,
		LimitType: LimitNone,
	}
}

// Clone creates an independent copy of the PipelineContext.
// This is used internally by mutation methods to preserve immutability.
func (p *PipelineContext) Clone() *PipelineContext {
	if p == nil {
		return nil
	}

	// Deep copy filters slice
	var filters []FilterCondition
	if len(p.Filters) > 0 {
		filters = make([]FilterCondition, len(p.Filters))
		copy(filters, p.Filters)
	}

	return &PipelineContext{
		Schema:            p.Schema,
		TableName:         p.TableName,
		PKColumn:          p.PKColumn,
		Filters:           filters,
		OrderBy:           p.OrderBy,
		OrderDesc:         p.OrderDesc,
		Limit:             p.Limit,
		LimitType:         p.LimitType,
		PreviousLimit:     p.PreviousLimit,
		PreviousLimitType: p.PreviousLimitType,
		HasOrdered:        p.HasOrdered,
		IsTerminal:        p.IsTerminal,
	}
}

// WithFilter returns a new PipelineContext with an additional filter.
// Filters are AND-combined.
//
// Parameters:
//   - col: Column name to filter on
//   - val: Value to match (equality)
//   - indexed: true if this comes from .by/ (indexed column), false for .filter/
//
// Returns a new PipelineContext with the filter added.
// If CanAddFilter() returns false, this still adds the filter (caller should check first).
func (p *PipelineContext) WithFilter(col, val string, indexed bool) *PipelineContext {
	clone := p.Clone()
	clone.Filters = append(clone.Filters, FilterCondition{
		Column:  col,
		Value:   val,
		Indexed: indexed,
	})
	return clone
}

// WithOrder returns a new PipelineContext with ordering set.
// Only one order is supported; calling this overwrites any previous order.
//
// Parameters:
//   - col: Column name to order by
//   - desc: true for descending order, false for ascending
//
// Returns a new PipelineContext with ordering set.
func (p *PipelineContext) WithOrder(col string, desc bool) *PipelineContext {
	clone := p.Clone()
	clone.OrderBy = col
	clone.OrderDesc = desc
	clone.HasOrdered = true
	return clone
}

// WithLimit returns a new PipelineContext with a limit applied.
// If a limit already exists, the current limit becomes the "previous" limit
// (requiring a subquery for nested pagination).
//
// Parameters:
//   - limit: Number of rows to limit to
//   - limitType: Type of limit (First, Last, Sample)
//
// Returns a new PipelineContext with the limit applied.
func (p *PipelineContext) WithLimit(limit int, limitType LimitType) *PipelineContext {
	clone := p.Clone()

	// If we already have a limit, push it to previous (nested pagination)
	if clone.LimitType != LimitNone {
		clone.PreviousLimit = clone.Limit
		clone.PreviousLimitType = clone.LimitType
	}

	clone.Limit = limit
	clone.LimitType = limitType
	return clone
}

// WithTerminal returns a new PipelineContext marked as terminal.
// Used when .export/ is reached.
func (p *PipelineContext) WithTerminal() *PipelineContext {
	clone := p.Clone()
	clone.IsTerminal = true
	return clone
}

// CanAddFilter returns true if filters can still be added.
// Filters are disallowed after .order/ (must filter before ordering).
func (p *PipelineContext) CanAddFilter() bool {
	if p.IsTerminal {
		return false
	}
	// Per ADR: after .order/, no more filters allowed
	return !p.HasOrdered
}

// CanAddOrder returns true if ordering can be set.
// Ordering is disallowed if already ordered (second order is redundant).
func (p *PipelineContext) CanAddOrder() bool {
	if p.IsTerminal {
		return false
	}
	// Per ADR: second order is redundant
	return !p.HasOrdered
}

// CanAddLimit returns true if the specified limit type can be added.
// Rules:
//   - No limits after terminal (.export/)
//   - No double-first (.first/.first) - redundant
//   - No double-last (.last/.last) - redundant
//   - No any limit after sample (.sample/.first, .sample/.last, .sample/.sample) - just sample fewer
func (p *PipelineContext) CanAddLimit(limitType LimitType) bool {
	if p.IsTerminal {
		return false
	}

	// After sample, no more limits allowed (just sample fewer)
	if p.LimitType == LimitSample {
		return false
	}

	// No double-first
	if p.LimitType == LimitFirst && limitType == LimitFirst {
		return false
	}

	// No double-last
	if p.LimitType == LimitLast && limitType == LimitLast {
		return false
	}

	return true
}

// CanExport returns true if .export/ can be added.
// Export is always available unless already terminal.
func (p *PipelineContext) CanExport() bool {
	return !p.IsTerminal
}

// HasFilters returns true if any filters have been applied.
func (p *PipelineContext) HasFilters() bool {
	return len(p.Filters) > 0
}

// HasLimit returns true if any limit has been applied.
func (p *PipelineContext) HasLimit() bool {
	return p.LimitType != LimitNone
}

// HasNestedLimit returns true if there are nested limits (requiring subquery).
func (p *PipelineContext) HasNestedLimit() bool {
	return p.PreviousLimitType != LimitNone
}

// NeedsSubquery returns true if the query requires a subquery for proper execution.
// This is needed for nested pagination like .first/100/.last/50/.
func (p *PipelineContext) NeedsSubquery() bool {
	return p.HasNestedLimit()
}

// AvailableCapabilities returns the list of capabilities that can be added next.
// This is used by FUSE nodes to determine what to expose in directory listings.
//
// Returns a slice of capability names (e.g., ".by", ".filter", ".order", ".first", ".last", ".sample", ".export").
func (p *PipelineContext) AvailableCapabilities() []string {
	if p.IsTerminal {
		return nil
	}

	var caps []string

	// Filters (.by/ and .filter/) available unless after .order/
	if p.CanAddFilter() {
		caps = append(caps, ".by", ".filter")
	}

	// Order available unless already ordered
	if p.CanAddOrder() {
		caps = append(caps, ".order")
	}

	// Limits with their specific rules
	if p.CanAddLimit(LimitFirst) {
		caps = append(caps, ".first")
	}
	if p.CanAddLimit(LimitLast) {
		caps = append(caps, ".last")
	}
	if p.CanAddLimit(LimitSample) {
		caps = append(caps, ".sample")
	}

	// Export always available unless terminal
	if p.CanExport() {
		caps = append(caps, ".export")
	}

	return caps
}

// QueryParams contains the parameters needed to build a pipeline query.
// This is used to pass context from the FUSE layer to the database layer.
type QueryParams struct {
	Schema    string
	Table     string
	PKColumn  string
	Filters   []FilterCondition
	OrderBy   string
	OrderDesc bool
	Limit     int
	LimitType LimitType
	// For nested limits (subquery needed)
	PreviousLimit     int
	PreviousLimitType LimitType
}

// ToQueryParams converts the PipelineContext to QueryParams for the database layer.
func (p *PipelineContext) ToQueryParams() QueryParams {
	// Copy filters to avoid sharing slice
	var filters []FilterCondition
	if len(p.Filters) > 0 {
		filters = make([]FilterCondition, len(p.Filters))
		copy(filters, p.Filters)
	}

	return QueryParams{
		Schema:            p.Schema,
		Table:             p.TableName,
		PKColumn:          p.PKColumn,
		Filters:           filters,
		OrderBy:           p.OrderBy,
		OrderDesc:         p.OrderDesc,
		Limit:             p.Limit,
		LimitType:         p.LimitType,
		PreviousLimit:     p.PreviousLimit,
		PreviousLimitType: p.PreviousLimitType,
	}
}
