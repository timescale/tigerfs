package fuse

import (
	"testing"
)

// TestLimitType_String tests the String method for LimitType.
func TestLimitType_String(t *testing.T) {
	tests := []struct {
		lt   LimitType
		want string
	}{
		{LimitNone, "none"},
		{LimitFirst, "first"},
		{LimitLast, "last"},
		{LimitSample, "sample"},
		{LimitType(99), "none"}, // Unknown type defaults to none
	}

	for _, tt := range tests {
		got := tt.lt.String()
		if got != tt.want {
			t.Errorf("LimitType(%d).String() = %q, want %q", tt.lt, got, tt.want)
		}
	}
}

// TestNewPipelineContext tests creating a new pipeline context.
func TestNewPipelineContext(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")

	if ctx.Schema != "public" {
		t.Errorf("Schema = %q, want %q", ctx.Schema, "public")
	}
	if ctx.TableName != "users" {
		t.Errorf("TableName = %q, want %q", ctx.TableName, "users")
	}
	if ctx.PKColumn != "id" {
		t.Errorf("PKColumn = %q, want %q", ctx.PKColumn, "id")
	}
	if len(ctx.Filters) != 0 {
		t.Errorf("Filters should be empty, got %d", len(ctx.Filters))
	}
	if ctx.LimitType != LimitNone {
		t.Errorf("LimitType = %v, want %v", ctx.LimitType, LimitNone)
	}
	if ctx.HasOrdered {
		t.Error("HasOrdered should be false")
	}
	if ctx.IsTerminal {
		t.Error("IsTerminal should be false")
	}
}

// TestPipelineContext_Clone tests that Clone creates an independent copy.
func TestPipelineContext_Clone(t *testing.T) {
	original := NewPipelineContext("public", "users", "id")
	original = original.WithFilter("status", "active", true)
	original = original.WithOrder("name", false)
	original = original.WithLimit(100, LimitFirst)

	clone := original.Clone()

	// Verify all fields are copied
	if clone.Schema != original.Schema {
		t.Errorf("Clone Schema = %q, want %q", clone.Schema, original.Schema)
	}
	if clone.TableName != original.TableName {
		t.Errorf("Clone TableName = %q, want %q", clone.TableName, original.TableName)
	}
	if clone.PKColumn != original.PKColumn {
		t.Errorf("Clone PKColumn = %q, want %q", clone.PKColumn, original.PKColumn)
	}
	if len(clone.Filters) != len(original.Filters) {
		t.Errorf("Clone Filters length = %d, want %d", len(clone.Filters), len(original.Filters))
	}
	if clone.OrderBy != original.OrderBy {
		t.Errorf("Clone OrderBy = %q, want %q", clone.OrderBy, original.OrderBy)
	}
	if clone.Limit != original.Limit {
		t.Errorf("Clone Limit = %d, want %d", clone.Limit, original.Limit)
	}
	if clone.LimitType != original.LimitType {
		t.Errorf("Clone LimitType = %v, want %v", clone.LimitType, original.LimitType)
	}

	// Verify independence - modifying clone's filters shouldn't affect original
	if len(clone.Filters) > 0 {
		clone.Filters[0].Column = "modified"
		if original.Filters[0].Column == "modified" {
			t.Error("Clone filters slice is not independent from original")
		}
	}
}

// TestPipelineContext_Clone_Nil tests Clone on nil context.
func TestPipelineContext_Clone_Nil(t *testing.T) {
	var ctx *PipelineContext
	clone := ctx.Clone()
	if clone != nil {
		t.Error("Clone of nil should return nil")
	}
}

// TestPipelineContext_WithFilter tests adding filters.
func TestPipelineContext_WithFilter(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")

	// Add first filter
	ctx2 := ctx.WithFilter("status", "active", true)
	if len(ctx2.Filters) != 1 {
		t.Fatalf("Expected 1 filter, got %d", len(ctx2.Filters))
	}
	if ctx2.Filters[0].Column != "status" {
		t.Errorf("Filter column = %q, want %q", ctx2.Filters[0].Column, "status")
	}
	if ctx2.Filters[0].Value != "active" {
		t.Errorf("Filter value = %q, want %q", ctx2.Filters[0].Value, "active")
	}
	if !ctx2.Filters[0].Indexed {
		t.Error("Filter should be indexed")
	}

	// Add second filter (AND-combined)
	ctx3 := ctx2.WithFilter("role", "admin", false)
	if len(ctx3.Filters) != 2 {
		t.Fatalf("Expected 2 filters, got %d", len(ctx3.Filters))
	}
	if ctx3.Filters[1].Column != "role" {
		t.Errorf("Second filter column = %q, want %q", ctx3.Filters[1].Column, "role")
	}
	if ctx3.Filters[1].Indexed {
		t.Error("Second filter should not be indexed")
	}

	// Original should be unchanged (immutability)
	if len(ctx.Filters) != 0 {
		t.Error("Original context was modified")
	}
	if len(ctx2.Filters) != 1 {
		t.Error("Intermediate context was modified")
	}
}

// TestPipelineContext_WithFilter_Conflicting tests adding conflicting filters.
// This simulates paths like .by/user_id/90/.by/user_id/71/ which should result
// in user_id=90 AND user_id=71 (impossible condition = empty results).
func TestPipelineContext_WithFilter_Conflicting(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")

	// Add first filter: user_id = 90
	ctx2 := ctx.WithFilter("user_id", "90", true)
	if len(ctx2.Filters) != 1 {
		t.Fatalf("Expected 1 filter, got %d", len(ctx2.Filters))
	}

	// Add conflicting filter on same column: user_id = 71
	ctx3 := ctx2.WithFilter("user_id", "71", true)
	if len(ctx3.Filters) != 2 {
		t.Fatalf("Expected 2 filters (both accumulated), got %d", len(ctx3.Filters))
	}

	// Verify both filters are present
	if ctx3.Filters[0].Column != "user_id" || ctx3.Filters[0].Value != "90" {
		t.Errorf("First filter incorrect: %+v", ctx3.Filters[0])
	}
	if ctx3.Filters[1].Column != "user_id" || ctx3.Filters[1].Value != "71" {
		t.Errorf("Second filter incorrect: %+v", ctx3.Filters[1])
	}

	// Verify ToQueryParams includes both filters
	params := ctx3.ToQueryParams()
	if len(params.Filters) != 2 {
		t.Fatalf("QueryParams should have 2 filters, got %d", len(params.Filters))
	}
	if params.Filters[0].Column != "user_id" || params.Filters[0].Value != "90" {
		t.Errorf("QueryParams first filter incorrect: %+v", params.Filters[0])
	}
	if params.Filters[1].Column != "user_id" || params.Filters[1].Value != "71" {
		t.Errorf("QueryParams second filter incorrect: %+v", params.Filters[1])
	}
}

// TestPipelineContext_MultipleFilters_SameColumn tests chaining multiple filters on the same column.
// This represents navigating .filter/status/active/.filter/status/inactive/ which is logically impossible.
func TestPipelineContext_MultipleFilters_SameColumn(t *testing.T) {
	tests := []struct {
		name    string
		filters []struct {
			col, val string
			indexed  bool
		}
		want int // expected filter count
	}{
		{
			name: "two .by/ on same column",
			filters: []struct {
				col, val string
				indexed  bool
			}{
				{"status", "active", true},
				{"status", "inactive", true},
			},
			want: 2,
		},
		{
			name: "three .by/ on same column",
			filters: []struct {
				col, val string
				indexed  bool
			}{
				{"type", "a", true},
				{"type", "b", true},
				{"type", "c", true},
			},
			want: 3,
		},
		{
			name: "mixed .by/ and .filter/ on same column",
			filters: []struct {
				col, val string
				indexed  bool
			}{
				{"status", "pending", true},   // .by/
				{"status", "approved", false}, // .filter/
			},
			want: 2,
		},
		{
			name: "filters on different columns then same",
			filters: []struct {
				col, val string
				indexed  bool
			}{
				{"status", "active", true},
				{"role", "admin", true},
				{"status", "pending", true}, // conflicts with first
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewPipelineContext("public", "users", "id")
			for _, f := range tt.filters {
				ctx = ctx.WithFilter(f.col, f.val, f.indexed)
			}

			if len(ctx.Filters) != tt.want {
				t.Errorf("Expected %d filters, got %d", tt.want, len(ctx.Filters))
			}

			// All filters should be properly accumulated
			params := ctx.ToQueryParams()
			if len(params.Filters) != tt.want {
				t.Errorf("QueryParams: expected %d filters, got %d", tt.want, len(params.Filters))
			}
		})
	}
}

// TestPipelineContext_WithOrder tests setting order.
func TestPipelineContext_WithOrder(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")

	// Add ascending order
	ctx2 := ctx.WithOrder("name", false)
	if ctx2.OrderBy != "name" {
		t.Errorf("OrderBy = %q, want %q", ctx2.OrderBy, "name")
	}
	if ctx2.OrderDesc {
		t.Error("OrderDesc should be false")
	}
	if !ctx2.HasOrdered {
		t.Error("HasOrdered should be true")
	}

	// Add descending order
	ctx3 := ctx.WithOrder("created_at", true)
	if ctx3.OrderBy != "created_at" {
		t.Errorf("OrderBy = %q, want %q", ctx3.OrderBy, "created_at")
	}
	if !ctx3.OrderDesc {
		t.Error("OrderDesc should be true")
	}

	// Original should be unchanged
	if ctx.OrderBy != "" {
		t.Error("Original OrderBy was modified")
	}
	if ctx.HasOrdered {
		t.Error("Original HasOrdered was modified")
	}
}

// TestPipelineContext_WithLimit tests setting limits.
func TestPipelineContext_WithLimit(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")

	// Add first limit
	ctx2 := ctx.WithLimit(100, LimitFirst)
	if ctx2.Limit != 100 {
		t.Errorf("Limit = %d, want %d", ctx2.Limit, 100)
	}
	if ctx2.LimitType != LimitFirst {
		t.Errorf("LimitType = %v, want %v", ctx2.LimitType, LimitFirst)
	}
	if ctx2.PreviousLimitType != LimitNone {
		t.Errorf("PreviousLimitType = %v, want %v", ctx2.PreviousLimitType, LimitNone)
	}

	// Add nested limit (should push first to previous)
	ctx3 := ctx2.WithLimit(50, LimitLast)
	if ctx3.Limit != 50 {
		t.Errorf("Limit = %d, want %d", ctx3.Limit, 50)
	}
	if ctx3.LimitType != LimitLast {
		t.Errorf("LimitType = %v, want %v", ctx3.LimitType, LimitLast)
	}
	if ctx3.PreviousLimit != 100 {
		t.Errorf("PreviousLimit = %d, want %d", ctx3.PreviousLimit, 100)
	}
	if ctx3.PreviousLimitType != LimitFirst {
		t.Errorf("PreviousLimitType = %v, want %v", ctx3.PreviousLimitType, LimitFirst)
	}

	// Original should be unchanged
	if ctx.LimitType != LimitNone {
		t.Error("Original LimitType was modified")
	}
}

// TestPipelineContext_WithTerminal tests terminal state.
func TestPipelineContext_WithTerminal(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")
	ctx = ctx.WithFilter("status", "active", true)

	ctx2 := ctx.WithTerminal()
	if !ctx2.IsTerminal {
		t.Error("IsTerminal should be true")
	}

	// Original should be unchanged
	if ctx.IsTerminal {
		t.Error("Original IsTerminal was modified")
	}
}

// TestPipelineContext_CanAddFilter tests filter availability rules.
func TestPipelineContext_CanAddFilter(t *testing.T) {
	tests := []struct {
		name string
		ctx  *PipelineContext
		want bool
	}{
		{
			name: "fresh context",
			ctx:  NewPipelineContext("public", "users", "id"),
			want: true,
		},
		{
			name: "after filter",
			ctx:  NewPipelineContext("public", "users", "id").WithFilter("status", "active", true),
			want: true,
		},
		{
			name: "after limit",
			ctx:  NewPipelineContext("public", "users", "id").WithLimit(100, LimitFirst),
			want: true,
		},
		{
			name: "after order - disallowed",
			ctx:  NewPipelineContext("public", "users", "id").WithOrder("name", false),
			want: false,
		},
		{
			name: "after terminal - disallowed",
			ctx:  NewPipelineContext("public", "users", "id").WithTerminal(),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ctx.CanAddFilter()
			if got != tt.want {
				t.Errorf("CanAddFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPipelineContext_CanAddOrder tests order availability rules.
func TestPipelineContext_CanAddOrder(t *testing.T) {
	tests := []struct {
		name string
		ctx  *PipelineContext
		want bool
	}{
		{
			name: "fresh context",
			ctx:  NewPipelineContext("public", "users", "id"),
			want: true,
		},
		{
			name: "after filter",
			ctx:  NewPipelineContext("public", "users", "id").WithFilter("status", "active", true),
			want: true,
		},
		{
			name: "after limit",
			ctx:  NewPipelineContext("public", "users", "id").WithLimit(100, LimitFirst),
			want: true,
		},
		{
			name: "after sample",
			ctx:  NewPipelineContext("public", "users", "id").WithLimit(50, LimitSample),
			want: true,
		},
		{
			name: "after order - disallowed (redundant)",
			ctx:  NewPipelineContext("public", "users", "id").WithOrder("name", false),
			want: false,
		},
		{
			name: "after terminal - disallowed",
			ctx:  NewPipelineContext("public", "users", "id").WithTerminal(),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ctx.CanAddOrder()
			if got != tt.want {
				t.Errorf("CanAddOrder() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPipelineContext_CanAddLimit tests limit availability rules.
func TestPipelineContext_CanAddLimit(t *testing.T) {
	tests := []struct {
		name      string
		ctx       *PipelineContext
		limitType LimitType
		want      bool
	}{
		// Fresh context - all limits allowed
		{
			name:      "fresh - first",
			ctx:       NewPipelineContext("public", "users", "id"),
			limitType: LimitFirst,
			want:      true,
		},
		{
			name:      "fresh - last",
			ctx:       NewPipelineContext("public", "users", "id"),
			limitType: LimitLast,
			want:      true,
		},
		{
			name:      "fresh - sample",
			ctx:       NewPipelineContext("public", "users", "id"),
			limitType: LimitSample,
			want:      true,
		},

		// After first - no double-first, others allowed
		{
			name:      "after first - first (redundant)",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(100, LimitFirst),
			limitType: LimitFirst,
			want:      false,
		},
		{
			name:      "after first - last",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(100, LimitFirst),
			limitType: LimitLast,
			want:      true,
		},
		{
			name:      "after first - sample",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(100, LimitFirst),
			limitType: LimitSample,
			want:      true,
		},

		// After last - no double-last, others allowed
		{
			name:      "after last - first",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(100, LimitLast),
			limitType: LimitFirst,
			want:      true,
		},
		{
			name:      "after last - last (redundant)",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(100, LimitLast),
			limitType: LimitLast,
			want:      false,
		},
		{
			name:      "after last - sample",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(100, LimitLast),
			limitType: LimitSample,
			want:      true,
		},

		// After sample - no limits allowed (just sample fewer)
		{
			name:      "after sample - first (disallowed)",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(50, LimitSample),
			limitType: LimitFirst,
			want:      false,
		},
		{
			name:      "after sample - last (disallowed)",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(50, LimitSample),
			limitType: LimitLast,
			want:      false,
		},
		{
			name:      "after sample - sample (disallowed)",
			ctx:       NewPipelineContext("public", "users", "id").WithLimit(50, LimitSample),
			limitType: LimitSample,
			want:      false,
		},

		// After terminal - nothing allowed
		{
			name:      "after terminal - first",
			ctx:       NewPipelineContext("public", "users", "id").WithTerminal(),
			limitType: LimitFirst,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ctx.CanAddLimit(tt.limitType)
			if got != tt.want {
				t.Errorf("CanAddLimit(%v) = %v, want %v", tt.limitType, got, tt.want)
			}
		})
	}
}

// TestPipelineContext_CanExport tests export availability.
func TestPipelineContext_CanExport(t *testing.T) {
	tests := []struct {
		name string
		ctx  *PipelineContext
		want bool
	}{
		{
			name: "fresh context",
			ctx:  NewPipelineContext("public", "users", "id"),
			want: true,
		},
		{
			name: "after filter",
			ctx:  NewPipelineContext("public", "users", "id").WithFilter("status", "active", true),
			want: true,
		},
		{
			name: "after order",
			ctx:  NewPipelineContext("public", "users", "id").WithOrder("name", false),
			want: true,
		},
		{
			name: "after limit",
			ctx:  NewPipelineContext("public", "users", "id").WithLimit(100, LimitFirst),
			want: true,
		},
		{
			name: "after terminal - disallowed",
			ctx:  NewPipelineContext("public", "users", "id").WithTerminal(),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ctx.CanExport()
			if got != tt.want {
				t.Errorf("CanExport() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPipelineContext_HasFilters tests filter presence check.
func TestPipelineContext_HasFilters(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")
	if ctx.HasFilters() {
		t.Error("Fresh context should not have filters")
	}

	ctx2 := ctx.WithFilter("status", "active", true)
	if !ctx2.HasFilters() {
		t.Error("Context with filter should have filters")
	}
}

// TestPipelineContext_HasLimit tests limit presence check.
func TestPipelineContext_HasLimit(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")
	if ctx.HasLimit() {
		t.Error("Fresh context should not have limit")
	}

	ctx2 := ctx.WithLimit(100, LimitFirst)
	if !ctx2.HasLimit() {
		t.Error("Context with limit should have limit")
	}
}

// TestPipelineContext_HasNestedLimit tests nested limit detection.
func TestPipelineContext_HasNestedLimit(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")
	if ctx.HasNestedLimit() {
		t.Error("Fresh context should not have nested limit")
	}

	ctx2 := ctx.WithLimit(100, LimitFirst)
	if ctx2.HasNestedLimit() {
		t.Error("Single limit should not be nested")
	}

	ctx3 := ctx2.WithLimit(50, LimitLast)
	if !ctx3.HasNestedLimit() {
		t.Error("Double limit should be nested")
	}
}

// TestPipelineContext_NeedsSubquery tests subquery requirement detection.
func TestPipelineContext_NeedsSubquery(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")
	if ctx.NeedsSubquery() {
		t.Error("Fresh context should not need subquery")
	}

	ctx2 := ctx.WithLimit(100, LimitFirst)
	if ctx2.NeedsSubquery() {
		t.Error("Single limit should not need subquery")
	}

	ctx3 := ctx2.WithLimit(50, LimitLast)
	if !ctx3.NeedsSubquery() {
		t.Error("Nested limit should need subquery")
	}
}

// TestPipelineContext_AvailableCapabilities tests capability listing.
func TestPipelineContext_AvailableCapabilities(t *testing.T) {
	tests := []struct {
		name     string
		ctx      *PipelineContext
		contains []string
		excludes []string
	}{
		{
			name:     "fresh context - all available",
			ctx:      NewPipelineContext("public", "users", "id"),
			contains: []string{".by", ".filter", ".order", ".first", ".last", ".sample", ".export"},
			excludes: nil,
		},
		{
			name:     "after filter - all available",
			ctx:      NewPipelineContext("public", "users", "id").WithFilter("status", "active", true),
			contains: []string{".by", ".filter", ".order", ".first", ".last", ".sample", ".export"},
			excludes: nil,
		},
		{
			name:     "after order - no filters or order",
			ctx:      NewPipelineContext("public", "users", "id").WithOrder("name", false),
			contains: []string{".first", ".last", ".sample", ".export"},
			excludes: []string{".by", ".filter", ".order"},
		},
		{
			name:     "after first - no double first",
			ctx:      NewPipelineContext("public", "users", "id").WithLimit(100, LimitFirst),
			contains: []string{".by", ".filter", ".order", ".last", ".sample", ".export"},
			excludes: []string{".first"},
		},
		{
			name:     "after last - no double last",
			ctx:      NewPipelineContext("public", "users", "id").WithLimit(100, LimitLast),
			contains: []string{".by", ".filter", ".order", ".first", ".sample", ".export"},
			excludes: []string{".last"},
		},
		{
			name:     "after sample - no limits",
			ctx:      NewPipelineContext("public", "users", "id").WithLimit(50, LimitSample),
			contains: []string{".by", ".filter", ".order", ".export"},
			excludes: []string{".first", ".last", ".sample"},
		},
		{
			name:     "after terminal - nothing",
			ctx:      NewPipelineContext("public", "users", "id").WithTerminal(),
			contains: nil,
			excludes: []string{".by", ".filter", ".order", ".first", ".last", ".sample", ".export"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caps := tt.ctx.AvailableCapabilities()
			capSet := make(map[string]bool)
			for _, c := range caps {
				capSet[c] = true
			}

			for _, c := range tt.contains {
				if !capSet[c] {
					t.Errorf("Expected capability %q to be available, but it wasn't. Got: %v", c, caps)
				}
			}

			for _, c := range tt.excludes {
				if capSet[c] {
					t.Errorf("Capability %q should not be available, but it was. Got: %v", c, caps)
				}
			}
		})
	}
}

// TestPipelineContext_ToQueryParams tests conversion to query params.
func TestPipelineContext_ToQueryParams(t *testing.T) {
	ctx := NewPipelineContext("public", "users", "id")
	ctx = ctx.WithFilter("status", "active", true)
	ctx = ctx.WithFilter("role", "admin", false)
	ctx = ctx.WithOrder("name", true)
	ctx = ctx.WithLimit(100, LimitFirst)
	ctx = ctx.WithLimit(50, LimitLast)

	params := ctx.ToQueryParams()

	if params.Schema != "public" {
		t.Errorf("Schema = %q, want %q", params.Schema, "public")
	}
	if params.Table != "users" {
		t.Errorf("Table = %q, want %q", params.Table, "users")
	}
	if params.PKColumn != "id" {
		t.Errorf("PKColumn = %q, want %q", params.PKColumn, "id")
	}
	if len(params.Filters) != 2 {
		t.Fatalf("Filters length = %d, want 2", len(params.Filters))
	}
	if params.Filters[0].Column != "status" || params.Filters[0].Value != "active" {
		t.Errorf("First filter = %+v, want status=active", params.Filters[0])
	}
	if params.Filters[1].Column != "role" || params.Filters[1].Value != "admin" {
		t.Errorf("Second filter = %+v, want role=admin", params.Filters[1])
	}
	if params.OrderBy != "name" {
		t.Errorf("OrderBy = %q, want %q", params.OrderBy, "name")
	}
	if !params.OrderDesc {
		t.Error("OrderDesc should be true")
	}
	if params.Limit != 50 {
		t.Errorf("Limit = %d, want 50", params.Limit)
	}
	if params.LimitType != LimitLast {
		t.Errorf("LimitType = %v, want %v", params.LimitType, LimitLast)
	}
	if params.PreviousLimit != 100 {
		t.Errorf("PreviousLimit = %d, want 100", params.PreviousLimit)
	}
	if params.PreviousLimitType != LimitFirst {
		t.Errorf("PreviousLimitType = %v, want %v", params.PreviousLimitType, LimitFirst)
	}

	// Verify filters slice independence
	params.Filters[0].Column = "modified"
	if ctx.Filters[0].Column == "modified" {
		t.Error("QueryParams filters should be independent from context")
	}
}

// TestPipelineContext_ComplexPipelines tests complex pipeline scenarios.
func TestPipelineContext_ComplexPipelines(t *testing.T) {
	t.Run(".by/status/active/.by/tier/premium/.first/100/.export/", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithFilter("status", "active", true)
		ctx = ctx.WithFilter("tier", "premium", true)
		ctx = ctx.WithLimit(100, LimitFirst)
		ctx = ctx.WithTerminal()

		if len(ctx.Filters) != 2 {
			t.Errorf("Expected 2 filters, got %d", len(ctx.Filters))
		}
		if ctx.Limit != 100 {
			t.Errorf("Expected limit 100, got %d", ctx.Limit)
		}
		if !ctx.IsTerminal {
			t.Error("Expected terminal state")
		}
	})

	t.Run(".first/100/.last/50/ (nested pagination)", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithLimit(100, LimitFirst)
		ctx = ctx.WithLimit(50, LimitLast)

		if !ctx.NeedsSubquery() {
			t.Error("Nested pagination should need subquery")
		}
		if ctx.PreviousLimit != 100 || ctx.PreviousLimitType != LimitFirst {
			t.Error("Previous limit not set correctly")
		}
		if ctx.Limit != 50 || ctx.LimitType != LimitLast {
			t.Error("Current limit not set correctly")
		}
	})

	t.Run(".sample/50/.order/name/ (sample then sort)", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithLimit(50, LimitSample)

		if !ctx.CanAddOrder() {
			t.Error("Order should be allowed after sample")
		}

		ctx = ctx.WithOrder("name", false)
		if ctx.OrderBy != "name" {
			t.Error("Order not set correctly")
		}
	})

	t.Run(".filter/status/active/.first/1000/.sample/50/", func(t *testing.T) {
		ctx := NewPipelineContext("public", "events", "id")
		ctx = ctx.WithFilter("status", "active", false) // non-indexed
		ctx = ctx.WithLimit(1000, LimitFirst)

		if !ctx.CanAddLimit(LimitSample) {
			t.Error("Sample should be allowed after first")
		}

		ctx = ctx.WithLimit(50, LimitSample)
		if ctx.Limit != 50 || ctx.LimitType != LimitSample {
			t.Error("Sample limit not set correctly")
		}
		if ctx.PreviousLimit != 1000 || ctx.PreviousLimitType != LimitFirst {
			t.Error("Previous first limit not preserved")
		}
	})
}

// =============================================================================
// Deep Nesting Tests
// =============================================================================

// TestPipelineContext_DeepFilterNesting tests deep filter chains like
// .by/col1/.by/col2/.by/col3/.filter/col4/.by/col5
func TestPipelineContext_DeepFilterNesting(t *testing.T) {
	t.Run("5 indexed filters (.by chains)", func(t *testing.T) {
		ctx := NewPipelineContext("public", "orders", "id")

		// Build: .by/customer_id/123/.by/status/pending/.by/region/us/.by/tier/premium/.by/channel/web/
		ctx = ctx.WithFilter("customer_id", "123", true)
		ctx = ctx.WithFilter("status", "pending", true)
		ctx = ctx.WithFilter("region", "us", true)
		ctx = ctx.WithFilter("tier", "premium", true)
		ctx = ctx.WithFilter("channel", "web", true)

		if len(ctx.Filters) != 5 {
			t.Fatalf("Expected 5 filters, got %d", len(ctx.Filters))
		}

		// All filters should be AND-combined
		expectedFilters := []struct {
			col, val string
			indexed  bool
		}{
			{"customer_id", "123", true},
			{"status", "pending", true},
			{"region", "us", true},
			{"tier", "premium", true},
			{"channel", "web", true},
		}

		for i, exp := range expectedFilters {
			if ctx.Filters[i].Column != exp.col {
				t.Errorf("Filter %d column = %q, want %q", i, ctx.Filters[i].Column, exp.col)
			}
			if ctx.Filters[i].Value != exp.val {
				t.Errorf("Filter %d value = %q, want %q", i, ctx.Filters[i].Value, exp.val)
			}
			if ctx.Filters[i].Indexed != exp.indexed {
				t.Errorf("Filter %d indexed = %v, want %v", i, ctx.Filters[i].Indexed, exp.indexed)
			}
		}

		// Should still allow more filters (not after order)
		if !ctx.CanAddFilter() {
			t.Error("Should still allow filters after 5 .by/ chains")
		}
	})

	t.Run("mixed .by and .filter chains", func(t *testing.T) {
		ctx := NewPipelineContext("public", "events", "id")

		// Build: .by/col1/v1/.by/col2/v2/.filter/col3/v3/.by/col4/v4/.filter/col5/v5/
		ctx = ctx.WithFilter("user_id", "123", true)     // .by/ (indexed)
		ctx = ctx.WithFilter("type", "click", true)      // .by/ (indexed)
		ctx = ctx.WithFilter("browser", "chrome", false) // .filter/ (non-indexed)
		ctx = ctx.WithFilter("campaign", "summer", true) // .by/ (indexed)
		ctx = ctx.WithFilter("country", "us", false)     // .filter/ (non-indexed)

		if len(ctx.Filters) != 5 {
			t.Fatalf("Expected 5 filters, got %d", len(ctx.Filters))
		}

		// Verify indexed flags
		indexedCounts := map[bool]int{}
		for _, f := range ctx.Filters {
			indexedCounts[f.Indexed]++
		}

		if indexedCounts[true] != 3 {
			t.Errorf("Expected 3 indexed filters, got %d", indexedCounts[true])
		}
		if indexedCounts[false] != 2 {
			t.Errorf("Expected 2 non-indexed filters, got %d", indexedCounts[false])
		}
	})

	t.Run("filter chain with limit and more filters", func(t *testing.T) {
		ctx := NewPipelineContext("public", "logs", "id")

		// Build: .by/level/error/.first/1000/.filter/source/api/.by/user_id/456/
		ctx = ctx.WithFilter("level", "error", true)
		ctx = ctx.WithLimit(1000, LimitFirst)
		ctx = ctx.WithFilter("source", "api", false)
		ctx = ctx.WithFilter("user_id", "456", true)

		if len(ctx.Filters) != 3 {
			t.Fatalf("Expected 3 filters, got %d", len(ctx.Filters))
		}

		if ctx.Limit != 1000 || ctx.LimitType != LimitFirst {
			t.Error("Limit should be preserved with filters")
		}

		// First filter before limit, other two after
		if ctx.Filters[0].Column != "level" {
			t.Error("First filter should be 'level'")
		}
		if ctx.Filters[1].Column != "source" {
			t.Error("Second filter should be 'source'")
		}
		if ctx.Filters[2].Column != "user_id" {
			t.Error("Third filter should be 'user_id'")
		}
	})
}

// TestPipelineContext_DeepPaginationNesting tests nested limit operations.
func TestPipelineContext_DeepPaginationNesting(t *testing.T) {
	t.Run(".first/1000/.last/100/ (rows 901-1000)", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithLimit(1000, LimitFirst)
		ctx = ctx.WithLimit(100, LimitLast)

		if !ctx.NeedsSubquery() {
			t.Error("Should need subquery for nested limits")
		}
		if ctx.PreviousLimit != 1000 || ctx.PreviousLimitType != LimitFirst {
			t.Error("Previous limit incorrect")
		}
		if ctx.Limit != 100 || ctx.LimitType != LimitLast {
			t.Error("Current limit incorrect")
		}
	})

	t.Run(".last/1000/.first/100/ (rows n-999 to n-900)", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithLimit(1000, LimitLast)
		ctx = ctx.WithLimit(100, LimitFirst)

		if !ctx.NeedsSubquery() {
			t.Error("Should need subquery for nested limits")
		}
		if ctx.PreviousLimit != 1000 || ctx.PreviousLimitType != LimitLast {
			t.Error("Previous limit incorrect")
		}
		if ctx.Limit != 100 || ctx.LimitType != LimitFirst {
			t.Error("Current limit incorrect")
		}
	})

	t.Run(".first/10000/.sample/100/ (sample from first 10k)", func(t *testing.T) {
		ctx := NewPipelineContext("public", "events", "id")
		ctx = ctx.WithLimit(10000, LimitFirst)
		ctx = ctx.WithLimit(100, LimitSample)

		if !ctx.NeedsSubquery() {
			t.Error("Should need subquery for first+sample")
		}
		if ctx.PreviousLimitType != LimitFirst {
			t.Error("Previous should be First")
		}
		if ctx.LimitType != LimitSample {
			t.Error("Current should be Sample")
		}
	})
}

// =============================================================================
// ADR-007 Capability Matrix Tests
// =============================================================================

// TestPipelineContext_ADR007_CapabilityMatrix tests all parent→child combinations
// from the ADR-007 capability matrix.
func TestPipelineContext_ADR007_CapabilityMatrix(t *testing.T) {
	// Test each row of the capability matrix

	t.Run("Table root - all capabilities allowed", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")

		if !ctx.CanAddFilter() {
			t.Error(".by and .filter should be allowed from table root")
		}
		if !ctx.CanAddOrder() {
			t.Error(".order should be allowed from table root")
		}
		if !ctx.CanAddLimit(LimitFirst) {
			t.Error(".first should be allowed from table root")
		}
		if !ctx.CanAddLimit(LimitLast) {
			t.Error(".last should be allowed from table root")
		}
		if !ctx.CanAddLimit(LimitSample) {
			t.Error(".sample should be allowed from table root")
		}
		if !ctx.CanExport() {
			t.Error(".export should be allowed from table root")
		}
	})

	t.Run(".by/<col>/<val>/ - all capabilities allowed", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithFilter("status", "active", true)

		if !ctx.CanAddFilter() {
			t.Error(".by and .filter should be allowed after .by")
		}
		if !ctx.CanAddOrder() {
			t.Error(".order should be allowed after .by")
		}
		if !ctx.CanAddLimit(LimitFirst) {
			t.Error(".first should be allowed after .by")
		}
		if !ctx.CanAddLimit(LimitLast) {
			t.Error(".last should be allowed after .by")
		}
		if !ctx.CanAddLimit(LimitSample) {
			t.Error(".sample should be allowed after .by")
		}
		if !ctx.CanExport() {
			t.Error(".export should be allowed after .by")
		}
	})

	t.Run(".filter/<col>/<val>/ - all capabilities allowed", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithFilter("name", "Alice", false) // non-indexed

		if !ctx.CanAddFilter() {
			t.Error(".by and .filter should be allowed after .filter")
		}
		if !ctx.CanAddOrder() {
			t.Error(".order should be allowed after .filter")
		}
		if !ctx.CanAddLimit(LimitFirst) {
			t.Error(".first should be allowed after .filter")
		}
		if !ctx.CanAddLimit(LimitLast) {
			t.Error(".last should be allowed after .filter")
		}
		if !ctx.CanAddLimit(LimitSample) {
			t.Error(".sample should be allowed after .filter")
		}
		if !ctx.CanExport() {
			t.Error(".export should be allowed after .filter")
		}
	})

	t.Run(".order/<col>/ - only limits and export allowed", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithOrder("name", false)

		// Disallowed
		if ctx.CanAddFilter() {
			t.Error(".by and .filter should be DISALLOWED after .order")
		}
		if ctx.CanAddOrder() {
			t.Error(".order should be DISALLOWED after .order (redundant)")
		}

		// Allowed
		if !ctx.CanAddLimit(LimitFirst) {
			t.Error(".first should be allowed after .order")
		}
		if !ctx.CanAddLimit(LimitLast) {
			t.Error(".last should be allowed after .order")
		}
		if !ctx.CanAddLimit(LimitSample) {
			t.Error(".sample should be allowed after .order")
		}
		if !ctx.CanExport() {
			t.Error(".export should be allowed after .order")
		}
	})

	t.Run(".first/N/ - no double-first", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithLimit(100, LimitFirst)

		// Allowed
		if !ctx.CanAddFilter() {
			t.Error(".by and .filter should be allowed after .first")
		}
		if !ctx.CanAddOrder() {
			t.Error(".order should be allowed after .first")
		}
		if !ctx.CanAddLimit(LimitLast) {
			t.Error(".last should be allowed after .first (nested pagination)")
		}
		if !ctx.CanAddLimit(LimitSample) {
			t.Error(".sample should be allowed after .first")
		}
		if !ctx.CanExport() {
			t.Error(".export should be allowed after .first")
		}

		// Disallowed
		if ctx.CanAddLimit(LimitFirst) {
			t.Error(".first should be DISALLOWED after .first (redundant)")
		}
	})

	t.Run(".last/N/ - no double-last", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithLimit(100, LimitLast)

		// Allowed
		if !ctx.CanAddFilter() {
			t.Error(".by and .filter should be allowed after .last")
		}
		if !ctx.CanAddOrder() {
			t.Error(".order should be allowed after .last")
		}
		if !ctx.CanAddLimit(LimitFirst) {
			t.Error(".first should be allowed after .last (nested pagination)")
		}
		if !ctx.CanAddLimit(LimitSample) {
			t.Error(".sample should be allowed after .last")
		}
		if !ctx.CanExport() {
			t.Error(".export should be allowed after .last")
		}

		// Disallowed
		if ctx.CanAddLimit(LimitLast) {
			t.Error(".last should be DISALLOWED after .last (redundant)")
		}
	})

	t.Run(".sample/N/ - no limits allowed", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithLimit(50, LimitSample)

		// Allowed
		if !ctx.CanAddFilter() {
			t.Error(".by and .filter should be allowed after .sample")
		}
		if !ctx.CanAddOrder() {
			t.Error(".order should be allowed after .sample")
		}
		if !ctx.CanExport() {
			t.Error(".export should be allowed after .sample")
		}

		// Disallowed
		if ctx.CanAddLimit(LimitFirst) {
			t.Error(".first should be DISALLOWED after .sample")
		}
		if ctx.CanAddLimit(LimitLast) {
			t.Error(".last should be DISALLOWED after .sample")
		}
		if ctx.CanAddLimit(LimitSample) {
			t.Error(".sample should be DISALLOWED after .sample")
		}
	})

	t.Run(".export/ - terminal, nothing allowed", func(t *testing.T) {
		ctx := NewPipelineContext("public", "users", "id")
		ctx = ctx.WithTerminal()

		if ctx.CanAddFilter() {
			t.Error(".by and .filter should be DISALLOWED after .export")
		}
		if ctx.CanAddOrder() {
			t.Error(".order should be DISALLOWED after .export")
		}
		if ctx.CanAddLimit(LimitFirst) {
			t.Error(".first should be DISALLOWED after .export")
		}
		if ctx.CanAddLimit(LimitLast) {
			t.Error(".last should be DISALLOWED after .export")
		}
		if ctx.CanAddLimit(LimitSample) {
			t.Error(".sample should be DISALLOWED after .export")
		}
		if ctx.CanExport() {
			t.Error(".export should be DISALLOWED after .export")
		}
	})
}

// =============================================================================
// Full Pipeline Path Tests
// =============================================================================

// TestPipelineContext_FullPaths tests complete pipeline paths from ADR-007 examples.
func TestPipelineContext_FullPaths(t *testing.T) {
	t.Run(".by/customer_id/123/.order/created_at/.last/10/.export/json", func(t *testing.T) {
		ctx := NewPipelineContext("public", "orders", "id")

		// Step 1: .by/customer_id/123/
		ctx = ctx.WithFilter("customer_id", "123", true)
		if len(ctx.Filters) != 1 {
			t.Fatal("Filter not added")
		}

		// Step 2: .order/created_at/
		if !ctx.CanAddOrder() {
			t.Fatal("Order should be allowed after .by")
		}
		ctx = ctx.WithOrder("created_at", false)

		// Step 3: .last/10/
		if !ctx.CanAddLimit(LimitLast) {
			t.Fatal(".last should be allowed after .order")
		}
		ctx = ctx.WithLimit(10, LimitLast)

		// Step 4: .export/json
		if !ctx.CanExport() {
			t.Fatal(".export should be allowed")
		}
		ctx = ctx.WithTerminal()

		// Final verification
		if len(ctx.Filters) != 1 {
			t.Error("Should have 1 filter")
		}
		if ctx.OrderBy != "created_at" {
			t.Error("Order should be created_at")
		}
		if ctx.Limit != 10 || ctx.LimitType != LimitLast {
			t.Error("Limit should be last 10")
		}
		if !ctx.IsTerminal {
			t.Error("Should be terminal")
		}
	})

	t.Run(".filter/status/active/.first/1000/.sample/50/.export/csv", func(t *testing.T) {
		ctx := NewPipelineContext("public", "events", "id")

		// Step 1: .filter/status/active/
		ctx = ctx.WithFilter("status", "active", false)

		// Step 2: .first/1000/
		ctx = ctx.WithLimit(1000, LimitFirst)

		// Step 3: .sample/50/
		if !ctx.CanAddLimit(LimitSample) {
			t.Fatal(".sample should be allowed after .first")
		}
		ctx = ctx.WithLimit(50, LimitSample)

		// Step 4: .export/csv
		ctx = ctx.WithTerminal()

		// Final verification
		if !ctx.NeedsSubquery() {
			t.Error("Should need subquery for nested limits")
		}
		if ctx.PreviousLimit != 1000 {
			t.Error("Previous limit should be 1000")
		}
		if ctx.Limit != 50 || ctx.LimitType != LimitSample {
			t.Error("Current limit should be sample 50")
		}
	})

	t.Run(".by/a/1/.by/b/2/.first/100/.last/50/.export/json", func(t *testing.T) {
		ctx := NewPipelineContext("public", "data", "id")

		// Build path
		ctx = ctx.WithFilter("a", "1", true)
		ctx = ctx.WithFilter("b", "2", true)
		ctx = ctx.WithLimit(100, LimitFirst)
		ctx = ctx.WithLimit(50, LimitLast)
		ctx = ctx.WithTerminal()

		// Verify
		if len(ctx.Filters) != 2 {
			t.Errorf("Expected 2 filters, got %d", len(ctx.Filters))
		}
		if ctx.PreviousLimit != 100 || ctx.PreviousLimitType != LimitFirst {
			t.Error("Previous limit incorrect")
		}
		if ctx.Limit != 50 || ctx.LimitType != LimitLast {
			t.Error("Current limit incorrect")
		}
	})

	t.Run(".sample/100/.order/price/.export/csv", func(t *testing.T) {
		ctx := NewPipelineContext("public", "products", "id")

		// Step 1: .sample/100/
		ctx = ctx.WithLimit(100, LimitSample)

		// Step 2: .order/price/ (sort the sample)
		if !ctx.CanAddOrder() {
			t.Fatal(".order should be allowed after .sample")
		}
		ctx = ctx.WithOrder("price", false)

		// Step 3: .export/csv
		ctx = ctx.WithTerminal()

		// Verify
		if ctx.LimitType != LimitSample {
			t.Error("Should be sample limit")
		}
		if ctx.OrderBy != "price" {
			t.Error("Should be ordered by price")
		}
	})

	t.Run("deep nesting: .by/col1/.filter/col2/.by/col3/.first/500/.by/col4/.order/col5/.last/100/", func(t *testing.T) {
		ctx := NewPipelineContext("public", "complex", "id")

		// Build deep pipeline
		ctx = ctx.WithFilter("col1", "v1", true)  // .by/
		ctx = ctx.WithFilter("col2", "v2", false) // .filter/
		ctx = ctx.WithFilter("col3", "v3", true)  // .by/
		ctx = ctx.WithLimit(500, LimitFirst)      // .first/500/
		ctx = ctx.WithFilter("col4", "v4", true)  // .by/ (filter after limit!)

		// After limit, can still filter
		if len(ctx.Filters) != 4 {
			t.Errorf("Expected 4 filters, got %d", len(ctx.Filters))
		}

		// Then order
		ctx = ctx.WithOrder("col5", false) // .order/

		// After order, no more filters allowed
		if ctx.CanAddFilter() {
			t.Error("Filters should be disallowed after .order")
		}

		// But can still add .last
		ctx = ctx.WithLimit(100, LimitLast) // .last/100/

		// Verify final state
		if len(ctx.Filters) != 4 {
			t.Errorf("Should still have 4 filters, got %d", len(ctx.Filters))
		}
		if ctx.OrderBy != "col5" {
			t.Error("Order should be col5")
		}
		if ctx.Limit != 100 || ctx.LimitType != LimitLast {
			t.Error("Final limit should be last 100")
		}
	})
}

// TestPipelineContext_ImmutabilityUnderDeepNesting verifies immutability is preserved
// across many operations.
func TestPipelineContext_ImmutabilityUnderDeepNesting(t *testing.T) {
	// Create a chain of contexts
	ctx0 := NewPipelineContext("public", "test", "id")
	ctx1 := ctx0.WithFilter("a", "1", true)
	ctx2 := ctx1.WithFilter("b", "2", true)
	ctx3 := ctx2.WithFilter("c", "3", false)
	ctx4 := ctx3.WithLimit(100, LimitFirst)
	ctx5 := ctx4.WithFilter("d", "4", true)
	ctx6 := ctx5.WithOrder("e", true)
	ctx7 := ctx6.WithLimit(50, LimitLast)
	ctx8 := ctx7.WithTerminal()

	// Verify each context has the correct number of filters
	expectedFilters := []int{0, 1, 2, 3, 3, 4, 4, 4, 4}
	contexts := []*PipelineContext{ctx0, ctx1, ctx2, ctx3, ctx4, ctx5, ctx6, ctx7, ctx8}

	for i, ctx := range contexts {
		if len(ctx.Filters) != expectedFilters[i] {
			t.Errorf("ctx%d: expected %d filters, got %d", i, expectedFilters[i], len(ctx.Filters))
		}
	}

	// Verify no context was mutated by later operations
	if ctx0.HasFilters() {
		t.Error("ctx0 should have no filters")
	}
	if ctx1.Filters[0].Column != "a" {
		t.Error("ctx1 filter should be 'a'")
	}
	if ctx4.HasOrdered {
		t.Error("ctx4 should not be ordered")
	}
	if ctx6.LimitType != LimitFirst {
		t.Error("ctx6 should still have first limit type")
	}
	if ctx7.IsTerminal {
		t.Error("ctx7 should not be terminal")
	}
	if !ctx8.IsTerminal {
		t.Error("ctx8 should be terminal")
	}
}
