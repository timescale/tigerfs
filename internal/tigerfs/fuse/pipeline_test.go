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
