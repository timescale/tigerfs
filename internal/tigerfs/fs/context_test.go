// Package fs tests for context.go (FSContext)
package fs

import (
	"testing"
)

// TestNewFSContext verifies context creation.
func TestNewFSContext(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})

	if ctx.Schema != "public" {
		t.Errorf("Schema = %q, want %q", ctx.Schema, "public")
	}
	if ctx.TableName != "users" {
		t.Errorf("TableName = %q, want %q", ctx.TableName, "users")
	}
	if len(ctx.PKColumns) != 1 || ctx.PKColumns[0] != "id" {
		t.Errorf("PKColumns = %v, want [id]", ctx.PKColumns)
	}
	if ctx.LimitType != LimitNone {
		t.Errorf("LimitType = %v, want %v", ctx.LimitType, LimitNone)
	}
	if len(ctx.Filters) != 0 {
		t.Errorf("Filters = %v, want empty", ctx.Filters)
	}
}

// TestCompositePK_Context verifies FSContext with composite primary keys.
func TestCompositePK_Context(t *testing.T) {
	ctx := NewFSContext("public", "orders", []string{"customer_id", "product_id"})

	// Verify PKColumns has 2 elements
	if len(ctx.PKColumns) != 2 {
		t.Fatalf("PKColumns has %d elements, want 2", len(ctx.PKColumns))
	}
	if ctx.PKColumns[0] != "customer_id" || ctx.PKColumns[1] != "product_id" {
		t.Errorf("PKColumns = %v, want [customer_id, product_id]", ctx.PKColumns)
	}

	// Verify ToQueryParams copies PKColumns
	params := ctx.ToQueryParams()
	if len(params.PKColumns) != 2 {
		t.Fatalf("params.PKColumns has %d elements, want 2", len(params.PKColumns))
	}
	if params.PKColumns[0] != "customer_id" || params.PKColumns[1] != "product_id" {
		t.Errorf("params.PKColumns = %v, want [customer_id, product_id]", params.PKColumns)
	}

	// Verify Clone preserves PKColumns
	clone := ctx.Clone()
	if len(clone.PKColumns) != 2 {
		t.Fatalf("clone.PKColumns has %d elements, want 2", len(clone.PKColumns))
	}
	if clone.PKColumns[0] != "customer_id" || clone.PKColumns[1] != "product_id" {
		t.Errorf("clone.PKColumns = %v, want [customer_id, product_id]", clone.PKColumns)
	}

	// Verify WithFilter preserves PKColumns
	filtered := ctx.WithFilter("status", "active", false)
	if len(filtered.PKColumns) != 2 {
		t.Fatalf("filtered.PKColumns has %d elements, want 2", len(filtered.PKColumns))
	}
	if filtered.PKColumns[0] != "customer_id" || filtered.PKColumns[1] != "product_id" {
		t.Errorf("filtered.PKColumns = %v, want [customer_id, product_id]", filtered.PKColumns)
	}
}

// TestFSContextClone verifies independent copy.
func TestFSContextClone(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx = ctx.WithFilter("status", "active", false)

	clone := ctx.Clone()

	// Modify clone's filters
	clone.Filters[0].Value = "inactive"

	// Original should be unchanged
	if ctx.Filters[0].Value != "active" {
		t.Errorf("Original filter modified: got %q, want %q", ctx.Filters[0].Value, "active")
	}
}

// TestFSContextCloneNil verifies nil clone returns nil.
func TestFSContextCloneNil(t *testing.T) {
	var ctx *FSContext
	if ctx.Clone() != nil {
		t.Error("Clone of nil should return nil")
	}
}

// TestFSContextWithFilter verifies filter addition.
func TestFSContextWithFilter(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx2 := ctx.WithFilter("status", "active", false)
	ctx3 := ctx2.WithFilter("role", "admin", true)

	// Original unchanged
	if len(ctx.Filters) != 0 {
		t.Errorf("Original has filters: %v", ctx.Filters)
	}

	// First filter added
	if len(ctx2.Filters) != 1 {
		t.Fatalf("ctx2 has %d filters, want 1", len(ctx2.Filters))
	}
	if ctx2.Filters[0].Column != "status" || ctx2.Filters[0].Value != "active" {
		t.Errorf("Filter = %+v, want status=active", ctx2.Filters[0])
	}
	if ctx2.Filters[0].Indexed {
		t.Error("Indexed should be false for .filter/")
	}

	// Second filter added
	if len(ctx3.Filters) != 2 {
		t.Fatalf("ctx3 has %d filters, want 2", len(ctx3.Filters))
	}
	if !ctx3.Filters[1].Indexed {
		t.Error("Indexed should be true for .by/")
	}
}

// TestFSContextWithOrder verifies ordering.
func TestFSContextWithOrder(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx2 := ctx.WithOrder("created_at", true)

	if ctx.HasOrdered {
		t.Error("Original should not be ordered")
	}
	if !ctx2.HasOrdered {
		t.Error("ctx2 should be ordered")
	}
	if ctx2.OrderBy != "created_at" {
		t.Errorf("OrderBy = %q, want %q", ctx2.OrderBy, "created_at")
	}
	if !ctx2.OrderDesc {
		t.Error("OrderDesc should be true")
	}
}

// TestFSContextWithLimit verifies limit application.
func TestFSContextWithLimit(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx2 := ctx.WithLimit(10, LimitFirst)

	if ctx.LimitType != LimitNone {
		t.Error("Original should have no limit")
	}
	if ctx2.Limit != 10 {
		t.Errorf("Limit = %d, want 10", ctx2.Limit)
	}
	if ctx2.LimitType != LimitFirst {
		t.Errorf("LimitType = %v, want %v", ctx2.LimitType, LimitFirst)
	}
}

// TestFSContextNestedLimit verifies nested pagination.
func TestFSContextNestedLimit(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx = ctx.WithLimit(100, LimitFirst) // .first/100/
	ctx = ctx.WithLimit(10, LimitLast)   // .first/100/.last/10/

	if ctx.Limit != 10 {
		t.Errorf("Limit = %d, want 10", ctx.Limit)
	}
	if ctx.LimitType != LimitLast {
		t.Errorf("LimitType = %v, want %v", ctx.LimitType, LimitLast)
	}
	if ctx.PreviousLimit != 100 {
		t.Errorf("PreviousLimit = %d, want 100", ctx.PreviousLimit)
	}
	if ctx.PreviousLimitType != LimitFirst {
		t.Errorf("PreviousLimitType = %v, want %v", ctx.PreviousLimitType, LimitFirst)
	}
}

// TestFSContextWithTerminal verifies terminal state.
func TestFSContextWithTerminal(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx2 := ctx.WithTerminal()

	if ctx.IsTerminal {
		t.Error("Original should not be terminal")
	}
	if !ctx2.IsTerminal {
		t.Error("ctx2 should be terminal")
	}
}

// TestFSContextCanAddFilter verifies filter permission rules.
func TestFSContextCanAddFilter(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})

	// Can add filter initially
	if !ctx.CanAddFilter() {
		t.Error("Should be able to add filter initially")
	}

	// Can add filter after filter
	ctx = ctx.WithFilter("a", "1", false)
	if !ctx.CanAddFilter() {
		t.Error("Should be able to add filter after filter")
	}

	// Cannot add filter after order
	ctx = ctx.WithOrder("id", false)
	if ctx.CanAddFilter() {
		t.Error("Should not be able to add filter after order")
	}

	// Cannot add filter when terminal
	ctx = NewFSContext("public", "users", []string{"id"}).WithTerminal()
	if ctx.CanAddFilter() {
		t.Error("Should not be able to add filter when terminal")
	}
}

// TestFSContextCanAddOrder verifies order permission rules.
func TestFSContextCanAddOrder(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})

	if !ctx.CanAddOrder() {
		t.Error("Should be able to add order initially")
	}

	ctx = ctx.WithOrder("id", false)
	if ctx.CanAddOrder() {
		t.Error("Should not be able to add second order")
	}
}

// TestFSContextCanAddLimit verifies limit permission rules.
func TestFSContextCanAddLimit(t *testing.T) {
	tests := []struct {
		name      string
		setup     func() *FSContext
		limitType LimitType
		want      bool
	}{
		{
			name:      "first on empty",
			setup:     func() *FSContext { return NewFSContext("s", "t", []string{"id"}) },
			limitType: LimitFirst,
			want:      true,
		},
		{
			name:      "last on empty",
			setup:     func() *FSContext { return NewFSContext("s", "t", []string{"id"}) },
			limitType: LimitLast,
			want:      true,
		},
		{
			name:      "sample on empty",
			setup:     func() *FSContext { return NewFSContext("s", "t", []string{"id"}) },
			limitType: LimitSample,
			want:      true,
		},
		{
			name: "double first not allowed",
			setup: func() *FSContext {
				return NewFSContext("s", "t", []string{"id"}).WithLimit(10, LimitFirst)
			},
			limitType: LimitFirst,
			want:      false,
		},
		{
			name: "double last not allowed",
			setup: func() *FSContext {
				return NewFSContext("s", "t", []string{"id"}).WithLimit(10, LimitLast)
			},
			limitType: LimitLast,
			want:      false,
		},
		{
			name: "last after first allowed (nested)",
			setup: func() *FSContext {
				return NewFSContext("s", "t", []string{"id"}).WithLimit(100, LimitFirst)
			},
			limitType: LimitLast,
			want:      true,
		},
		{
			name: "first after last allowed (nested)",
			setup: func() *FSContext {
				return NewFSContext("s", "t", []string{"id"}).WithLimit(100, LimitLast)
			},
			limitType: LimitFirst,
			want:      true,
		},
		{
			name: "nothing after sample",
			setup: func() *FSContext {
				return NewFSContext("s", "t", []string{"id"}).WithLimit(10, LimitSample)
			},
			limitType: LimitFirst,
			want:      false,
		},
		{
			name: "terminal blocks all",
			setup: func() *FSContext {
				return NewFSContext("s", "t", []string{"id"}).WithTerminal()
			},
			limitType: LimitFirst,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setup()
			if got := ctx.CanAddLimit(tt.limitType); got != tt.want {
				t.Errorf("CanAddLimit(%v) = %v, want %v", tt.limitType, got, tt.want)
			}
		})
	}
}

// TestFSContextAvailableCapabilities verifies capability listing.
func TestFSContextAvailableCapabilities(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	caps := ctx.AvailableCapabilities()

	// Should have all capabilities initially
	expected := []string{".by", ".columns", ".filter", ".order", ".first", ".last", ".sample", ".export"}
	for _, exp := range expected {
		found := false
		for _, cap := range caps {
			if cap == exp {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Missing capability %q in %v", exp, caps)
		}
	}

	// After order, no more filter/by/order
	ctx = ctx.WithOrder("id", false)
	caps = ctx.AvailableCapabilities()
	for _, cap := range caps {
		if cap == ".by" || cap == ".filter" || cap == ".order" {
			t.Errorf("Should not have %q after order", cap)
		}
	}

	// Terminal has no capabilities
	ctx = NewFSContext("s", "t", []string{"id"}).WithTerminal()
	if caps := ctx.AvailableCapabilities(); len(caps) != 0 {
		t.Errorf("Terminal should have no capabilities, got %v", caps)
	}
}

// TestFSContextHasFilters verifies filter presence check.
func TestFSContextHasFilters(t *testing.T) {
	ctx := NewFSContext("s", "t", []string{"id"})
	if ctx.HasFilters() {
		t.Error("New context should not have filters")
	}

	ctx = ctx.WithFilter("a", "1", false)
	if !ctx.HasFilters() {
		t.Error("Context with filter should have filters")
	}
}

// TestFSContextHasLimit verifies limit presence check.
func TestFSContextHasLimit(t *testing.T) {
	ctx := NewFSContext("s", "t", []string{"id"})
	if ctx.HasLimit() {
		t.Error("New context should not have limit")
	}

	ctx = ctx.WithLimit(10, LimitFirst)
	if !ctx.HasLimit() {
		t.Error("Context with limit should have limit")
	}
}

// TestFSContextNeedsSubquery verifies subquery detection.
func TestFSContextNeedsSubquery(t *testing.T) {
	ctx := NewFSContext("s", "t", []string{"id"})
	if ctx.NeedsSubquery() {
		t.Error("New context should not need subquery")
	}

	ctx = ctx.WithLimit(100, LimitFirst)
	if ctx.NeedsSubquery() {
		t.Error("Single limit should not need subquery")
	}

	ctx = ctx.WithLimit(10, LimitLast)
	if !ctx.NeedsSubquery() {
		t.Error("Nested limit should need subquery")
	}
}

// TestFSContextWithColumns verifies column projection.
func TestFSContextWithColumns(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx2 := ctx.WithColumns([]string{"id", "name", "email"})

	// Original unchanged
	if len(ctx.Columns) != 0 {
		t.Errorf("Original has columns: %v", ctx.Columns)
	}
	if ctx.HasColumns {
		t.Error("Original should not have HasColumns set")
	}

	// Columns set on new context
	if len(ctx2.Columns) != 3 {
		t.Fatalf("ctx2 has %d columns, want 3", len(ctx2.Columns))
	}
	if ctx2.Columns[0] != "id" || ctx2.Columns[1] != "name" || ctx2.Columns[2] != "email" {
		t.Errorf("Columns = %v, want [id, name, email]", ctx2.Columns)
	}
	if !ctx2.HasColumns {
		t.Error("ctx2 should have HasColumns set")
	}
}

// TestFSContextCanAddColumns verifies column projection permission rules.
func TestFSContextCanAddColumns(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})

	// Can add initially
	if !ctx.CanAddColumns() {
		t.Error("Should be able to add columns initially")
	}

	// Cannot add after columns already set
	ctx2 := ctx.WithColumns([]string{"id"})
	if ctx2.CanAddColumns() {
		t.Error("Should not be able to add columns after .columns/")
	}

	// Cannot add when terminal
	ctx3 := ctx.WithTerminal()
	if ctx3.CanAddColumns() {
		t.Error("Should not be able to add columns when terminal")
	}
}

// TestFSContextColumnsAvailableCapabilities verifies only .export after .columns.
func TestFSContextColumnsAvailableCapabilities(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx = ctx.WithColumns([]string{"id", "name"})

	caps := ctx.AvailableCapabilities()
	if len(caps) != 1 {
		t.Fatalf("After .columns/, expected 1 capability, got %v", caps)
	}
	if caps[0] != ".export" {
		t.Errorf("After .columns/, expected [.export], got %v", caps)
	}
}

// TestFSContextColumnsHasPipelineOperations verifies columns trigger pipeline mode.
func TestFSContextColumnsHasPipelineOperations(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	if ctx.HasPipelineOperations() {
		t.Error("New context should not have pipeline operations")
	}

	ctx2 := ctx.WithColumns([]string{"id"})
	if !ctx2.HasPipelineOperations() {
		t.Error("Context with columns should have pipeline operations")
	}
}

// TestFSContextCloneColumns verifies deep copy of columns slice.
func TestFSContextCloneColumns(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx = ctx.WithColumns([]string{"id", "name"})

	clone := ctx.Clone()

	// Modify clone's columns
	clone.Columns[0] = "email"

	// Original should be unchanged
	if ctx.Columns[0] != "id" {
		t.Errorf("Original columns modified: got %q, want %q", ctx.Columns[0], "id")
	}
}

// TestFSContextToQueryParamsColumns verifies columns are copied to QueryParams.
func TestFSContextToQueryParamsColumns(t *testing.T) {
	ctx := NewFSContext("public", "users", []string{"id"})
	ctx = ctx.WithColumns([]string{"id", "name", "email"})

	params := ctx.ToQueryParams()
	if len(params.Columns) != 3 {
		t.Fatalf("params.Columns has %d entries, want 3", len(params.Columns))
	}
	if params.Columns[0] != "id" || params.Columns[1] != "name" || params.Columns[2] != "email" {
		t.Errorf("params.Columns = %v, want [id, name, email]", params.Columns)
	}

	// Verify independence: modify params, original ctx unchanged
	params.Columns[0] = "changed"
	if ctx.Columns[0] != "id" {
		t.Errorf("ctx.Columns modified via params: got %q, want %q", ctx.Columns[0], "id")
	}
}
