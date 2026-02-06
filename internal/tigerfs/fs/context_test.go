// Package fs tests for context.go (FSContext)
package fs

import (
	"testing"
)

// TestNewFSContext verifies context creation.
func TestNewFSContext(t *testing.T) {
	ctx := NewFSContext("public", "users", "id")

	if ctx.Schema != "public" {
		t.Errorf("Schema = %q, want %q", ctx.Schema, "public")
	}
	if ctx.TableName != "users" {
		t.Errorf("TableName = %q, want %q", ctx.TableName, "users")
	}
	if ctx.PKColumn != "id" {
		t.Errorf("PKColumn = %q, want %q", ctx.PKColumn, "id")
	}
	if ctx.LimitType != LimitNone {
		t.Errorf("LimitType = %v, want %v", ctx.LimitType, LimitNone)
	}
	if len(ctx.Filters) != 0 {
		t.Errorf("Filters = %v, want empty", ctx.Filters)
	}
}

// TestFSContextClone verifies independent copy.
func TestFSContextClone(t *testing.T) {
	ctx := NewFSContext("public", "users", "id")
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
	ctx := NewFSContext("public", "users", "id")
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
	ctx := NewFSContext("public", "users", "id")
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
	ctx := NewFSContext("public", "users", "id")
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
	ctx := NewFSContext("public", "users", "id")
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
	ctx := NewFSContext("public", "users", "id")
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
	ctx := NewFSContext("public", "users", "id")

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
	ctx = NewFSContext("public", "users", "id").WithTerminal()
	if ctx.CanAddFilter() {
		t.Error("Should not be able to add filter when terminal")
	}
}

// TestFSContextCanAddOrder verifies order permission rules.
func TestFSContextCanAddOrder(t *testing.T) {
	ctx := NewFSContext("public", "users", "id")

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
			setup:     func() *FSContext { return NewFSContext("s", "t", "id") },
			limitType: LimitFirst,
			want:      true,
		},
		{
			name:      "last on empty",
			setup:     func() *FSContext { return NewFSContext("s", "t", "id") },
			limitType: LimitLast,
			want:      true,
		},
		{
			name:      "sample on empty",
			setup:     func() *FSContext { return NewFSContext("s", "t", "id") },
			limitType: LimitSample,
			want:      true,
		},
		{
			name: "double first not allowed",
			setup: func() *FSContext {
				return NewFSContext("s", "t", "id").WithLimit(10, LimitFirst)
			},
			limitType: LimitFirst,
			want:      false,
		},
		{
			name: "double last not allowed",
			setup: func() *FSContext {
				return NewFSContext("s", "t", "id").WithLimit(10, LimitLast)
			},
			limitType: LimitLast,
			want:      false,
		},
		{
			name: "last after first allowed (nested)",
			setup: func() *FSContext {
				return NewFSContext("s", "t", "id").WithLimit(100, LimitFirst)
			},
			limitType: LimitLast,
			want:      true,
		},
		{
			name: "first after last allowed (nested)",
			setup: func() *FSContext {
				return NewFSContext("s", "t", "id").WithLimit(100, LimitLast)
			},
			limitType: LimitFirst,
			want:      true,
		},
		{
			name: "nothing after sample",
			setup: func() *FSContext {
				return NewFSContext("s", "t", "id").WithLimit(10, LimitSample)
			},
			limitType: LimitFirst,
			want:      false,
		},
		{
			name: "terminal blocks all",
			setup: func() *FSContext {
				return NewFSContext("s", "t", "id").WithTerminal()
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
	ctx := NewFSContext("public", "users", "id")
	caps := ctx.AvailableCapabilities()

	// Should have all capabilities initially
	expected := []string{".by", ".filter", ".order", ".first", ".last", ".sample", ".export"}
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
	ctx = NewFSContext("s", "t", "id").WithTerminal()
	if caps := ctx.AvailableCapabilities(); len(caps) != 0 {
		t.Errorf("Terminal should have no capabilities, got %v", caps)
	}
}

// TestFSContextHasFilters verifies filter presence check.
func TestFSContextHasFilters(t *testing.T) {
	ctx := NewFSContext("s", "t", "id")
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
	ctx := NewFSContext("s", "t", "id")
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
	ctx := NewFSContext("s", "t", "id")
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
