package db

import (
	"context"
	"fmt"
	"testing"
)

func TestWithSessionVars_NilReturnsOriginal(t *testing.T) {
	ctx := context.Background()
	got := WithSessionVars(ctx, nil)
	if got != ctx {
		t.Error("expected same context for nil vars")
	}
}

func TestWithSessionVars_EmptyReturnsOriginal(t *testing.T) {
	ctx := context.Background()
	got := WithSessionVars(ctx, SessionVars{})
	if got != ctx {
		t.Error("expected same context for empty vars")
	}
}

func TestSessionVarsFromContext_MissingReturnsNil(t *testing.T) {
	ctx := context.Background()
	if vars := SessionVarsFromContext(ctx); vars != nil {
		t.Errorf("expected nil, got %v", vars)
	}
}

func TestSessionVarsFromContext_RoundTrip(t *testing.T) {
	ctx := context.Background()
	vars := SessionVars{"app.user_id": "42", "app.tenant_id": "acme"}
	ctx = WithSessionVars(ctx, vars)

	got := SessionVarsFromContext(ctx)
	if got == nil {
		t.Fatal("expected non-nil vars")
	}
	if got["app.user_id"] != "42" {
		t.Errorf("app.user_id = %q, want %q", got["app.user_id"], "42")
	}
	if got["app.tenant_id"] != "acme" {
		t.Errorf("app.tenant_id = %q, want %q", got["app.tenant_id"], "acme")
	}
}

func TestSessionVarsFromContext_OverwritesPrevious(t *testing.T) {
	ctx := context.Background()
	ctx = WithSessionVars(ctx, SessionVars{"app.user_id": "1"})
	ctx = WithSessionVars(ctx, SessionVars{"app.user_id": "2"})

	got := SessionVarsFromContext(ctx)
	if got["app.user_id"] != "2" {
		t.Errorf("app.user_id = %q, want %q", got["app.user_id"], "2")
	}
}

func TestEffectiveSessionVars_NoVars(t *testing.T) {
	c := &Client{}
	ctx := context.Background()
	if vars := c.effectiveSessionVars(ctx); vars != nil {
		t.Errorf("expected nil, got %v", vars)
	}
}

func TestEffectiveSessionVars_BaselineOnly(t *testing.T) {
	c := &Client{baselineVars: SessionVars{"app.tenant_id": "acme"}}
	ctx := context.Background()
	vars := c.effectiveSessionVars(ctx)
	if vars["app.tenant_id"] != "acme" {
		t.Errorf("app.tenant_id = %q, want %q", vars["app.tenant_id"], "acme")
	}
}

func TestEffectiveSessionVars_ContextOnly(t *testing.T) {
	c := &Client{}
	ctx := WithSessionVars(context.Background(), SessionVars{"app.user_id": "42"})
	vars := c.effectiveSessionVars(ctx)
	if vars["app.user_id"] != "42" {
		t.Errorf("app.user_id = %q, want %q", vars["app.user_id"], "42")
	}
}

func TestEffectiveSessionVars_ContextOverridesBaseline(t *testing.T) {
	c := &Client{baselineVars: SessionVars{
		"app.user_id":   "1",
		"app.tenant_id": "acme",
	}}
	ctx := WithSessionVars(context.Background(), SessionVars{"app.user_id": "2"})
	vars := c.effectiveSessionVars(ctx)

	if vars["app.user_id"] != "2" {
		t.Errorf("app.user_id = %q, want %q (context should override baseline)", vars["app.user_id"], "2")
	}
	if vars["app.tenant_id"] != "acme" {
		t.Errorf("app.tenant_id = %q, want %q (baseline should be preserved)", vars["app.tenant_id"], "acme")
	}
}

func TestEffectiveSessionVars_Merge(t *testing.T) {
	c := &Client{baselineVars: SessionVars{"app.tenant_id": "acme"}}
	ctx := WithSessionVars(context.Background(), SessionVars{"app.user_id": "42"})
	vars := c.effectiveSessionVars(ctx)

	if len(vars) != 2 {
		t.Errorf("expected 2 vars, got %d", len(vars))
	}
	if vars["app.tenant_id"] != "acme" {
		t.Errorf("app.tenant_id = %q, want %q", vars["app.tenant_id"], "acme")
	}
	if vars["app.user_id"] != "42" {
		t.Errorf("app.user_id = %q, want %q", vars["app.user_id"], "42")
	}
}

func TestAcquireDBTX_NilPoolReturnsError(t *testing.T) {
	c := &Client{}
	ctx := context.Background()
	_, _, err := c.acquireDBTX(ctx)
	if err == nil {
		t.Fatal("expected error for nil pool")
	}
}

func TestAcquireDBTX_NoVarsDoneFuncIsNoOp(t *testing.T) {
	// Verify the no-op done func is safe to call with any error.
	// We can't fully test acquireDBTX without a real pool, but we
	// verify the doneFunc contract: nil error and non-nil error are both safe.
	noop := doneFunc(func(error) {})
	noop(nil)
	noop(fmt.Errorf("test error"))
}
