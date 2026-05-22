package db

import (
	"context"
	"fmt"
	"testing"
)

func TestWithSessionVars_ZeroValueReturnsOriginal(t *testing.T) {
	ctx := context.Background()
	got := WithSessionVars(ctx, SessionVars{})
	if got != ctx {
		t.Error("expected same context for zero-value vars")
	}
}

func TestWithSessionVars_EmptyMapReturnsOriginal(t *testing.T) {
	ctx := context.Background()
	got := WithSessionVars(ctx, NewSessionVars(nil))
	if got != ctx {
		t.Error("expected same context for nil map")
	}
}

func TestSessionVarsFromContext_MissingReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	vars := SessionVarsFromContext(ctx)
	if !vars.Empty() {
		t.Errorf("expected empty, got %d vars", vars.Len())
	}
}

func TestSessionVarsFromContext_RoundTrip(t *testing.T) {
	ctx := context.Background()
	vars := NewSessionVars(map[string]string{"app.user_id": "42", "app.tenant_id": "acme"})
	ctx = WithSessionVars(ctx, vars)

	got := SessionVarsFromContext(ctx)
	if got.Empty() {
		t.Fatal("expected non-empty vars")
	}
	assertVar(t, got, "app.user_id", "42")
	assertVar(t, got, "app.tenant_id", "acme")
}

func TestSessionVarsFromContext_OverwritesPrevious(t *testing.T) {
	ctx := context.Background()
	ctx = WithSessionVars(ctx, NewSessionVars(map[string]string{"app.user_id": "1"}))
	ctx = WithSessionVars(ctx, NewSessionVars(map[string]string{"app.user_id": "2"}))

	got := SessionVarsFromContext(ctx)
	assertVar(t, got, "app.user_id", "2")
}

func TestNewSessionVars_SortsKeys(t *testing.T) {
	vars := NewSessionVars(map[string]string{
		"z.last": "1", "a.first": "2", "m.middle": "3",
	})
	var keys []string
	vars.Range(func(k, v string) { keys = append(keys, k) })
	if keys[0] != "a.first" || keys[1] != "m.middle" || keys[2] != "z.last" {
		t.Errorf("keys not sorted: %v", keys)
	}
}

func TestNewSessionVars_CopiesMap(t *testing.T) {
	m := map[string]string{"app.user_id": "1"}
	vars := NewSessionVars(m)
	m["app.user_id"] = "mutated"
	assertVar(t, vars, "app.user_id", "1") // should not see mutation
}

func TestSessionVars_Merge(t *testing.T) {
	base := NewSessionVars(map[string]string{"app.tenant_id": "acme", "app.user_id": "1"})
	override := NewSessionVars(map[string]string{"app.user_id": "2"})
	merged := base.Merge(override)

	if merged.Len() != 2 {
		t.Errorf("expected 2 vars, got %d", merged.Len())
	}
	assertVar(t, merged, "app.tenant_id", "acme")
	assertVar(t, merged, "app.user_id", "2")
}

func TestSessionVars_MergeEmptyBase(t *testing.T) {
	override := NewSessionVars(map[string]string{"app.user_id": "1"})
	merged := SessionVars{}.Merge(override)
	assertVar(t, merged, "app.user_id", "1")
}

func TestSessionVars_MergeEmptyOverride(t *testing.T) {
	base := NewSessionVars(map[string]string{"app.user_id": "1"})
	merged := base.Merge(SessionVars{})
	assertVar(t, merged, "app.user_id", "1")
}

func TestEffectiveSessionVars_NoVars(t *testing.T) {
	c := &Client{}
	ctx := context.Background()
	if vars := c.effectiveSessionVars(ctx); !vars.Empty() {
		t.Errorf("expected empty, got %d vars", vars.Len())
	}
}

func TestEffectiveSessionVars_BaselineOnly(t *testing.T) {
	c := &Client{baselineVars: NewSessionVars(map[string]string{"app.tenant_id": "acme"})}
	ctx := context.Background()
	vars := c.effectiveSessionVars(ctx)
	assertVar(t, vars, "app.tenant_id", "acme")
}

func TestEffectiveSessionVars_ContextOnly(t *testing.T) {
	c := &Client{}
	ctx := WithSessionVars(context.Background(), NewSessionVars(map[string]string{"app.user_id": "42"}))
	vars := c.effectiveSessionVars(ctx)
	assertVar(t, vars, "app.user_id", "42")
}

func TestEffectiveSessionVars_ContextOverridesBaseline(t *testing.T) {
	c := &Client{baselineVars: NewSessionVars(map[string]string{
		"app.user_id":   "1",
		"app.tenant_id": "acme",
	})}
	ctx := WithSessionVars(context.Background(), NewSessionVars(map[string]string{"app.user_id": "2"}))
	vars := c.effectiveSessionVars(ctx)

	assertVar(t, vars, "app.user_id", "2")
	assertVar(t, vars, "app.tenant_id", "acme")
}

func TestEffectiveSessionVars_Merge(t *testing.T) {
	c := &Client{baselineVars: NewSessionVars(map[string]string{"app.tenant_id": "acme"})}
	ctx := WithSessionVars(context.Background(), NewSessionVars(map[string]string{"app.user_id": "42"}))
	vars := c.effectiveSessionVars(ctx)

	if vars.Len() != 2 {
		t.Errorf("expected 2 vars, got %d", vars.Len())
	}
	assertVar(t, vars, "app.tenant_id", "acme")
	assertVar(t, vars, "app.user_id", "42")
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
	noop := doneFunc(func(error) {})
	noop(nil)
	noop(fmt.Errorf("test error"))
}

// assertVar checks that a SessionVars contains the expected key-value pair.
func assertVar(t *testing.T, vars SessionVars, key, want string) {
	t.Helper()
	var found bool
	vars.Range(func(k, v string) {
		if k == key {
			found = true
			if v != want {
				t.Errorf("%s = %q, want %q", key, v, want)
			}
		}
	})
	if !found {
		t.Errorf("key %q not found in session vars", key)
	}
}
