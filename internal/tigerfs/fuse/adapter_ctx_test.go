package fuse

import (
	"context"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	tigerfs "github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// Tests for the read-path request-context decoupling described in
// decoupleFromRequestCancel (see adapter.go). The bug being pinned:
// the Linux kernel sends FUSE_INTERRUPT for in-flight FUSE ops when
// Go's runtime sends SIGURG to preempt the originating goroutine,
// go-fuse cancels the per-request ctx, tigerfs's DB call sees
// context.Canceled and tigerfs maps that to ErrIO -> EIO.
//
// The fix decouples the request ctx from the DB ctx for read paths.
// These tests verify that when a caller invokes FSAdapter.{Stat,
// ReadDir, ReadFile} with an already-cancelled ctx, the downstream
// Operations receives a non-cancelled ctx (so the DB query is not
// pre-emptively aborted).

func TestDecoupleFromRequestCancel_StripsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ctx.Err() == nil {
		t.Fatal("parent ctx should be cancelled")
	}

	decoupled := decoupleFromRequestCancel(ctx)
	if decoupled.Err() != nil {
		t.Errorf("decoupled ctx still cancelled: %v", decoupled.Err())
	}

	select {
	case <-decoupled.Done():
		t.Error("decoupled ctx Done() channel fired; should not be cancellable via parent")
	default:
	}
}

func TestDecoupleFromRequestCancel_PreservesValues(t *testing.T) {
	type ctxKey struct{}
	parent := context.WithValue(context.Background(), ctxKey{}, "hello")

	decoupled := decoupleFromRequestCancel(parent)
	if got := decoupled.Value(ctxKey{}); got != "hello" {
		t.Errorf("value lost across decouple: got %v, want %q", got, "hello")
	}
}

// TestFSAdapter_ReadDir_DecouplesRequestCancel verifies the integration:
// when ReadDir is called with a cancelled ctx, the ctx that flows into
// Operations (and therefore into the DB layer) is NOT cancelled.
//
// We hook the earliest DB call Operations.ReadDir makes
// (GetCurrentSchema) to capture the ctx, then assert.
func TestFSAdapter_ReadDir_DecouplesRequestCancel(t *testing.T) {
	capturedCtx := captureCtxViaReadDir(t, "/")
	if capturedCtx == nil {
		t.Fatal("Operations was not reached; FSAdapter short-circuited (regression)")
	}
	if capturedCtx.Err() != nil {
		t.Errorf("FSAdapter forwarded cancelled ctx to DB layer: %v", capturedCtx.Err())
	}
}

func TestFSAdapter_Stat_DecouplesRequestCancel(t *testing.T) {
	capturedCtx := captureCtxViaStat(t, "/public/anything")
	if capturedCtx == nil {
		t.Fatal("Operations was not reached; FSAdapter short-circuited (regression)")
	}
	if capturedCtx.Err() != nil {
		t.Errorf("FSAdapter forwarded cancelled ctx to DB layer: %v", capturedCtx.Err())
	}
}

func TestFSAdapter_ReadFile_DecouplesRequestCancel(t *testing.T) {
	capturedCtx := captureCtxViaReadFile(t, "/public/anything")
	if capturedCtx == nil {
		t.Fatal("Operations was not reached; FSAdapter short-circuited (regression)")
	}
	if capturedCtx.Err() != nil {
		t.Errorf("FSAdapter forwarded cancelled ctx to DB layer: %v", capturedCtx.Err())
	}
}

// captureCtxViaReadDir constructs an FSAdapter wired to a MockDBClient
// that records the ctx given to GetCurrentSchema (the earliest DB call
// Operations.ReadDir makes), invokes ReadDir with an already-cancelled
// ctx, and returns the captured ctx. The return value is nil if the
// hook was never called (which would indicate FSAdapter short-circuited
// before reaching Operations).
func captureCtxViaReadDir(t *testing.T, path string) context.Context {
	t.Helper()
	adapter, captured := newAdapterWithCtxCapture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = adapter.ReadDir(ctx, path)
	return *captured
}

func captureCtxViaStat(t *testing.T, path string) context.Context {
	t.Helper()
	adapter, captured := newAdapterWithCtxCapture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out gofuse.EntryOut
	_ = adapter.Stat(ctx, path, &out)
	return *captured
}

func captureCtxViaReadFile(t *testing.T, path string) context.Context {
	t.Helper()
	adapter, captured := newAdapterWithCtxCapture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = adapter.ReadFile(ctx, path)
	return *captured
}

// newAdapterWithCtxCapture returns an FSAdapter whose underlying mock DB
// records the ctx it receives in GetCurrentSchema (an early-path call
// for all read ops). The returned pointer-to-context is initially nil
// and gets populated by the first DB call made through Operations.
func newAdapterWithCtxCapture() (*FSAdapter, *context.Context) {
	mock := db.NewMockDBClient()
	var captured context.Context
	mock.MockSchemaReader.GetCurrentSchemaFunc = func(ctx context.Context) (string, error) {
		if captured == nil {
			captured = ctx
		}
		// Returning an error is fine -- we only care about the captured
		// ctx, not about ReadDir/Stat/ReadFile succeeding.
		return "", context.Canceled // any error short-circuits cleanly
	}
	cfg := &config.Config{DirListingLimit: 100}
	ops := tigerfs.NewOperations(cfg, mock)
	return NewFSAdapter(ops), &captured
}
