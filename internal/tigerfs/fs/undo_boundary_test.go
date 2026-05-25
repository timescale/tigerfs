package fs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
)

// boundaryFixture builds an Operations with its synth cache pre-seeded so
// checkBoundary reads the supplied metadata entries directly. The cache
// short-circuits any DB calls.
func boundaryFixture(t *testing.T, schema, table string, entries []db.MetadataEntry) *Operations {
	t.Helper()
	ops := newUndoOps(newUndoMock())
	ops.synthState.cache = map[string]map[string]*synth.ViewInfo{
		schema: {
			table: {
				HasHistory: true,
				Metadata:   entries,
			},
		},
	}
	return ops
}

// TestCheckBoundary covers every documented case in the v0.7 plan:
// fresh installs (empty metadata), blocking subjects before/equal/after
// target, non-blocking subjects ignored, and mixed-event scenarios.
func TestCheckBoundary(t *testing.T) {
	const (
		schema = "public"
		table  = "notes"
		// Synthetic UUIDv7-shaped strings. Lexical compare == time order,
		// which is exactly the semantic checkBoundary relies on.
		earlyTarget = "019d0000-0000-7000-8000-000000000001"
		preBoundary = "019d5000-0000-7000-8000-000000000001"
		boundary    = "019e0000-0000-7000-8000-000000000001"
		postEntry   = "019e5000-0000-7000-8000-000000000001"
		laterEntry  = "019f0000-0000-7000-8000-000000000001"
	)
	hint := "Pre-0.7 history is read-only."

	cases := []struct {
		name        string
		entries     []db.MetadataEntry
		targetLogID string
		wantBlocked bool
		wantHint    string // checked only when wantBlocked is true
		wantSubject string // substring expected in Message when blocked
	}{
		{
			name:        "empty metadata (fresh install fast path)",
			entries:     nil,
			targetLogID: earlyTarget,
			wantBlocked: false,
		},
		{
			name: "blocking subject, target before entry",
			entries: []db.MetadataEntry{
				{EntryID: boundary, Subject: synth.SubjectHistoryFormatMigration, Description: hint},
			},
			targetLogID: preBoundary,
			wantBlocked: true,
			wantHint:    hint,
			wantSubject: synth.SubjectHistoryFormatMigration,
		},
		{
			name: "blocking subject, target equal to entry (boundary is not 'before')",
			entries: []db.MetadataEntry{
				{EntryID: boundary, Subject: synth.SubjectHistoryFormatMigration, Description: hint},
			},
			targetLogID: boundary,
			wantBlocked: false,
		},
		{
			name: "blocking subject, target after entry",
			entries: []db.MetadataEntry{
				{EntryID: boundary, Subject: synth.SubjectHistoryFormatMigration, Description: hint},
			},
			targetLogID: postEntry,
			wantBlocked: false,
		},
		{
			name: "non-blocking subject ignored",
			entries: []db.MetadataEntry{
				{EntryID: boundary, Subject: "future-other-subject", Description: "irrelevant"},
			},
			targetLogID: preBoundary,
			wantBlocked: false,
		},
		{
			name: "mixed: non-blocker + blocker, target before blocker",
			entries: []db.MetadataEntry{
				{EntryID: preBoundary, Subject: "future-other-subject", Description: "irrelevant"},
				{EntryID: boundary, Subject: synth.SubjectHistoryFormatMigration, Description: hint},
			},
			targetLogID: earlyTarget,
			wantBlocked: true,
			wantHint:    hint,
			wantSubject: synth.SubjectHistoryFormatMigration,
		},
		{
			name: "mixed: non-blocker + blocker, target after both",
			entries: []db.MetadataEntry{
				{EntryID: preBoundary, Subject: "future-other-subject", Description: "irrelevant"},
				{EntryID: boundary, Subject: synth.SubjectHistoryFormatMigration, Description: hint},
			},
			targetLogID: laterEntry,
			wantBlocked: false,
		},
		{
			name: "multiple blockers, target before all (first match wins)",
			entries: []db.MetadataEntry{
				{EntryID: boundary, Subject: synth.SubjectHistoryFormatMigration, Description: "first"},
				{EntryID: laterEntry, Subject: synth.SubjectHistoryFormatMigration, Description: "second"},
			},
			targetLogID: earlyTarget,
			wantBlocked: true,
			wantHint:    "first",
			wantSubject: synth.SubjectHistoryFormatMigration,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := boundaryFixture(t, schema, table, tc.entries)
			fsErr := ops.checkBoundary(context.Background(), schema, table, tc.targetLogID)

			if !tc.wantBlocked {
				assert.Nil(t, fsErr, "expected no block")
				return
			}
			require.NotNil(t, fsErr, "expected block")
			assert.Equal(t, ErrPermission, fsErr.Code)
			assert.Equal(t, tc.wantHint, fsErr.Hint)
			assert.Contains(t, fsErr.Message, tc.wantSubject)
		})
	}
}

// TestCheckBoundary_NoSynthCache verifies that views without a cached
// ViewInfo (e.g. non-history workspaces) are treated as "no boundary" so
// undo proceeds normally. This is the same fast path as empty metadata.
func TestCheckBoundary_NoSynthCache(t *testing.T) {
	ops := newUndoOps(newUndoMock())
	// synthState.cache left nil
	fsErr := ops.checkBoundary(context.Background(), "public", "notes", "any-log-id")
	assert.Nil(t, fsErr)
}

// TestUndoSingle_BoundaryRefuses verifies the wiring at ExecuteUndoSingle:
// a pre-boundary target returns ErrPermission before any QueryLogEntry
// call is made (i.e. the boundary check runs before db access).
func TestUndoSingle_BoundaryRefuses(t *testing.T) {
	const (
		schema = "public"
		table  = "notes"
		target = "019d5000-0000-7000-8000-000000000001"
		marker = "019e0000-0000-7000-8000-000000000001"
	)
	hint := "Pre-0.7 history is read-only."

	ops := boundaryFixture(t, schema, table, []db.MetadataEntry{
		{EntryID: marker, Subject: synth.SubjectHistoryFormatMigration, Description: hint},
	})

	_, err := ops.ExecuteUndoSingle(context.Background(), schema, table, target)
	require.Error(t, err)

	fsErr, ok := err.(*FSError)
	require.True(t, ok, "expected *FSError, got %T", err)
	assert.Equal(t, ErrPermission, fsErr.Code)
	assert.Equal(t, hint, fsErr.Hint)
}

// TestUndoMulti_BoundaryRefuses verifies the wiring at ExecuteUndo: a
// pre-boundary target refuses before QueryUndoAffectedFiles or any
// transaction begins.
func TestUndoMulti_BoundaryRefuses(t *testing.T) {
	const (
		schema = "public"
		table  = "notes"
		target = "019d5000-0000-7000-8000-000000000001"
		marker = "019e0000-0000-7000-8000-000000000001"
	)
	hint := "Pre-0.7 history is read-only."

	ops := boundaryFixture(t, schema, table, []db.MetadataEntry{
		{EntryID: marker, Subject: synth.SubjectHistoryFormatMigration, Description: hint},
	})

	_, err := ops.ExecuteUndo(context.Background(), schema, table, target, "test", nil)
	require.Error(t, err)

	fsErr, ok := err.(*FSError)
	require.True(t, ok, "expected *FSError, got %T", err)
	assert.Equal(t, ErrPermission, fsErr.Code)
	assert.Equal(t, hint, fsErr.Hint)
}

// TestUndoSingle_PostBoundaryAllowed verifies the happy path: a target
// after the boundary is not refused (boundary check returns nil), so
// ExecuteUndoSingle proceeds to its normal QueryLogEntry lookup.
func TestUndoSingle_PostBoundaryAllowed(t *testing.T) {
	const (
		schema = "public"
		table  = "notes"
		marker = "019e0000-0000-7000-8000-000000000001"
		target = "019e5000-0000-7000-8000-000000000001"
	)

	ops := boundaryFixture(t, schema, table, []db.MetadataEntry{
		{EntryID: marker, Subject: synth.SubjectHistoryFormatMigration, Description: "blocked"},
	})

	// No undoLogEntry seeded → ExecuteUndoSingle fails with "not found"
	// AFTER the boundary check passes. The error is the proof the
	// boundary check fell through to the DB layer.
	_, err := ops.ExecuteUndoSingle(context.Background(), schema, table, target)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "boundary", "boundary should not have blocked")
}
