package fs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// --- autoSavepointName tests ---

func TestSynth_AutoSavepoint_NameWithUserID(t *testing.T) {
	ops := &Operations{userID: "agent-7"}
	ts := time.Date(2026, 4, 8, 14, 30, 0, 0, time.UTC)
	assert.Equal(t, "auto-agent-7-20260408T143000Z", ops.autoSavepointName(ts))
}

func TestSynth_AutoSavepoint_NameAnonymous(t *testing.T) {
	ops := &Operations{}
	ts := time.Date(2026, 4, 8, 14, 30, 0, 0, time.UTC)
	assert.Equal(t, "auto-20260408T143000Z", ops.autoSavepointName(ts))
}

// --- maybeCreateAutoSavepoint tests ---

func TestSynth_AutoSavepoint_CreatedOnGap(t *testing.T) {
	mockDB := &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.notes_savepoint": {column: "name"},
		},
		lastInsertReturnPK: "auto-test",
	}
	cfg := &config.Config{
		DirListingLimit:       1000,
		AutoSavepointInterval: 5 * time.Minute,
	}
	ops := NewOperations(cfg, mockDB)
	ops.SetUserID("agent-7")

	baseTime := time.Date(2026, 4, 8, 14, 0, 0, 0, time.UTC)
	currentTime := baseTime
	ops.nowFunc = func() time.Time { return currentTime }

	// Seed the last write time (simulate a previous write)
	ops.lastWriteTime = map[string]time.Time{
		"public.notes": baseTime,
	}

	// Advance time beyond the interval
	currentTime = baseTime.Add(10 * time.Minute)

	ops.maybeCreateAutoSavepoint(context.Background(), "public", "notes")

	// Should have inserted an auto-savepoint
	require.True(t, mockDB.insertCalled, "should create auto-savepoint")
	require.Len(t, mockDB.insertedRows, 1)

	row := mockDB.insertedRows[0]
	colMap := make(map[string]interface{})
	for i, col := range row.columns {
		colMap[col] = row.values[i]
	}
	assert.Equal(t, "auto-agent-7-20260408T141000Z", colMap["name"])
	assert.Equal(t, "agent-7", colMap["user_id"])
	assert.Contains(t, colMap["description"], "inactivity")
}

func TestSynth_AutoSavepoint_SkippedWithinInterval(t *testing.T) {
	mockDB := &mockDBClient{}
	cfg := &config.Config{
		DirListingLimit:       1000,
		AutoSavepointInterval: 30 * time.Minute,
	}
	ops := NewOperations(cfg, mockDB)

	baseTime := time.Date(2026, 4, 8, 14, 0, 0, 0, time.UTC)
	currentTime := baseTime.Add(5 * time.Minute) // Only 5 min gap
	ops.nowFunc = func() time.Time { return currentTime }

	ops.lastWriteTime = map[string]time.Time{
		"public.notes": baseTime,
	}

	ops.maybeCreateAutoSavepoint(context.Background(), "public", "notes")

	assert.False(t, mockDB.insertCalled, "should NOT create auto-savepoint within interval")
}

func TestSynth_AutoSavepoint_DisabledWhenIntervalZero(t *testing.T) {
	mockDB := &mockDBClient{}
	cfg := &config.Config{
		DirListingLimit:       1000,
		AutoSavepointInterval: 0, // disabled
	}
	ops := NewOperations(cfg, mockDB)

	ops.lastWriteTime = map[string]time.Time{
		"public.notes": time.Now().Add(-1 * time.Hour),
	}

	ops.maybeCreateAutoSavepoint(context.Background(), "public", "notes")

	assert.False(t, mockDB.insertCalled, "should NOT create auto-savepoint when interval=0")
}

func TestSynth_AutoSavepoint_SkippedOnFirstWrite(t *testing.T) {
	mockDB := &mockDBClient{}
	cfg := &config.Config{
		DirListingLimit:       1000,
		AutoSavepointInterval: 5 * time.Minute,
	}
	ops := NewOperations(cfg, mockDB)

	// No lastWriteTime entry -- first write after mount
	ops.maybeCreateAutoSavepoint(context.Background(), "public", "notes")

	assert.False(t, mockDB.insertCalled, "should NOT create auto-savepoint on first write")
}

func TestSynth_AutoSavepoint_PerTableTracking(t *testing.T) {
	mockDB := &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.notes_savepoint": {column: "name"},
			"tigerfs.docs_savepoint":  {column: "name"},
		},
		lastInsertReturnPK: "auto-test",
	}
	cfg := &config.Config{
		DirListingLimit:       1000,
		AutoSavepointInterval: 5 * time.Minute,
	}
	ops := NewOperations(cfg, mockDB)

	baseTime := time.Date(2026, 4, 8, 14, 0, 0, 0, time.UTC)
	currentTime := baseTime.Add(10 * time.Minute)
	ops.nowFunc = func() time.Time { return currentTime }

	// notes had a recent write, docs had an old write
	ops.lastWriteTime = map[string]time.Time{
		"public.notes": baseTime.Add(8 * time.Minute), // 2 min ago -- within interval
		"public.docs":  baseTime,                      // 10 min ago -- exceeds interval
	}

	ops.maybeCreateAutoSavepoint(context.Background(), "public", "notes")
	assert.False(t, mockDB.insertCalled, "notes should NOT trigger (within interval)")

	ops.maybeCreateAutoSavepoint(context.Background(), "public", "docs")
	assert.True(t, mockDB.insertCalled, "docs should trigger (exceeds interval)")
}
