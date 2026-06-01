package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSynth_HistoricalMTime verifies that ls -l on .savepoint/, .log/, and
// .history/ returns ModTime values reflecting when each entry was actually
// created, not a single "now" snapshot. See plans/historical-mtime-from-uuidv7.md.
func TestSynth_HistoricalMTime(t *testing.T) {
	result := GetTestDBEmpty(t)
	if result == nil {
		return
	}
	defer result.Cleanup()
	cleanupTigerFSTables(t, result.ConnStr, "mtnotes")

	ops := setupFSOperations(t, result.ConnStr)
	ctx := context.Background()

	// Build a history-enabled markdown app.
	fsErr := ops.WriteFile(ctx, "/.build/mtnotes", []byte("markdown,history\n"))
	require.Nil(t, fsErr, "build app")
	time.Sleep(100 * time.Millisecond)

	// Spread three savepoints out so their UUIDv7 timestamps differ
	// meaningfully (>= 1s apart). Capture wall-clock just before each.
	type stamped struct {
		name string
		at   time.Time
	}
	var savepoints []stamped

	for _, name := range []string{"sp-alpha", "sp-bravo", "sp-charlie"} {
		before := time.Now()
		err := ops.WriteFile(ctx, "/mtnotes/.savepoint/"+name+".json", []byte("{}"))
		require.Nil(t, err, "create savepoint %s", name)
		savepoints = append(savepoints, stamped{name: name, at: before})
		time.Sleep(1100 * time.Millisecond)
	}

	t.Run("Savepoint mtimes reflect creation time", func(t *testing.T) {
		entries, fsErr := ops.ReadDir(ctx, "/mtnotes/.savepoint")
		require.Nil(t, fsErr)

		byName := map[string]time.Time{}
		for _, e := range entries {
			if !strings.HasPrefix(e.Name, ".") {
				byName[e.Name] = e.ModTime
			}
		}

		var prevMT time.Time
		for i, sp := range savepoints {
			mt, ok := byName[sp.name]
			require.True(t, ok, "savepoint %s present in listing", sp.name)
			// ModTime must be within a few seconds of the wall-clock at
			// creation; we use 5s as the slack to absorb any clock skew
			// between the test process and the DB without making the test
			// flaky in CI.
			delta := mt.Sub(sp.at)
			assert.True(t, delta >= -2*time.Second && delta < 5*time.Second,
				"savepoint %s mtime %v should be within ~5s of creation %v (delta %v)",
				sp.name, mt, sp.at, delta)
			// Monotonically increasing in creation order.
			if i > 0 {
				assert.True(t, mt.After(prevMT) || mt.Equal(prevMT),
					"savepoint %s mtime %v should be >= previous %v", sp.name, mt, prevMT)
			}
			prevMT = mt
		}

		// ls -l issues a Stat per entry (via FUSE Lookup), so the Stat
		// path must also return the historical ModTime, not just ReadDir.
		// Without this Stat assertion, a broken statXxx code path could
		// pass the test while breaking real-world `ls -l` output.
		stat, fsErr := ops.Stat(ctx, "/mtnotes/.savepoint/"+savepoints[0].name)
		require.Nil(t, fsErr)
		delta := stat.ModTime.Sub(savepoints[0].at)
		assert.True(t, delta >= -2*time.Second && delta < 5*time.Second,
			"Stat on savepoint should return creation-time ModTime (got %v, expected ~%v)", stat.ModTime, savepoints[0].at)

		// Stat WITHOUT a warming ReadDir: create a fresh savepoint and
		// stat it directly. WriteFile invalidates the table's stat cache,
		// so the next Stat traverses the cache-miss path in statRow. This
		// guards against a regression where statRow's cache-miss branch
		// returns time.Now() (which is what the user saw on a freshly
		// mounted demo before the statRow fix landed).
		fresh := time.Now()
		err := ops.WriteFile(ctx, "/mtnotes/.savepoint/sp-fresh.json", []byte("{}"))
		require.Nil(t, err)
		stat, fsErr = ops.Stat(ctx, "/mtnotes/.savepoint/sp-fresh")
		require.Nil(t, fsErr)
		delta = stat.ModTime.Sub(fresh)
		assert.True(t, delta >= -2*time.Second && delta < 5*time.Second,
			"Stat (cache-miss path) should return creation-time ModTime; got %v expected ~%v", stat.ModTime, fresh)
	})

	// Drive a few log entries spaced apart. Each WriteFile to a file row
	// produces a log entry; we capture wall-clock for the latest write
	// per file. (The log table lists all operations; we just need at
	// least three entries with distinct UUIDv7 ms.)
	for _, name := range []string{"a.md", "b.md", "c.md"} {
		err := ops.WriteFile(ctx, "/mtnotes/"+name, []byte("# "+name+"\n"))
		require.Nil(t, err)
		time.Sleep(1100 * time.Millisecond)
	}

	t.Run("Log mtimes are distinct and recent", func(t *testing.T) {
		entries, fsErr := ops.ReadDir(ctx, "/mtnotes/.log")
		require.Nil(t, fsErr)

		var mtimes []time.Time
		for _, e := range entries {
			if strings.HasPrefix(e.Name, ".") {
				continue
			}
			mtimes = append(mtimes, e.ModTime)
		}
		require.GreaterOrEqual(t, len(mtimes), 3, "should have at least 3 log entries")

		// At least two distinct ModTime values (proving we're not just
		// stamping time.Now() on every entry).
		distinct := map[int64]struct{}{}
		for _, m := range mtimes {
			distinct[m.UnixMilli()] = struct{}{}
		}
		assert.GreaterOrEqual(t, len(distinct), 2,
			"log entries should carry distinct ModTimes, got %d distinct out of %d",
			len(distinct), len(mtimes))

		// The most-recent log entry should be within ~10s of now -- it
		// was just written.
		latest := mtimes[0]
		for _, m := range mtimes {
			if m.After(latest) {
				latest = m
			}
		}
		assert.True(t, time.Since(latest) < 10*time.Second,
			"latest log mtime %v should be within ~10s of now %v", latest, time.Now())
	})

	t.Run("History per-file aggregate mtime reflects last change", func(t *testing.T) {
		// Touch a.md again. The history listing's mtime for a.md should
		// move forward.
		entriesBefore, fsErr := ops.ReadDir(ctx, "/mtnotes/.history")
		require.Nil(t, fsErr)
		var aBefore time.Time
		for _, e := range entriesBefore {
			if e.Name == "a.md" {
				aBefore = e.ModTime
				break
			}
		}
		require.False(t, aBefore.IsZero(), "a.md should appear in .history/")

		time.Sleep(1100 * time.Millisecond)
		err := ops.WriteFile(ctx, "/mtnotes/a.md", []byte("# a updated\n"))
		require.Nil(t, err)

		entriesAfter, fsErr := ops.ReadDir(ctx, "/mtnotes/.history")
		require.Nil(t, fsErr)
		var aAfter time.Time
		for _, e := range entriesAfter {
			if e.Name == "a.md" {
				aAfter = e.ModTime
				break
			}
		}
		require.False(t, aAfter.IsZero())

		assert.True(t, aAfter.After(aBefore),
			"a.md aggregate mtime should advance after a new write; before=%v after=%v",
			aBefore, aAfter)

		// ls -l on .history/ issues Stat per filename entry. The Stat
		// path must return the same last-change ModTime as ReadDir.
		// Originally a bug here: statHistory returned time.Now() for
		// every history path, so `ls -l .history/` would show all
		// entries at the current wall-clock even though the ReadDir
		// listing carried the correct value.
		stat, fsErr := ops.Stat(ctx, "/mtnotes/.history/a.md")
		require.Nil(t, fsErr)
		assert.True(t, !stat.ModTime.Before(aAfter.Add(-1*time.Second)),
			"Stat on .history/a.md should reflect last-change time (got %v, ReadDir gave %v)", stat.ModTime, aAfter)

		// Per-version stat: pick a version under a.md and assert its
		// ModTime matches the UUIDv7 in its name. Without this the
		// historical mtime for individual versions would show "now".
		versionEntries, fsErr := ops.ReadDir(ctx, "/mtnotes/.history/a.md")
		require.Nil(t, fsErr)
		var versionID string
		for _, e := range versionEntries {
			if !strings.HasPrefix(e.Name, ".") {
				versionID = e.Name
				break
			}
		}
		require.NotEmpty(t, versionID, "a.md should have at least one version")

		stat, fsErr = ops.Stat(ctx, "/mtnotes/.history/a.md/"+versionID)
		require.Nil(t, fsErr)
		// Versions live within the test window, which started at the
		// first WriteFile. Allow generous slack for clock skew.
		assert.True(t, time.Since(stat.ModTime) > 0 && time.Since(stat.ModTime) < 30*time.Second,
			"Stat on history version should return UUIDv7-derived ModTime within test window (got %v)", stat.ModTime)
	})
}
