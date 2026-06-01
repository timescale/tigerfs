package fs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
)

// readDirHistoryDispatch resolves the synth view info and delegates to readDirHistory.
func (o *Operations) readDirHistoryDispatch(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{Code: ErrInvalidPath, Message: "missing context for .history/"}
	}
	info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName)
	if info == nil || !info.HasHistory {
		return nil, &FSError{Code: ErrNotExist, Message: ".history/ not available (no versioned history)"}
	}
	return o.readDirHistory(ctx, parsed, info)
}

// statHistoryDispatch resolves the synth view info and delegates to statHistory.
func (o *Operations) statHistoryDispatch(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{Code: ErrInvalidPath, Message: "missing context for .history/"}
	}
	info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName)
	if info == nil || !info.HasHistory {
		return nil, &FSError{Code: ErrNotExist, Message: ".history/ not available (no versioned history)"}
	}
	return o.statHistory(ctx, parsed, info)
}

// readHistoryFileDispatch resolves the synth view info and delegates to readHistoryFile.
func (o *Operations) readHistoryFileDispatch(ctx context.Context, parsed *ParsedPath) (*FileContent, *FSError) {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return nil, &FSError{Code: ErrInvalidPath, Message: "missing context for .history/"}
	}
	info := o.getSynthViewInfo(ctx, fsCtx.Schema, fsCtx.TableName)
	if info == nil || !info.HasHistory {
		return nil, &FSError{Code: ErrNotExist, Message: ".history/ not available (no versioned history)"}
	}
	data, fsErr := o.readHistoryFile(ctx, parsed, info)
	if fsErr != nil {
		return nil, fsErr
	}
	return &FileContent{Data: data}, nil
}

// readDirHistory lists entries in the .history/ virtual directory.
// Branches on HistoryByID to support by-filename and by-UUID navigation.
func (o *Operations) readDirHistory(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]Entry, *FSError) {
	fsCtx := parsed.Context
	schema := synth.TigerFSSchema
	// cacheSchema is the user-facing schema where the live view lives.
	// resolveSynthPath uses (cacheSchema, table) as the pathCache key, and
	// every other code path keys cache writes/lookups under the user's
	// schema, so the history reads must too -- otherwise history's cache
	// entries land in a disjoint (tigerfs, table) namespace and get neither
	// shared with normal reads nor cleared by normal invalidate calls.
	cacheSchema := fsCtx.Schema
	historyTable := fsCtx.TableName + "_history"
	now := time.Now()

	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	if parsed.HistoryByID {
		return o.readDirHistoryByID(ctx, schema, historyTable, parsed, info, now, limit)
	}
	return o.readDirHistoryByFilename(ctx, schema, cacheSchema, historyTable, parsed, info, now, limit)
}

// readDirHistoryByFilename lists history entries organized by filename.
// Uses parsed.PrimaryKey as a directory prefix to show only files at the current level.
//
// schema is the synth schema (tigerfs) where the history table lives;
// cacheSchema is the user's schema, used for pathCache lookups so cache
// keys match live-table reads.
func (o *Operations) readDirHistoryByFilename(ctx context.Context, schema, cacheSchema, historyTable string, parsed *ParsedPath, info *synth.ViewInfo, now time.Time, limit int) ([]Entry, *FSError) {
	if parsed.HistoryFile == "" {
		// /{table}/.history/ or /{table}/subdir/.history/ — list filenames at this level
		dirPrefix := parsed.PrimaryKey

		var filenames []string
		// lastChanges parallels filenames; each element is the MAX(version_id)
		// (UUIDv7) for that filename. Used to populate per-file ModTime so
		// "ls -l .history/" reflects when each file last changed. Stays nil
		// in the legacy non-parent-pointer code path -- ModTime falls back
		// to `now` for those entries.
		var lastChanges []string
		var err error

		// Parent-pointer model (ADR-017): query history by parent_id
		if info.Roles.ParentID != "" {
			var parentID string
			if dirPrefix != "" {
				// Resolve directory path in the LIVE table to get its UUID
				segments := strings.Split(dirPrefix, "/")
				var ok bool
				var resolveErr error
				parentID, ok, resolveErr = o.resolveSynthPath(ctx, cacheSchema, parsed.Context.TableName, segments)
				if resolveErr != nil {
					return nil, &FSError{Code: ErrIO, Message: fmt.Sprintf("failed to resolve directory: %s", dirPrefix), Cause: resolveErr}
				}
				if !ok {
					return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("directory not found: %s", dirPrefix)}
				}
			}
			filenames, lastChanges, err = o.db.QueryHistoryFilenamesByParentWithLastChange(ctx, schema, historyTable, parentID, limit)
		} else {
			// Old model: get all filenames and filter by prefix
			filenames, err = o.db.QueryHistoryDistinctFilenames(ctx, schema, historyTable, limit)
		}
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to list history filenames", Cause: err}
		}

		var filtered []Entry
		if info.Roles.ParentID != "" {
			// Parent-pointer: filenames are already scoped to the directory.
			// ModTime is the file's most-recent history version, decoded from
			// the UUIDv7 last_change column.
			for i, fn := range filenames {
				mt := now
				if i < len(lastChanges) {
					if t := uuidv7ModTime(lastChanges[i]); !t.IsZero() {
						mt = t
					}
				}
				filtered = append(filtered, Entry{
					Name: fn, IsDir: true,
					Mode: os.ModeDir | 0555, ModTime: mt,
				})
			}
		} else {
			filtered = filterHistoryAtLevel(filenames, dirPrefix)
		}

		entries := make([]Entry, 0, len(filtered)+1)
		// Add .by/ entry only at root level (dirPrefix == "")
		if dirPrefix == "" {
			entries = append(entries, Entry{Name: ".by", IsDir: true, Mode: os.ModeDir | 0555, ModTime: now})
		}
		entries = append(entries, filtered...)
		return entries, nil
	}

	// /{table}/.history/foo.md/ — list versions for filename + .id file
	rawFilename := historyDBFilename(info, parsed.PrimaryKey, parsed.HistoryFile)

	columns, rows, err := o.db.QueryHistoryByFilename(ctx, schema, historyTable, rawFilename, limit)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query history versions", Cause: err}
	}

	entries := make([]Entry, 0, len(rows)+1)
	// Add .id virtual file
	entries = append(entries, Entry{Name: FileID, IsDir: false, Mode: 0444, Size: 36, ModTime: now})

	historyIDIdx := columnIndex(columns, synth.ColVersionID)
	for _, row := range rows {
		if historyIDIdx < 0 {
			continue
		}
		versionID := historyIDToVersionID(row[historyIDIdx])
		if versionID == "" {
			continue
		}
		mt := historyIDToModTime(row[historyIDIdx])
		if mt.IsZero() {
			mt = now
		}
		entries = append(entries, Entry{
			Name:    versionID,
			IsDir:   false,
			Mode:    0444,
			Size:    0, // Size unknown without synthesizing content
			ModTime: mt,
		})
	}
	return entries, nil
}

// readDirHistoryByID lists history entries organized by row UUID.
func (o *Operations) readDirHistoryByID(ctx context.Context, schema, historyTable string, parsed *ParsedPath, info *synth.ViewInfo, now time.Time, limit int) ([]Entry, *FSError) {
	if parsed.HistoryRowID == "" {
		// /{table}/.history/.by/ — list distinct row UUIDs
		ids, err := o.db.QueryHistoryDistinctIDs(ctx, schema, historyTable, limit)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to list history row IDs", Cause: err}
		}

		entries := make([]Entry, 0, len(ids))
		for _, id := range ids {
			entries = append(entries, Entry{
				Name:    id,
				IsDir:   true,
				Mode:    os.ModeDir | 0555,
				ModTime: now,
			})
		}
		return entries, nil
	}

	// /{table}/.history/.by/<uuid>/ — list versions for this row UUID
	columns, rows, err := o.db.QueryHistoryByID(ctx, schema, historyTable, parsed.HistoryRowID, limit)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query history by ID", Cause: err}
	}

	entries := make([]Entry, 0, len(rows))
	historyIDIdx := columnIndex(columns, synth.ColVersionID)
	for _, row := range rows {
		if historyIDIdx < 0 {
			continue
		}
		versionID := historyIDToVersionID(row[historyIDIdx])
		if versionID == "" {
			continue
		}
		mt := historyIDToModTime(row[historyIDIdx])
		if mt.IsZero() {
			mt = now
		}
		entries = append(entries, Entry{
			Name:    versionID,
			IsDir:   false,
			Mode:    0444,
			Size:    0,
			ModTime: mt,
		})
	}
	return entries, nil
}

// statHistory returns metadata for .history/ paths.
func (o *Operations) statHistory(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) (*Entry, *FSError) {
	now := time.Now()
	schema := synth.TigerFSSchema
	historyTable := parsed.Context.TableName + "_history"

	// .history/ directory itself
	if parsed.HistoryFile == "" && !parsed.HistoryByID {
		return &Entry{Name: ".history", IsDir: true, Mode: os.ModeDir | 0555, ModTime: now}, nil
	}

	// .history/.by/ directory
	if parsed.HistoryByID && parsed.HistoryRowID == "" {
		return &Entry{Name: ".by", IsDir: true, Mode: os.ModeDir | 0555, ModTime: now}, nil
	}

	// .history/.by/<uuid>/ — check UUID has history. ModTime is the
	// most-recent version_id for that file_id ("when did this row last
	// change?"). QueryHistoryByID orders DESC, so rows[0] is the latest.
	if parsed.HistoryByID && parsed.HistoryRowID != "" && parsed.HistoryVersionID == "" {
		columns, rows, err := o.db.QueryHistoryByID(ctx, schema, historyTable, parsed.HistoryRowID, 1)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to check history by ID", Cause: err}
		}
		if len(rows) == 0 {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("no history for row ID %s", parsed.HistoryRowID)}
		}
		mt := now
		if idx := columnIndex(columns, synth.ColVersionID); idx >= 0 {
			if t := historyIDToModTime(rows[0][idx]); !t.IsZero() {
				mt = t
			}
		}
		return &Entry{Name: parsed.HistoryRowID, IsDir: true, Mode: os.ModeDir | 0555, ModTime: mt}, nil
	}

	// .history/.by/<uuid>/<versionID> — version file by UUID. ModTime
	// comes from the version_id (UUIDv7) -- the version itself is an
	// immutable snapshot, so create time == mtime.
	if parsed.HistoryByID && parsed.HistoryVersionID != "" {
		return o.statHistoryVersion(ctx, schema, historyTable, "file_id", parsed.HistoryRowID, parsed.HistoryVersionID, info, now)
	}

	// .history/foo.md/ — filename directory. ModTime is the most-recent
	// version_id for that filename ("when did this file last change?").
	if parsed.HistoryFile != "" && parsed.HistoryVersionID == "" {
		rawFilename := historyDBFilename(info, parsed.PrimaryKey, parsed.HistoryFile)
		columns, rows, err := o.db.QueryHistoryByFilename(ctx, schema, historyTable, rawFilename, 1)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to check history by filename", Cause: err}
		}
		if len(rows) == 0 {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("no history for %s", parsed.HistoryFile)}
		}
		mt := now
		if idx := columnIndex(columns, synth.ColVersionID); idx >= 0 {
			if t := historyIDToModTime(rows[0][idx]); !t.IsZero() {
				mt = t
			}
		}
		return &Entry{Name: parsed.HistoryFile, IsDir: true, Mode: os.ModeDir | 0555, ModTime: mt}, nil
	}

	// .history/foo.md/.id — virtual file returning the row UUID
	if parsed.HistoryFile != "" && parsed.HistoryVersionID == FileID {
		return &Entry{Name: FileID, IsDir: false, Mode: 0444, Size: 36, ModTime: now}, nil
	}

	// .history/foo.md/<versionID> — version file by filename
	if parsed.HistoryFile != "" && parsed.HistoryVersionID != "" {
		rawFilename := historyDBFilename(info, parsed.PrimaryKey, parsed.HistoryFile)
		return o.statHistoryVersion(ctx, schema, historyTable, "filename", rawFilename, parsed.HistoryVersionID, info, now)
	}

	return nil, &FSError{Code: ErrNotExist, Message: "invalid history path"}
}

// statHistoryVersion returns metadata for a specific version file in .history/.
//
// ModTime is the UUIDv7-embedded creation time of the version: history rows
// are immutable snapshots, so the row's create time IS its mtime. Falling
// back to now only if the versionID can't be parsed.
func (o *Operations) statHistoryVersion(ctx context.Context, schema, historyTable, filterColumn, filterValue, versionID string, info *synth.ViewInfo, now time.Time) (*Entry, *FSError) {
	columns, rows, err := o.db.QueryHistoryVersionByTime(ctx, schema, historyTable, filterColumn, filterValue, versionID, 1000)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query history version", Cause: err}
	}

	row := findVersionRow(columns, rows, versionID)
	if row == nil {
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("version %s not found", versionID)}
	}

	// Synthesize content to get size
	content, synthErr := o.synthesizeContent(columns, row, info)
	var size int64
	if synthErr == nil {
		size = int64(len(content))
	}

	mt := uuidv7ModTime(versionID)
	if mt.IsZero() {
		mt = now
	}

	return &Entry{Name: versionID, IsDir: false, Mode: 0444, Size: size, ModTime: mt}, nil
}

// readHistoryFile reads a file within .history/.
func (o *Operations) readHistoryFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]byte, *FSError) {
	schema := synth.TigerFSSchema
	historyTable := parsed.Context.TableName + "_history"

	// .id file: return the row UUID for this filename
	if parsed.HistoryFile != "" && parsed.HistoryVersionID == FileID {
		rawFilename := historyDBFilename(info, parsed.PrimaryKey, parsed.HistoryFile)
		columns, rows, err := o.db.QueryHistoryByFilename(ctx, schema, historyTable, rawFilename, 1)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to query history for .id", Cause: err}
		}
		if len(rows) == 0 {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("no history for %s", parsed.HistoryFile)}
		}
		idIdx := columnIndex(columns, synth.ColFileID)
		if idIdx < 0 {
			return nil, &FSError{Code: ErrIO, Message: "file_id column not found in history table"}
		}
		idStr := synth.ValueToString(rows[0][idIdx])
		return []byte(idStr + "\n"), nil
	}

	// Version file: read and synthesize content
	var filterColumn, filterValue string
	if parsed.HistoryByID {
		filterColumn = "file_id"
		filterValue = parsed.HistoryRowID
	} else {
		filterColumn = "filename"
		filterValue = historyDBFilename(info, parsed.PrimaryKey, parsed.HistoryFile)
	}

	columns, rows, err := o.db.QueryHistoryVersionByTime(ctx, schema, historyTable, filterColumn, filterValue, parsed.HistoryVersionID, 100)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query history version", Cause: err}
	}

	row := findVersionRow(columns, rows, parsed.HistoryVersionID)
	if row == nil {
		return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("version %s not found", parsed.HistoryVersionID)}
	}

	content, synthErr := o.synthesizeContent(columns, row, info)
	if synthErr != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to synthesize history content", Cause: synthErr}
	}

	return content, nil
}

// findVersionRow scans rows for one whose version_id matches the given versionID.
func findVersionRow(columns []string, rows [][]interface{}, versionID string) []interface{} {
	historyIDIdx := columnIndex(columns, synth.ColVersionID)
	if historyIDIdx < 0 {
		return nil
	}
	for _, row := range rows {
		vid := historyIDToVersionID(row[historyIDIdx])
		if vid == versionID {
			return row
		}
	}
	return nil
}

// columnIndex returns the index of a column name in a column list, or -1.
func columnIndex(columns []string, name string) int {
	for i, c := range columns {
		if c == name {
			return i
		}
	}
	return -1
}

// historyIDToModTime returns the creation time embedded in a version_id
// (UUIDv7) value, accepting the same heterogeneous inputs as
// historyIDToVersionID. Returns the zero time on a parse failure so callers
// can substitute a fallback.
func historyIDToModTime(val interface{}) time.Time {
	versionID := historyIDToVersionID(val)
	if versionID == "" {
		return time.Time{}
	}
	return uuidv7ModTime(versionID)
}

// historyIDToVersionID converts a version_id value (UUIDv7) to a display version ID string.
func historyIDToVersionID(val interface{}) string {
	// The value may be a [16]byte, uuid.UUID, or string
	switch v := val.(type) {
	case [16]byte:
		id := v
		return synth.UUIDv7ToVersionID(id)
	case string:
		// Parse string UUID
		parsed, err := parseUUID(v)
		if err != nil {
			return ""
		}
		return synth.UUIDv7ToVersionID(parsed)
	default:
		// Try fmt.Sprint and parse
		s := fmt.Sprintf("%v", val)
		parsed, err := parseUUID(s)
		if err != nil {
			return ""
		}
		return synth.UUIDv7ToVersionID(parsed)
	}
}

// parseUUID parses a UUID string into a [16]byte array.
func parseUUID(s string) ([16]byte, error) {
	var id [16]byte
	// Remove hyphens
	clean := ""
	for _, c := range s {
		if c != '-' {
			clean += string(c)
		}
	}
	if len(clean) != 32 {
		return id, fmt.Errorf("invalid UUID: %s", s)
	}
	for i := 0; i < 16; i++ {
		var b byte
		_, err := fmt.Sscanf(clean[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return id, err
		}
		id[i] = b
	}
	return id, nil
}

// buildHistoryFilename constructs the full DB filename from a directory prefix and local name.
// At root level (dirPrefix=""), returns just the localName.
// In subdirectories, returns "dirPrefix/localName".
// historyDBFilename returns the filename to use for history table queries.
// For parent-pointer model: extracts the leaf name (history stores leaf names).
// The greedy history path parser may produce multi-segment localName like
// "inbox/task.md" -- only the leaf "task.md" is in the history table.
// For old model: builds full path from directory prefix + local filename.
func historyDBFilename(info *synth.ViewInfo, dirPrefix, localName string) string {
	if info.Roles.ParentID != "" {
		if idx := strings.LastIndex(localName, "/"); idx >= 0 {
			return localName[idx+1:]
		}
		return localName
	}
	return buildHistoryFilename(dirPrefix, localName)
}

func buildHistoryFilename(dirPrefix, localName string) string {
	if dirPrefix == "" {
		return localName
	}
	return dirPrefix + "/" + localName
}

// filterHistoryAtLevel filters history filenames to those at exactly one level
// below dirPrefix, returning directory entries for each unique filename at that level.
// For dirPrefix="" (root): returns filenames with no "/" (top-level only).
// For dirPrefix="getting-started": returns basenames of filenames like
// "getting-started/installation" (i.e., files in that subdirectory).
func filterHistoryAtLevel(filenames []string, dirPrefix string) []Entry {
	now := time.Now()
	seen := make(map[string]bool)
	var entries []Entry

	for _, fn := range filenames {
		var localName string
		if dirPrefix == "" {
			// Root level: only filenames with no "/"
			if strings.Contains(fn, "/") {
				continue
			}
			localName = fn
		} else {
			// Subdirectory: must start with prefix + "/"
			pfx := dirPrefix + "/"
			if !strings.HasPrefix(fn, pfx) {
				continue
			}
			rest := fn[len(pfx):]
			// Must be immediate child (no more slashes)
			if strings.Contains(rest, "/") {
				continue
			}
			localName = rest
		}

		if localName == "" || seen[localName] {
			continue
		}
		seen[localName] = true

		entries = append(entries, Entry{
			Name:    localName,
			IsDir:   true,
			Mode:    os.ModeDir | 0555,
			ModTime: now,
		})
	}

	return entries
}
