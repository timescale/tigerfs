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
	schema := fsCtx.Schema
	historyTable := "_" + fsCtx.TableName + "_history"
	now := time.Now()

	limit := o.config.DirListingLimit
	if limit <= 0 {
		limit = 10000
	}

	if parsed.HistoryByID {
		return o.readDirHistoryByID(ctx, schema, historyTable, parsed, info, now, limit)
	}
	return o.readDirHistoryByFilename(ctx, schema, historyTable, parsed, info, now, limit)
}

// readDirHistoryByFilename lists history entries organized by filename.
// Uses parsed.PrimaryKey as a directory prefix to show only files at the current level.
func (o *Operations) readDirHistoryByFilename(ctx context.Context, schema, historyTable string, parsed *ParsedPath, info *synth.ViewInfo, now time.Time, limit int) ([]Entry, *FSError) {
	if parsed.HistoryFile == "" {
		// /{table}/.history/ or /{table}/subdir/.history/ — list filenames at this level
		filenames, err := o.db.QueryHistoryDistinctFilenames(ctx, schema, historyTable, limit)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to list history filenames", Cause: err}
		}

		dirPrefix := parsed.PrimaryKey
		filtered := filterHistoryAtLevel(filenames, dirPrefix)

		entries := make([]Entry, 0, len(filtered)+1)
		// Add .by/ entry only at root level (dirPrefix == "")
		if dirPrefix == "" {
			entries = append(entries, Entry{Name: ".by", IsDir: true, Mode: os.ModeDir | 0555, ModTime: now})
		}
		entries = append(entries, filtered...)
		return entries, nil
	}

	// /{table}/.history/foo.md/ — list versions for filename + .id file
	// Build full DB filename from directory prefix + local filename
	rawFilename := buildHistoryFilename(parsed.PrimaryKey, parsed.HistoryFile)

	columns, rows, err := o.db.QueryHistoryByFilename(ctx, schema, historyTable, rawFilename, limit)
	if err != nil {
		return nil, &FSError{Code: ErrIO, Message: "failed to query history versions", Cause: err}
	}

	entries := make([]Entry, 0, len(rows)+1)
	// Add .id virtual file
	entries = append(entries, Entry{Name: ".id", IsDir: false, Mode: 0444, Size: 36, ModTime: now})

	historyIDIdx := columnIndex(columns, "_history_id")
	for _, row := range rows {
		if historyIDIdx < 0 {
			continue
		}
		versionID := historyIDToVersionID(row[historyIDIdx])
		if versionID == "" {
			continue
		}
		entries = append(entries, Entry{
			Name:    versionID,
			IsDir:   false,
			Mode:    0444,
			Size:    0, // Size unknown without synthesizing content
			ModTime: now,
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
	historyIDIdx := columnIndex(columns, "_history_id")
	for _, row := range rows {
		if historyIDIdx < 0 {
			continue
		}
		versionID := historyIDToVersionID(row[historyIDIdx])
		if versionID == "" {
			continue
		}
		entries = append(entries, Entry{
			Name:    versionID,
			IsDir:   false,
			Mode:    0444,
			Size:    0,
			ModTime: now,
		})
	}
	return entries, nil
}

// statHistory returns metadata for .history/ paths.
func (o *Operations) statHistory(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) (*Entry, *FSError) {
	now := time.Now()
	schema := parsed.Context.Schema
	historyTable := "_" + parsed.Context.TableName + "_history"

	// .history/ directory itself
	if parsed.HistoryFile == "" && !parsed.HistoryByID {
		return &Entry{Name: ".history", IsDir: true, Mode: os.ModeDir | 0555, ModTime: now}, nil
	}

	// .history/.by/ directory
	if parsed.HistoryByID && parsed.HistoryRowID == "" {
		return &Entry{Name: ".by", IsDir: true, Mode: os.ModeDir | 0555, ModTime: now}, nil
	}

	// .history/.by/<uuid>/ — check UUID has history
	if parsed.HistoryByID && parsed.HistoryRowID != "" && parsed.HistoryVersionID == "" {
		_, rows, err := o.db.QueryHistoryByID(ctx, schema, historyTable, parsed.HistoryRowID, 1)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to check history by ID", Cause: err}
		}
		if len(rows) == 0 {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("no history for row ID %s", parsed.HistoryRowID)}
		}
		return &Entry{Name: parsed.HistoryRowID, IsDir: true, Mode: os.ModeDir | 0555, ModTime: now}, nil
	}

	// .history/.by/<uuid>/<versionID> — version file by UUID
	if parsed.HistoryByID && parsed.HistoryVersionID != "" {
		return o.statHistoryVersion(ctx, schema, historyTable, "id", parsed.HistoryRowID, parsed.HistoryVersionID, info, now)
	}

	// .history/foo.md/ — filename directory
	if parsed.HistoryFile != "" && parsed.HistoryVersionID == "" {
		rawFilename := buildHistoryFilename(parsed.PrimaryKey, parsed.HistoryFile)
		_, rows, err := o.db.QueryHistoryByFilename(ctx, schema, historyTable, rawFilename, 1)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to check history by filename", Cause: err}
		}
		if len(rows) == 0 {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("no history for %s", parsed.HistoryFile)}
		}
		return &Entry{Name: parsed.HistoryFile, IsDir: true, Mode: os.ModeDir | 0555, ModTime: now}, nil
	}

	// .history/foo.md/.id — virtual file returning the row UUID
	if parsed.HistoryFile != "" && parsed.HistoryVersionID == ".id" {
		return &Entry{Name: ".id", IsDir: false, Mode: 0444, Size: 36, ModTime: now}, nil
	}

	// .history/foo.md/<versionID> — version file by filename
	if parsed.HistoryFile != "" && parsed.HistoryVersionID != "" {
		rawFilename := buildHistoryFilename(parsed.PrimaryKey, parsed.HistoryFile)
		return o.statHistoryVersion(ctx, schema, historyTable, "filename", rawFilename, parsed.HistoryVersionID, info, now)
	}

	return nil, &FSError{Code: ErrNotExist, Message: "invalid history path"}
}

// statHistoryVersion returns metadata for a specific version file in .history/.
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

	return &Entry{Name: versionID, IsDir: false, Mode: 0444, Size: size, ModTime: now}, nil
}

// readHistoryFile reads a file within .history/.
func (o *Operations) readHistoryFile(ctx context.Context, parsed *ParsedPath, info *synth.ViewInfo) ([]byte, *FSError) {
	schema := parsed.Context.Schema
	historyTable := "_" + parsed.Context.TableName + "_history"

	// .id file: return the row UUID for this filename
	if parsed.HistoryFile != "" && parsed.HistoryVersionID == ".id" {
		rawFilename := buildHistoryFilename(parsed.PrimaryKey, parsed.HistoryFile)
		columns, rows, err := o.db.QueryHistoryByFilename(ctx, schema, historyTable, rawFilename, 1)
		if err != nil {
			return nil, &FSError{Code: ErrIO, Message: "failed to query history for .id", Cause: err}
		}
		if len(rows) == 0 {
			return nil, &FSError{Code: ErrNotExist, Message: fmt.Sprintf("no history for %s", parsed.HistoryFile)}
		}
		idIdx := columnIndex(columns, "id")
		if idIdx < 0 {
			return nil, &FSError{Code: ErrIO, Message: "id column not found in history table"}
		}
		idStr := synth.ValueToString(rows[0][idIdx])
		return []byte(idStr + "\n"), nil
	}

	// Version file: read and synthesize content
	var filterColumn, filterValue string
	if parsed.HistoryByID {
		filterColumn = "id"
		filterValue = parsed.HistoryRowID
	} else {
		filterColumn = "filename"
		filterValue = buildHistoryFilename(parsed.PrimaryKey, parsed.HistoryFile)
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

// findVersionRow scans rows for one whose _history_id matches the given versionID.
func findVersionRow(columns []string, rows [][]interface{}, versionID string) []interface{} {
	historyIDIdx := columnIndex(columns, "_history_id")
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

// historyIDToVersionID converts a _history_id value (UUIDv7) to a version ID string.
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
