package fs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/timescale/tigerfs/internal/tigerfs/fs/synth"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// readDirBuild lists the /.build/ directory.
// This is a write-only directory — listing returns no entries.
func (o *Operations) readDirBuild(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	return []Entry{}, nil
}

// statBuild returns metadata for .build paths.
//   - /.build/ → directory
//   - /.build/<name> → writable virtual file
func (o *Operations) statBuild(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	now := time.Now()

	if parsed.BuildName == "" {
		// /.build/ directory itself
		return &Entry{Name: ".build", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	// /.build/<name> — virtual writable file
	return &Entry{
		Name:    parsed.BuildName,
		IsDir:   false,
		Mode:    0644,
		Size:    0,
		ModTime: now,
	}, nil
}

// writeBuildFile handles writes to /.build/<name>.
// The written content is a format name or comma-separated features.
//
// Supported inputs:
//   - "markdown" → create new markdown app
//   - "markdown,history" → create new markdown app with versioned history
//   - "history" → add versioned history to an existing app
func (o *Operations) writeBuildFile(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	if parsed.BuildName == "" {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "app name is required",
			Hint:    "write to .build/<name>, e.g.: echo markdown > .build/posts",
		}
	}

	// Parse features from written content
	featureStr := strings.TrimSpace(string(data))
	features := synth.ParseFeatureString(featureStr)

	// Determine schema — use context schema if set (schema-level .build),
	// otherwise resolve the default schema
	schema := ""
	if parsed.Context != nil && parsed.Context.Schema != "" {
		schema = parsed.Context.Schema
	} else {
		o.schemaOnce.Do(func() {
			o.cachedSchema, o.schemaErr = o.db.GetCurrentSchema(ctx)
		})
		if o.schemaErr != nil {
			return &FSError{Code: ErrIO, Message: "failed to resolve current schema", Cause: o.schemaErr}
		}
		schema = o.cachedSchema
	}

	appName := parsed.BuildName

	// Check if history is requested — requires TimescaleDB
	if features.History {
		hasTS, err := o.db.HasExtension(ctx, "timescaledb")
		if err != nil {
			return &FSError{Code: ErrIO, Message: "failed to check TimescaleDB extension", Cause: err}
		}
		if !hasTS {
			return &FSError{
				Code:    ErrInvalidPath,
				Message: "history requires TimescaleDB extension",
				Hint:    "install TimescaleDB or use a TimescaleDB-enabled PostgreSQL image",
			}
		}
	}

	// Check if this is "add history to existing app" path
	if features.Format == synth.FormatNative && features.History {
		return o.writeBuildAddHistory(ctx, schema, appName)
	}

	// Validate format for new app creation
	if features.Format == synth.FormatNative || features.Format == synth.FormatTasks {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("unsupported format: %q", featureStr),
			Hint:    "supported formats: markdown, txt (optionally with ,history)",
		}
	}

	// Generate the SQL statements for new app (with optional history)
	statements, err := synth.GenerateBuildSQLWithFeatures(schema, appName, features)
	if err != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to generate build SQL",
			Cause:   err,
		}
	}

	// Execute each statement separately (Exec doesn't support multi-statement)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if execErr := o.db.Exec(ctx, stmt); execErr != nil {
			return &FSError{
				Code:    ErrIO,
				Message: fmt.Sprintf("failed to execute build SQL for %q", appName),
				Hint:    fmt.Sprintf("statement failed: %s", truncateSQL(stmt)),
				Cause:   execErr,
			}
		}
	}

	// Invalidate synth cache and metadata cache so the new view/table are detected
	o.invalidateSynthCache()
	o.metaCache.Invalidate()

	historyStr := ""
	if features.History {
		historyStr = "+history"
	}
	logging.Info("synthesized app created",
		zap.String("app", appName),
		zap.String("schema", schema),
		zap.String("format", features.Format.String()+historyStr),
		zap.String("table", "_"+appName),
		zap.String("view", appName))

	return nil
}

// writeBuildAddHistory adds versioned history to an existing synth app.
// Called when the .build/ content is just "history" (no format).
func (o *Operations) writeBuildAddHistory(ctx context.Context, schema, appName string) *FSError {
	// Check that the app (backing table) exists
	tableName := "_" + appName
	exists, err := o.db.TableExists(ctx, schema, tableName)
	if err != nil {
		return &FSError{Code: ErrIO, Message: "failed to check table existence", Cause: err}
	}
	if !exists {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: fmt.Sprintf("app %q does not exist", appName),
			Hint:    "create the app first with a format (e.g., 'markdown,history')",
		}
	}

	// Detect existing features from view comment
	info := o.getSynthViewInfo(ctx, schema, appName)
	existingFeatures := synth.FeatureSet{}
	if info != nil {
		existingFeatures.Format = info.Format
	}

	// Generate history-only SQL (comment update + history infrastructure)
	statements := synth.GenerateHistoryOnlySQL(schema, appName, existingFeatures)

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if execErr := o.db.Exec(ctx, stmt); execErr != nil {
			return &FSError{
				Code:    ErrIO,
				Message: fmt.Sprintf("failed to add history to %q", appName),
				Hint:    fmt.Sprintf("statement failed: %s", truncateSQL(stmt)),
				Cause:   execErr,
			}
		}
	}

	// Invalidate synth cache and metadata cache
	o.invalidateSynthCache()
	o.metaCache.Invalidate()

	logging.Info("versioned history added",
		zap.String("app", appName),
		zap.String("schema", schema),
		zap.String("history_table", tableName+"_history"))

	return nil
}

// readDirFormat lists the /{table}/.format/ directory.
// Shows available format names that can be written to create synthesized views.
func (o *Operations) readDirFormat(ctx context.Context, parsed *ParsedPath) ([]Entry, *FSError) {
	now := time.Now()

	var entries []Entry
	for _, name := range synth.AvailableFormats() {
		entries = append(entries, Entry{
			Name:    name,
			IsDir:   false,
			Mode:    0644,
			Size:    0,
			ModTime: now,
		})
	}

	return entries, nil
}

// statFormat returns metadata for .format paths.
//   - /{table}/.format/ → directory
//   - /{table}/.format/<format> → writable virtual file
func (o *Operations) statFormat(ctx context.Context, parsed *ParsedPath) (*Entry, *FSError) {
	now := time.Now()

	if parsed.FormatTarget == "" {
		return &Entry{Name: ".format", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now}, nil
	}

	return &Entry{
		Name:    parsed.FormatTarget,
		IsDir:   false,
		Mode:    0644,
		Size:    0,
		ModTime: now,
	}, nil
}

// writeFormatFile handles writes to /{table}/.format/<format>.
// The written content is ignored — the format is determined by the filename.
// Creates a synthesized view on the existing table with the appropriate comment.
func (o *Operations) writeFormatFile(ctx context.Context, parsed *ParsedPath, data []byte) *FSError {
	fsCtx := parsed.Context
	if fsCtx == nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "missing table context for .format/",
		}
	}

	if parsed.FormatTarget == "" {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: "format name is required",
			Hint:    "write to .format/<format>, e.g.: echo ok > table/.format/markdown",
		}
	}

	// Parse format from the path (FormatTarget), not from data.
	// The format is the filename within .format/ (e.g., .format/markdown).
	viewName, format, err := synth.ViewNameFromTableAndFormat(fsCtx.TableName, parsed.FormatTarget)
	if err != nil {
		return &FSError{
			Code:    ErrInvalidPath,
			Message: err.Error(),
			Hint:    "supported formats: markdown, txt",
		}
	}

	// Generate SQL
	sql, genErr := synth.GenerateSynthesizedViewSQL(fsCtx.Schema, fsCtx.TableName, format)
	if genErr != nil {
		return &FSError{
			Code:    ErrIO,
			Message: "failed to generate view SQL",
			Cause:   genErr,
		}
	}

	// Execute each statement separately
	statements := strings.Split(sql, ";\n")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if execErr := o.db.Exec(ctx, stmt); execErr != nil {
			return &FSError{
				Code:    ErrIO,
				Message: fmt.Sprintf("failed to create synthesized view %q", viewName),
				Hint:    fmt.Sprintf("statement failed: %s", truncateSQL(stmt)),
				Cause:   execErr,
			}
		}
	}

	// Invalidate synth cache and metadata cache so the new view/table are detected
	o.invalidateSynthCache()
	o.metaCache.Invalidate()

	logging.Info("synthesized view created",
		zap.String("table", fsCtx.TableName),
		zap.String("schema", fsCtx.Schema),
		zap.String("format", format.String()),
		zap.String("view", viewName))

	return nil
}

// truncateSQL truncates a SQL statement for error messages.
func truncateSQL(sql string) string {
	if len(sql) <= 80 {
		return sql
	}
	return sql[:80] + "..."
}
