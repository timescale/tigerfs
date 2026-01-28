package fuse

import (
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// SchemaFileNode represents a .sql file in a staging directory.
// On read: returns staged content if exists, otherwise generates a template.
// On write: stores content in the StagingTracker.
type SchemaFileNode struct {
	fs.Inode

	cfg     *config.Config
	db      db.DDLExecutor // Currently unused, but available for future template enhancement
	staging *StagingTracker
	ctx     StagingContext
}

var _ fs.InodeEmbedder = (*SchemaFileNode)(nil)
var _ fs.NodeGetattrer = (*SchemaFileNode)(nil)
var _ fs.NodeOpener = (*SchemaFileNode)(nil)
var _ fs.NodeSetattrer = (*SchemaFileNode)(nil)

// NewSchemaFileNode creates a new .sql file node.
func NewSchemaFileNode(cfg *config.Config, dbClient db.DDLExecutor, staging *StagingTracker, ctx StagingContext) *SchemaFileNode {
	return &SchemaFileNode{
		cfg:     cfg,
		db:      dbClient,
		staging: staging,
		ctx:     ctx,
	}
}

// Getattr returns attributes for the .sql file.
func (s *SchemaFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("SchemaFileNode.Getattr called",
		zap.String("path", s.ctx.StagingPath))

	// Get content to determine size
	content := s.getContent(ctx)

	out.Mode = 0644 | syscall.S_IFREG // Writable
	out.Nlink = 1
	out.Size = uint64(len(content))

	return 0
}

// Open opens the .sql file for reading or writing.
func (s *SchemaFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("SchemaFileNode.Open called",
		zap.String("path", s.ctx.StagingPath),
		zap.Uint32("flags", flags))

	content := s.getContent(ctx)

	fh := &SchemaFileHandle{
		node:    s,
		content: []byte(content),
	}

	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// Setattr handles attribute changes (e.g., truncate before write).
func (s *SchemaFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("SchemaFileNode.Setattr called",
		zap.String("path", s.ctx.StagingPath))

	// Handle truncate
	if sz, ok := in.GetSize(); ok && sz == 0 {
		s.staging.Set(s.ctx.StagingPath, "")
	}

	// Update attributes
	content := s.getContent(ctx)
	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = uint64(len(content))

	return 0
}

// getContent returns the staged content or generates a template.
func (s *SchemaFileNode) getContent(ctx context.Context) string {
	content := s.staging.GetContent(s.ctx.StagingPath)
	if content != "" {
		return content
	}

	// Generate template based on context
	return s.generateTemplate(ctx)
}

// generateTemplate generates a DDL template based on the staging context.
// This is a placeholder that will be enhanced in Task 5.2.
func (s *SchemaFileNode) generateTemplate(ctx context.Context) string {
	switch s.ctx.Operation {
	case DDLCreate:
		return s.generateCreateTemplate()
	case DDLModify:
		return s.generateModifyTemplate(ctx)
	case DDLDelete:
		return s.generateDeleteTemplate(ctx)
	default:
		return "-- Unknown operation\n"
	}
}

func (s *SchemaFileNode) generateCreateTemplate() string {
	switch s.ctx.ObjectType {
	case "table":
		return fmt.Sprintf(`-- Create table: %s
-- Uncomment and modify the following template:

-- CREATE TABLE %s (
--     id SERIAL PRIMARY KEY,
--     name TEXT NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT NOW()
-- );
`, s.ctx.ObjectName, s.ctx.ObjectName)

	case "index":
		return fmt.Sprintf(`-- Create index: %s on table %s
-- Uncomment and modify the following template:

-- CREATE INDEX %s ON %s (
--     column_name
-- );
`, s.ctx.ObjectName, s.ctx.TableName, s.ctx.ObjectName, s.ctx.TableName)

	case "schema":
		return fmt.Sprintf(`-- Create schema: %s
-- Uncomment to execute:

-- CREATE SCHEMA %s;
`, s.ctx.ObjectName, s.ctx.ObjectName)

	case "view":
		return fmt.Sprintf(`-- Create view: %s
-- Uncomment and modify the following template:

-- CREATE VIEW %s AS
-- SELECT
--     column1,
--     column2
-- FROM
--     table_name
-- WHERE
--     condition;
`, s.ctx.ObjectName, s.ctx.ObjectName)

	default:
		return fmt.Sprintf("-- Create %s: %s\n", s.ctx.ObjectType, s.ctx.ObjectName)
	}
}

func (s *SchemaFileNode) generateModifyTemplate(ctx context.Context) string {
	// Will be enhanced in Task 5.2 to fetch current schema
	return fmt.Sprintf(`-- Modify %s: %s
-- Examples:
-- ALTER TABLE %s ADD COLUMN column_name TYPE;
-- ALTER TABLE %s DROP COLUMN column_name;
-- ALTER TABLE %s ALTER COLUMN column_name TYPE new_type;

-- Add your ALTER statement below:

`, s.ctx.ObjectType, s.ctx.ObjectName, s.ctx.ObjectName, s.ctx.ObjectName, s.ctx.ObjectName)
}

func (s *SchemaFileNode) generateDeleteTemplate(ctx context.Context) string {
	switch s.ctx.ObjectType {
	case "table":
		return s.generateDeleteTableTemplate(ctx)

	case "index":
		return fmt.Sprintf(`-- Delete index: %s

-- Uncomment to delete:
-- DROP INDEX %s;
`, s.ctx.ObjectName, s.ctx.ObjectName)

	case "schema":
		return fmt.Sprintf(`-- Delete schema: %s
-- WARNING: This will delete the schema and potentially all objects within it.

-- Uncomment to delete (fails if schema contains objects):
-- DROP SCHEMA %s;

-- Or with CASCADE to delete schema and all contained objects:
-- DROP SCHEMA %s CASCADE;
`, s.ctx.ObjectName, s.ctx.ObjectName, s.ctx.ObjectName)

	case "view":
		return fmt.Sprintf(`-- Delete view: %s

-- Uncomment to delete:
-- DROP VIEW %s;

-- Or with CASCADE to also drop dependent views:
-- DROP VIEW %s CASCADE;
`, s.ctx.ObjectName, s.ctx.ObjectName, s.ctx.ObjectName)

	default:
		return fmt.Sprintf("-- Delete %s: %s\n", s.ctx.ObjectType, s.ctx.ObjectName)
	}
}

// generateDeleteTableTemplate generates a delete template for tables.
// Uses rich template with column info, row count, and foreign keys if db supports it.
func (s *SchemaFileNode) generateDeleteTableTemplate(ctx context.Context) string {
	// Try to use rich template if db supports the required interfaces
	if dbClient, ok := s.db.(db.DBClient); ok {
		template := &DeleteTableTemplate{
			Schema:    s.ctx.Schema,
			TableName: s.ctx.ObjectName,
			DB:        dbClient,
		}
		content, err := template.Generate(ctx)
		if err != nil {
			logging.Warn("Failed to generate rich delete template, using simple template",
				zap.String("table", s.ctx.ObjectName),
				zap.Error(err))
			// Fall through to simple template
		} else {
			return content
		}
	}

	// Simple template fallback (for mocks or when db doesn't support full interface)
	return fmt.Sprintf(`-- Delete table: %s
-- WARNING: This will permanently delete the table and all its data.

-- Uncomment to delete:
-- DROP TABLE %s;

-- Or with CASCADE to also drop dependent objects:
-- DROP TABLE %s CASCADE;
`, s.ctx.ObjectName, s.ctx.ObjectName, s.ctx.ObjectName)
}

// SchemaFileHandle handles read/write operations on .sql files.
type SchemaFileHandle struct {
	node    *SchemaFileNode
	content []byte
}

var _ fs.FileReader = (*SchemaFileHandle)(nil)
var _ fs.FileWriter = (*SchemaFileHandle)(nil)

// Read reads from the .sql file.
func (fh *SchemaFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	logging.Debug("SchemaFileHandle.Read called",
		zap.Int64("offset", off),
		zap.Int("size", len(dest)))

	end := off + int64(len(dest))
	if end > int64(len(fh.content)) {
		end = int64(len(fh.content))
	}

	if off >= int64(len(fh.content)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	return fuse.ReadResultData(fh.content[off:end]), 0
}

// Write writes to the .sql file, storing content in the staging tracker.
func (fh *SchemaFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	logging.Debug("SchemaFileHandle.Write called",
		zap.Int64("offset", off),
		zap.Int("size", len(data)))

	// Handle writes at offset (append or replace)
	if off == 0 {
		fh.content = data
	} else {
		// Extend content if needed
		if int64(len(fh.content)) < off+int64(len(data)) {
			newContent := make([]byte, off+int64(len(data)))
			copy(newContent, fh.content)
			fh.content = newContent
		}
		copy(fh.content[off:], data)
	}

	// Store in staging tracker
	fh.node.staging.Set(fh.node.ctx.StagingPath, string(fh.content))

	return uint32(len(data)), 0
}

// TestFileNode represents a .test file in a staging directory.
// Touch/open triggers DDL validation via BEGIN/ROLLBACK.
// This is a trigger-only file - results are read from .test.log.
type TestFileNode struct {
	fs.Inode

	cfg     *config.Config
	db      db.DDLExecutor
	staging *StagingTracker
	ctx     StagingContext
}

var _ fs.InodeEmbedder = (*TestFileNode)(nil)
var _ fs.NodeGetattrer = (*TestFileNode)(nil)
var _ fs.NodeOpener = (*TestFileNode)(nil)
var _ fs.NodeSetattrer = (*TestFileNode)(nil)

// NewTestFileNode creates a new .test file node.
func NewTestFileNode(cfg *config.Config, dbClient db.DDLExecutor, staging *StagingTracker, ctx StagingContext) *TestFileNode {
	return &TestFileNode{
		cfg:     cfg,
		db:      dbClient,
		staging: staging,
		ctx:     ctx,
	}
}

// Getattr returns attributes for the .test file.
// Size is always 0 since this is a trigger-only file.
func (t *TestFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = 0 // Trigger-only file, no readable content

	return 0
}

// Open opens the .test file. Any open triggers validation.
// Results are written to .test.log for reading.
func (t *TestFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("TestFileNode.Open called",
		zap.String("path", t.ctx.StagingPath),
		zap.Uint32("flags", flags))

	// Any open triggers the test (touch, cat, echo, etc.)
	if err := t.runTest(ctx); err != nil {
		logging.Error("DDL test failed",
			zap.String("path", t.ctx.StagingPath),
			zap.Error(err))
		return nil, 0, syscall.EIO
	}

	// Return nil file handle - this is a trigger-only file
	return nil, 0, 0
}

// Setattr handles touch operations (utime) which trigger validation.
func (t *TestFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("TestFileNode.Setattr called",
		zap.String("path", t.ctx.StagingPath))

	// Check if this is a touch (mtime update)
	if _, ok := in.GetMTime(); ok {
		if err := t.runTest(ctx); err != nil {
			logging.Error("DDL test failed",
				zap.String("path", t.ctx.StagingPath),
				zap.Error(err))
			return syscall.EIO
		}
	}

	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = 0 // Trigger-only file

	return 0
}

// runTest validates the DDL via BEGIN/ROLLBACK.
func (t *TestFileNode) runTest(ctx context.Context) error {
	// Check if there's content to test
	if !t.staging.HasContent(t.ctx.StagingPath) {
		result := "Error: No DDL content to test. Write DDL to .sql first.\n"
		t.staging.SetTestResult(t.ctx.StagingPath, result)
		return fmt.Errorf("no DDL content")
	}

	// Get and extract SQL
	content := t.staging.GetContent(t.ctx.StagingPath)
	sql := ExtractSQL(content)

	if sql == "" {
		result := "Error: .sql contains only comments. Uncomment the DDL to test.\n"
		t.staging.SetTestResult(t.ctx.StagingPath, result)
		return fmt.Errorf("only comments in schema")
	}

	// Test via ExecInTransaction
	err := t.db.ExecInTransaction(ctx, sql)
	if err != nil {
		result := fmt.Sprintf("Error: %s\n", err.Error())
		t.staging.SetTestResult(t.ctx.StagingPath, result)
		return err
	}

	result := "OK: DDL validated successfully.\n"
	t.staging.SetTestResult(t.ctx.StagingPath, result)

	logging.Info("DDL test passed",
		zap.String("path", t.ctx.StagingPath))

	return nil
}

// TestLogFileNode represents a .test.log file in a staging directory.
// This is a read-only file that shows the results of DDL validation.
type TestLogFileNode struct {
	fs.Inode

	cfg     *config.Config
	staging *StagingTracker
	ctx     StagingContext
}

var _ fs.InodeEmbedder = (*TestLogFileNode)(nil)
var _ fs.NodeGetattrer = (*TestLogFileNode)(nil)
var _ fs.NodeOpener = (*TestLogFileNode)(nil)

// NewTestLogFileNode creates a new .test.log file node.
func NewTestLogFileNode(cfg *config.Config, staging *StagingTracker, ctx StagingContext) *TestLogFileNode {
	return &TestLogFileNode{
		cfg:     cfg,
		staging: staging,
		ctx:     ctx,
	}
}

// Getattr returns attributes for the .test.log file.
func (t *TestLogFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	result := t.staging.GetTestResult(t.ctx.StagingPath)

	out.Mode = 0444 | syscall.S_IFREG // Read-only
	out.Nlink = 1
	out.Size = uint64(len(result))

	return 0
}

// Open opens the .test.log file for reading.
func (t *TestLogFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("TestLogFileNode.Open called",
		zap.String("path", t.ctx.StagingPath),
		zap.Uint32("flags", flags))

	// Read-only file - reject write attempts
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		return nil, 0, syscall.EACCES
	}

	result := t.staging.GetTestResult(t.ctx.StagingPath)
	fh := &TestLogFileHandle{content: []byte(result)}

	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// TestLogFileHandle handles read operations on .test.log files.
type TestLogFileHandle struct {
	content []byte
}

var _ fs.FileReader = (*TestLogFileHandle)(nil)

// Read reads the test result.
func (fh *TestLogFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	end := off + int64(len(dest))
	if end > int64(len(fh.content)) {
		end = int64(len(fh.content))
	}

	if off >= int64(len(fh.content)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	return fuse.ReadResultData(fh.content[off:end]), 0
}

// CommitFileNode represents a .commit file in a staging directory.
// Touch/open executes the staged DDL.
type CommitFileNode struct {
	fs.Inode

	cfg     *config.Config
	db      db.DDLExecutor
	staging *StagingTracker
	cache   *MetadataCache
	ctx     StagingContext
}

var _ fs.InodeEmbedder = (*CommitFileNode)(nil)
var _ fs.NodeGetattrer = (*CommitFileNode)(nil)
var _ fs.NodeOpener = (*CommitFileNode)(nil)
var _ fs.NodeSetattrer = (*CommitFileNode)(nil)

// NewCommitFileNode creates a new .commit file node.
func NewCommitFileNode(cfg *config.Config, dbClient db.DDLExecutor, staging *StagingTracker, ctx StagingContext) *CommitFileNode {
	return &CommitFileNode{
		cfg:     cfg,
		db:      dbClient,
		staging: staging,
		ctx:     ctx,
	}
}

// Getattr returns attributes for the .commit file.
func (c *CommitFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = 0

	return 0
}

// Open opens the .commit file. If opened for writing, executes DDL.
func (c *CommitFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("CommitFileNode.Open called",
		zap.String("path", c.ctx.StagingPath),
		zap.Uint32("flags", flags))

	// If opened for writing, trigger commit
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		if err := c.runCommit(ctx); err != nil {
			logging.Error("DDL commit failed",
				zap.String("path", c.ctx.StagingPath),
				zap.Error(err))
			return nil, 0, syscall.EIO
		}
	}

	return nil, 0, 0
}

// Setattr handles touch operations which trigger commit.
func (c *CommitFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("CommitFileNode.Setattr called",
		zap.String("path", c.ctx.StagingPath))

	// Check if this is a touch (mtime update)
	if _, ok := in.GetMTime(); ok {
		if err := c.runCommit(ctx); err != nil {
			logging.Error("DDL commit failed",
				zap.String("path", c.ctx.StagingPath),
				zap.Error(err))
			return syscall.EIO
		}
	}

	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = 0

	return 0
}

// runCommit executes the DDL.
func (c *CommitFileNode) runCommit(ctx context.Context) error {
	// Check if there's content to commit
	if !c.staging.HasContent(c.ctx.StagingPath) {
		return fmt.Errorf("no DDL content to commit. Write DDL to .sql first")
	}

	// Get and extract SQL
	content := c.staging.GetContent(c.ctx.StagingPath)
	sql := ExtractSQL(content)

	if sql == "" {
		return fmt.Errorf(".sql contains only comments. Uncomment the DDL to commit")
	}

	// Execute DDL
	err := c.db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("DDL execution failed: %w", err)
	}

	logging.Info("DDL committed successfully",
		zap.String("path", c.ctx.StagingPath),
		zap.String("objectType", c.ctx.ObjectType),
		zap.String("objectName", c.ctx.ObjectName))

	// Clear staging entry on success
	c.staging.Delete(c.ctx.StagingPath)

	// Invalidate metadata cache to pick up changes
	if c.cache != nil {
		c.cache.Invalidate()
	}

	return nil
}

// AbortFileNode represents a .abort file in a staging directory.
// Touch/open clears the staging entry.
type AbortFileNode struct {
	fs.Inode

	cfg     *config.Config
	db      db.DDLExecutor // Unused, but kept for consistency with other control file nodes
	staging *StagingTracker
	ctx     StagingContext
}

var _ fs.InodeEmbedder = (*AbortFileNode)(nil)
var _ fs.NodeGetattrer = (*AbortFileNode)(nil)
var _ fs.NodeOpener = (*AbortFileNode)(nil)
var _ fs.NodeSetattrer = (*AbortFileNode)(nil)

// NewAbortFileNode creates a new .abort file node.
func NewAbortFileNode(cfg *config.Config, dbClient db.DDLExecutor, staging *StagingTracker, ctx StagingContext) *AbortFileNode {
	return &AbortFileNode{
		cfg:     cfg,
		db:      dbClient,
		staging: staging,
		ctx:     ctx,
	}
}

// Getattr returns attributes for the .abort file.
func (a *AbortFileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = 0

	return 0
}

// Open opens the .abort file. If opened for writing, clears staging.
func (a *AbortFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debug("AbortFileNode.Open called",
		zap.String("path", a.ctx.StagingPath),
		zap.Uint32("flags", flags))

	// If opened for writing, trigger abort
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		a.runAbort()
	}

	return nil, 0, 0
}

// Setattr handles touch operations which trigger abort.
func (a *AbortFileNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	logging.Debug("AbortFileNode.Setattr called",
		zap.String("path", a.ctx.StagingPath))

	// Check if this is a touch (mtime update)
	if _, ok := in.GetMTime(); ok {
		a.runAbort()
	}

	out.Mode = 0644 | syscall.S_IFREG
	out.Nlink = 1
	out.Size = 0

	return 0
}

// runAbort clears the staging entry.
func (a *AbortFileNode) runAbort() {
	logging.Info("DDL aborted",
		zap.String("path", a.ctx.StagingPath))

	a.staging.Delete(a.ctx.StagingPath)
}
