// Package fuse provides FUSE filesystem operations for TigerFS.
//
// This file re-exports constants from the shared fs package for backwards
// compatibility. New code should import from fs directly.

package fuse

import (
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// Metadata directory (re-exported from fs)
const DirInfo = fs.DirInfo

// Metadata files (re-exported from fs)
const (
	FileCount   = fs.FileCount
	FileDDL     = fs.FileDDL
	FileSchema  = fs.FileSchema
	FileColumns = fs.FileColumns
)

// Navigation capabilities (re-exported from fs)
const (
	DirBy     = fs.DirBy
	DirFilter = fs.DirFilter
	DirFirst  = fs.DirFirst
	DirLast   = fs.DirLast
	DirSample = fs.DirSample
	DirAll    = fs.DirAll
	DirOrder  = fs.DirOrder
)

// Bulk data capabilities (re-exported from fs)
const (
	DirExport      = fs.DirExport
	DirImport      = fs.DirImport
	DirOverwrite   = fs.DirOverwrite
	DirSync        = fs.DirSync
	DirAppend      = fs.DirAppend
	DirWithHeaders = fs.DirWithHeaders
	DirNoHeaders   = fs.DirNoHeaders
)

// Schema-level directories (re-exported from fs)
const (
	DirSchemas = fs.DirSchemas
	DirViews   = fs.DirViews
)

// DDL capabilities (re-exported from fs)
const (
	DirIndexes = fs.DirIndexes
	DirCreate  = fs.DirCreate
	DirModify  = fs.DirModify
	DirDelete  = fs.DirDelete
)

// Control files (re-exported from fs)
const (
	FileSQL     = fs.FileSQL
	FileTest    = fs.FileTest
	FileTestLog = fs.FileTestLog
	FileCommit  = fs.FileCommit
	FileAbort   = fs.FileAbort
)

// Format extensions (re-exported from fs)
const (
	ExtJSON = fs.ExtJSON
	ExtCSV  = fs.ExtCSV
	ExtTSV  = fs.ExtTSV
	ExtYAML = fs.ExtYAML
	ExtTxt  = fs.ExtTxt
	ExtBin  = fs.ExtBin
)

// Format names (re-exported from fs)
const (
	FmtJSON = fs.FmtJSON
	FmtCSV  = fs.FmtCSV
	FmtTSV  = fs.FmtTSV
	FmtYAML = fs.FmtYAML
)

// capabilityDirectories lists all pipeline capability directory names.
// Used to prevent these names from being interpreted as column values.
var capabilityDirectories = map[string]bool{
	DirBy:     true,
	DirFilter: true,
	DirFirst:  true,
	DirLast:   true,
	DirSample: true,
	DirAll:    true,
	DirOrder:  true,
	DirExport: true,
	DirImport: true,
}

// isCapabilityDirectory returns true if name is a reserved capability directory.
// This prevents capability names from being interpreted as column values.
func isCapabilityDirectory(name string) bool {
	return capabilityDirectories[name]
}
