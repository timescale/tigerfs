// Package fuse provides FUSE filesystem operations for TigerFS.
//
// This file provides backwards compatibility for PipelineContext, which has
// been moved to the shared fs package as FSContext. New code should use
// fs.FSContext directly; PipelineContext is retained as a type alias.

package fuse

import (
	"github.com/timescale/tigerfs/internal/tigerfs/fs"
)

// Type aliases for fs package types.
// PipelineContext is now implemented in fs.FSContext.
// These aliases maintain backwards compatibility with existing FUSE code.
type (
	// PipelineContext is an alias for fs.FSContext.
	// Deprecated: Use fs.FSContext directly in new code.
	PipelineContext = fs.FSContext

	// LimitType represents the type of row limiting operation.
	LimitType = fs.LimitType

	// FilterCondition represents a column equality filter.
	FilterCondition = fs.FilterCondition

	// QueryParams contains the parameters needed to build a pipeline query.
	QueryParams = fs.QueryParams
)

// Re-export LimitType constants for convenience.
const (
	LimitNone   = fs.LimitNone
	LimitFirst  = fs.LimitFirst
	LimitLast   = fs.LimitLast
	LimitSample = fs.LimitSample
)

// NewPipelineContext creates a new pipeline context for a table.
// Deprecated: Use fs.NewFSContext directly in new code.
func NewPipelineContext(schema, table, pkColumn string) *PipelineContext {
	return fs.NewFSContext(schema, table, pkColumn)
}
