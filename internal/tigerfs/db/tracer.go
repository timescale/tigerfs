package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

// traceStartKey is the context key for storing query trace data.
type traceStartKey struct{}

// traceData holds the start time and SQL text for a traced query.
type traceData struct {
	start time.Time
	sql   string
}

// dbTracer implements pgx.QueryTracer to log SQL queries with timing
// and connection identity at debug level.
type dbTracer struct{}

// Compile-time interface check.
var _ pgx.QueryTracer = (*dbTracer)(nil)

// TraceQueryStart records the start time and SQL text in the context.
func (t *dbTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, traceStartKey{}, &traceData{
		start: time.Now(),
		sql:   data.SQL,
	})
}

// TraceQueryEnd logs the SQL text, elapsed duration, command tag, backend PID, and any error.
func (t *dbTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	td, _ := ctx.Value(traceStartKey{}).(*traceData)

	fields := []zap.Field{
		zap.Duration("duration", time.Since(td.start)),
		zap.String("sql", td.sql),
		zap.String("command_tag", data.CommandTag.String()),
		zap.Uint32("pg_pid", conn.PgConn().PID()),
	}
	if data.Err != nil {
		fields = append(fields, zap.Error(data.Err))
	}
	logging.Info("sql query", fields...)
}
