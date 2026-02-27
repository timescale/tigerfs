package logging

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

// Init initializes the global logger with the specified level.
// Valid levels: "error", "warn", "info", "debug".
// For "warn" and "error", a minimal production config is used.
// For "info" and "debug", a development config with timestamps and colors is used.
func Init(level string) error {
	zapLevel, err := parseLevel(level)
	if err != nil {
		return err
	}

	var config zap.Config

	if zapLevel <= zapcore.InfoLevel {
		// Development mode: full logging with colors
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		// Production mode: minimal logging
		config = zap.NewProductionConfig()
		config.EncoderConfig.MessageKey = "message"
		config.EncoderConfig.LevelKey = ""
		config.EncoderConfig.TimeKey = ""
		config.EncoderConfig.CallerKey = ""
		config.EncoderConfig.StacktraceKey = ""
	}

	config.Level = zap.NewAtomicLevelAt(zapLevel)

	// Output to stderr by default
	config.OutputPaths = []string{"stderr"}
	config.ErrorOutputPaths = []string{"stderr"}

	logger, err = config.Build()
	if err != nil {
		return err
	}

	return nil
}

// parseLevel converts a level string to a zapcore.Level.
func parseLevel(level string) (zapcore.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return zapcore.WarnLevel, fmt.Errorf("invalid log level %q (must be debug, info, warn, or error)", level)
	}
}

// Debug logs a debug message
func Debug(msg string, fields ...zap.Field) {
	if logger != nil {
		logger.Debug(msg, fields...)
	}
}

// Info logs an info message
func Info(msg string, fields ...zap.Field) {
	if logger != nil {
		logger.Info(msg, fields...)
	}
}

// Warn logs a warning message
func Warn(msg string, fields ...zap.Field) {
	if logger != nil {
		logger.Warn(msg, fields...)
	}
}

// Error logs an error message
func Error(msg string, fields ...zap.Field) {
	if logger != nil {
		logger.Error(msg, fields...)
	}
}

// Fatal logs a fatal message and exits
func Fatal(msg string, fields ...zap.Field) {
	if logger != nil {
		logger.Fatal(msg, fields...)
	}
}

// Sync flushes any buffered log entries
func Sync() error {
	if logger != nil {
		return logger.Sync()
	}
	return nil
}
