package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

// Init initializes the global logger with the specified debug mode
func Init(debug bool) error {
	var config zap.Config

	if debug {
		// Development mode: full logging with colors
		config = zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		// Production mode: minimal logging
		config = zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
		config.EncoderConfig.MessageKey = "message"
		config.EncoderConfig.LevelKey = ""
		config.EncoderConfig.TimeKey = ""
		config.EncoderConfig.CallerKey = ""
		config.EncoderConfig.StacktraceKey = ""
	}

	// Output to stderr by default
	config.OutputPaths = []string{"stderr"}
	config.ErrorOutputPaths = []string{"stderr"}

	var err error
	logger, err = config.Build()
	if err != nil {
		return err
	}

	return nil
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
