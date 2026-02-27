package logging

import (
	"testing"

	"go.uber.org/zap"
)

// resetLogger resets the global logger for test isolation
func resetLogger() {
	logger = nil
}

// TestInit_DebugMode tests initialization in debug mode
func TestInit_DebugMode(t *testing.T) {
	resetLogger()

	err := Init("debug")
	if err != nil {
		t.Fatalf("Init(\"debug\") failed: %v", err)
	}

	if logger == nil {
		t.Error("Expected logger to be initialized")
	}

	// Clean up
	_ = Sync()
}

// TestInit_ProductionMode tests initialization in production mode
func TestInit_ProductionMode(t *testing.T) {
	resetLogger()

	err := Init("warn")
	if err != nil {
		t.Fatalf("Init(\"warn\") failed: %v", err)
	}

	if logger == nil {
		t.Error("Expected logger to be initialized")
	}

	// Clean up
	_ = Sync()
}

// TestInit_AllLevels tests that all valid levels are accepted
func TestInit_AllLevels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			resetLogger()
			err := Init(level)
			if err != nil {
				t.Fatalf("Init(%q) failed: %v", level, err)
			}
			if logger == nil {
				t.Error("Expected logger to be initialized")
			}
			_ = Sync()
		})
	}
}

// TestInit_InvalidLevel tests that invalid levels return an error
func TestInit_InvalidLevel(t *testing.T) {
	resetLogger()

	err := Init("invalid")
	if err == nil {
		t.Error("Expected error for invalid log level")
	}
}

// TestInit_Multiple tests calling Init multiple times
func TestInit_Multiple(t *testing.T) {
	resetLogger()

	// First init
	err := Init("debug")
	if err != nil {
		t.Fatalf("First Init failed: %v", err)
	}

	// Second init should succeed and replace logger
	err = Init("warn")
	if err != nil {
		t.Fatalf("Second Init failed: %v", err)
	}

	if logger == nil {
		t.Error("Expected logger to be initialized after multiple inits")
	}

	_ = Sync()
}

// TestDebug_WithInitializedLogger tests Debug with an initialized logger
func TestDebug_WithInitializedLogger(t *testing.T) {
	resetLogger()
	_ = Init("debug") // Debug mode to actually log debug messages

	// Should not panic
	Debug("test debug message")
	Debug("test with field", zap.String("key", "value"))
	Debug("test with multiple fields", zap.Int("count", 42), zap.Bool("active", true))

	_ = Sync()
}

// TestDebug_WithNilLogger tests Debug with nil logger (before Init)
func TestDebug_WithNilLogger(t *testing.T) {
	resetLogger()

	// Should not panic even with nil logger
	Debug("test debug message")
	Debug("test with field", zap.String("key", "value"))
}

// TestInfo_WithInitializedLogger tests Info with an initialized logger
func TestInfo_WithInitializedLogger(t *testing.T) {
	resetLogger()
	_ = Init("debug")

	// Should not panic
	Info("test info message")
	Info("test with field", zap.String("key", "value"))
	Info("test with multiple fields", zap.Int("count", 42), zap.Bool("active", true))

	_ = Sync()
}

// TestInfo_WithNilLogger tests Info with nil logger (before Init)
func TestInfo_WithNilLogger(t *testing.T) {
	resetLogger()

	// Should not panic even with nil logger
	Info("test info message")
	Info("test with field", zap.String("key", "value"))
}

// TestWarn_WithInitializedLogger tests Warn with an initialized logger
func TestWarn_WithInitializedLogger(t *testing.T) {
	resetLogger()
	_ = Init("warn") // Production mode still logs warn

	// Should not panic
	Warn("test warn message")
	Warn("test with field", zap.String("key", "value"))
	Warn("test with error", zap.Error(nil))

	_ = Sync()
}

// TestWarn_WithNilLogger tests Warn with nil logger (before Init)
func TestWarn_WithNilLogger(t *testing.T) {
	resetLogger()

	// Should not panic even with nil logger
	Warn("test warn message")
	Warn("test with field", zap.String("key", "value"))
}

// TestError_WithInitializedLogger tests Error with an initialized logger
func TestError_WithInitializedLogger(t *testing.T) {
	resetLogger()
	_ = Init("warn")

	// Should not panic
	Error("test error message")
	Error("test with field", zap.String("key", "value"))
	Error("test with error", zap.Error(nil))

	_ = Sync()
}

// TestError_WithNilLogger tests Error with nil logger (before Init)
func TestError_WithNilLogger(t *testing.T) {
	resetLogger()

	// Should not panic even with nil logger
	Error("test error message")
	Error("test with field", zap.String("key", "value"))
}

// Note: TestFatal is not tested because it calls os.Exit
// Fatal logging is covered by integration tests

// TestSync_WithInitializedLogger tests Sync with an initialized logger
func TestSync_WithInitializedLogger(t *testing.T) {
	resetLogger()
	_ = Init("debug")

	err := Sync()
	// Note: Sync may return an error on some systems due to stderr
	// We just check it doesn't panic
	_ = err
}

// TestSync_WithNilLogger tests Sync with nil logger
func TestSync_WithNilLogger(t *testing.T) {
	resetLogger()

	err := Sync()
	if err != nil {
		t.Errorf("Sync() with nil logger should return nil, got %v", err)
	}
}

// TestLogging_AllLogLevels tests all logging levels in sequence
func TestLogging_AllLogLevels(t *testing.T) {
	resetLogger()
	_ = Init("debug")

	// Log at all levels
	Debug("debug message")
	Info("info message")
	Warn("warn message")
	Error("error message")

	_ = Sync()
}

// TestLogging_VariousFieldTypes tests logging with various field types
func TestLogging_VariousFieldTypes(t *testing.T) {
	resetLogger()
	_ = Init("debug")

	Debug("test",
		zap.String("string", "value"),
		zap.Int("int", 42),
		zap.Int64("int64", 123456789012345),
		zap.Float64("float", 3.14159),
		zap.Bool("bool", true),
		zap.Strings("strings", []string{"a", "b", "c"}),
		zap.Error(nil),
	)

	_ = Sync()
}

// TestInit_DebugModeConfiguration tests that debug mode is configured correctly
func TestInit_DebugModeConfiguration(t *testing.T) {
	resetLogger()
	err := Init("debug")
	if err != nil {
		t.Fatalf("Init(\"debug\") failed: %v", err)
	}

	// In debug mode, debug messages should be logged
	// We can't easily verify output, but we verify initialization succeeds
	if logger == nil {
		t.Error("Logger should be initialized in debug mode")
	}

	_ = Sync()
}

// TestInit_ProductionModeConfiguration tests that production mode is configured correctly
func TestInit_ProductionModeConfiguration(t *testing.T) {
	resetLogger()
	err := Init("warn")
	if err != nil {
		t.Fatalf("Init(\"warn\") failed: %v", err)
	}

	// In production mode, only warn and above should be logged
	// We can't easily verify output, but we verify initialization succeeds
	if logger == nil {
		t.Error("Logger should be initialized in production mode")
	}

	_ = Sync()
}

// TestLogging_EmptyMessage tests logging empty messages
func TestLogging_EmptyMessage(t *testing.T) {
	resetLogger()
	_ = Init("debug")

	// Should not panic with empty messages
	Debug("")
	Info("")
	Warn("")
	Error("")

	_ = Sync()
}

// TestLogging_LongMessage tests logging very long messages
func TestLogging_LongMessage(t *testing.T) {
	resetLogger()
	_ = Init("debug")

	// Create a long message
	longMsg := ""
	for i := 0; i < 1000; i++ {
		longMsg += "This is a very long message. "
	}

	// Should not panic with long messages
	Debug(longMsg)

	_ = Sync()
}

// TestLogging_SpecialCharacters tests logging messages with special characters
func TestLogging_SpecialCharacters(t *testing.T) {
	resetLogger()
	_ = Init("debug")

	// Should not panic with special characters
	Debug("Tab:\t Newline:\n Quote:\"")
	Debug("Unicode: 日本語 emoji: 🎉")
	Debug("Backslash: \\ Null: \x00")

	_ = Sync()
}

// TestLogging_ConcurrentAccess tests concurrent logging
func TestLogging_ConcurrentAccess(t *testing.T) {
	resetLogger()
	_ = Init("debug")

	done := make(chan bool, 10)

	// Spawn multiple goroutines logging concurrently
	for i := 0; i < 10; i++ {
		go func(n int) {
			for j := 0; j < 100; j++ {
				Debug("concurrent message", zap.Int("goroutine", n), zap.Int("iter", j))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	_ = Sync()
}
