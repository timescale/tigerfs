package db

import (
	"context"
	"os"
	"testing"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

func TestResolvePassword_FromPGPASSWORD(t *testing.T) {
	// Set environment variable
	originalValue := os.Getenv("PGPASSWORD")
	defer func() {
		if originalValue != "" {
			os.Setenv("PGPASSWORD", originalValue)
		} else {
			os.Unsetenv("PGPASSWORD")
		}
	}()

	os.Setenv("PGPASSWORD", "test_password")

	cfg := &config.Config{}
	ctx := context.Background()

	password, err := resolvePassword(ctx, cfg, "postgres://user@localhost/db")
	if err != nil {
		t.Fatalf("resolvePassword failed: %v", err)
	}

	if password != "test_password" {
		t.Errorf("Expected password 'test_password', got '%s'", password)
	}
}

func TestResolvePassword_FromTIGERFS_PASSWORD(t *testing.T) {
	// Clear PGPASSWORD and set TIGERFS_PASSWORD
	originalPGPASSWORD := os.Getenv("PGPASSWORD")
	originalTIGERFS := os.Getenv("TIGERFS_PASSWORD")
	defer func() {
		if originalPGPASSWORD != "" {
			os.Setenv("PGPASSWORD", originalPGPASSWORD)
		} else {
			os.Unsetenv("PGPASSWORD")
		}
		if originalTIGERFS != "" {
			os.Setenv("TIGERFS_PASSWORD", originalTIGERFS)
		} else {
			os.Unsetenv("TIGERFS_PASSWORD")
		}
	}()

	os.Unsetenv("PGPASSWORD")
	os.Setenv("TIGERFS_PASSWORD", "tigerfs_test_password")

	cfg := &config.Config{}
	ctx := context.Background()

	password, err := resolvePassword(ctx, cfg, "postgres://user@localhost/db")
	if err != nil {
		t.Fatalf("resolvePassword failed: %v", err)
	}

	if password != "tigerfs_test_password" {
		t.Errorf("Expected password 'tigerfs_test_password', got '%s'", password)
	}
}

func TestResolvePassword_PGPASSWORD_HasPrecedence(t *testing.T) {
	// Set both environment variables
	originalPGPASSWORD := os.Getenv("PGPASSWORD")
	originalTIGERFS := os.Getenv("TIGERFS_PASSWORD")
	defer func() {
		if originalPGPASSWORD != "" {
			os.Setenv("PGPASSWORD", originalPGPASSWORD)
		} else {
			os.Unsetenv("PGPASSWORD")
		}
		if originalTIGERFS != "" {
			os.Setenv("TIGERFS_PASSWORD", originalTIGERFS)
		} else {
			os.Unsetenv("TIGERFS_PASSWORD")
		}
	}()

	os.Setenv("PGPASSWORD", "pgpassword_wins")
	os.Setenv("TIGERFS_PASSWORD", "tigerfs_loses")

	cfg := &config.Config{}
	ctx := context.Background()

	password, err := resolvePassword(ctx, cfg, "postgres://user@localhost/db")
	if err != nil {
		t.Fatalf("resolvePassword failed: %v", err)
	}

	if password != "pgpassword_wins" {
		t.Errorf("Expected PGPASSWORD to take precedence, got '%s'", password)
	}
}

func TestResolvePassword_NoPasswordSource(t *testing.T) {
	// Clear all password sources
	originalPGPASSWORD := os.Getenv("PGPASSWORD")
	originalTIGERFS := os.Getenv("TIGERFS_PASSWORD")
	defer func() {
		if originalPGPASSWORD != "" {
			os.Setenv("PGPASSWORD", originalPGPASSWORD)
		}
		if originalTIGERFS != "" {
			os.Setenv("TIGERFS_PASSWORD", originalTIGERFS)
		}
	}()

	os.Unsetenv("PGPASSWORD")
	os.Unsetenv("TIGERFS_PASSWORD")

	cfg := &config.Config{}
	ctx := context.Background()

	password, err := resolvePassword(ctx, cfg, "postgres://user@localhost/db")
	if err != nil {
		t.Fatalf("resolvePassword failed: %v", err)
	}

	// Should return empty string, allowing pgx to check .pgpass
	if password != "" {
		t.Errorf("Expected empty password (pgx will check .pgpass), got '%s'", password)
	}
}

func TestResolvePassword_PasswordCommand(t *testing.T) {
	// Clear environment variables
	originalPGPASSWORD := os.Getenv("PGPASSWORD")
	originalTIGERFS := os.Getenv("TIGERFS_PASSWORD")
	defer func() {
		if originalPGPASSWORD != "" {
			os.Setenv("PGPASSWORD", originalPGPASSWORD)
		}
		if originalTIGERFS != "" {
			os.Setenv("TIGERFS_PASSWORD", originalTIGERFS)
		}
	}()

	os.Unsetenv("PGPASSWORD")
	os.Unsetenv("TIGERFS_PASSWORD")

	// Use echo command to simulate password_command
	cfg := &config.Config{
		PasswordCommand: "echo my_secret_password",
	}
	ctx := context.Background()

	password, err := resolvePassword(ctx, cfg, "postgres://user@localhost/db")
	if err != nil {
		t.Fatalf("resolvePassword failed: %v", err)
	}

	if password != "my_secret_password" {
		t.Errorf("Expected password 'my_secret_password', got '%s'", password)
	}
}

func TestResolvePassword_PasswordCommand_EnvVarHasPrecedence(t *testing.T) {
	// Set PGPASSWORD and password_command
	originalPGPASSWORD := os.Getenv("PGPASSWORD")
	defer func() {
		if originalPGPASSWORD != "" {
			os.Setenv("PGPASSWORD", originalPGPASSWORD)
		} else {
			os.Unsetenv("PGPASSWORD")
		}
	}()

	os.Setenv("PGPASSWORD", "env_var_wins")

	cfg := &config.Config{
		PasswordCommand: "echo command_loses",
	}
	ctx := context.Background()

	password, err := resolvePassword(ctx, cfg, "postgres://user@localhost/db")
	if err != nil {
		t.Fatalf("resolvePassword failed: %v", err)
	}

	// Environment variable should take precedence over password_command
	if password != "env_var_wins" {
		t.Errorf("Expected env var to take precedence, got '%s'", password)
	}
}

func TestExecutePasswordCommand_Success(t *testing.T) {
	ctx := context.Background()

	password, err := executePasswordCommand(ctx, "echo test_output")
	if err != nil {
		t.Fatalf("executePasswordCommand failed: %v", err)
	}

	if password != "test_output" {
		t.Errorf("Expected 'test_output', got '%s'", password)
	}
}

func TestExecutePasswordCommand_EmptyOutput(t *testing.T) {
	ctx := context.Background()

	_, err := executePasswordCommand(ctx, "echo")
	if err == nil {
		t.Fatal("Expected error for empty output, got nil")
	}

	if err.Error() != "password_command returned empty output" {
		t.Errorf("Expected 'password_command returned empty output', got '%s'", err.Error())
	}
}

func TestExecutePasswordCommand_NonZeroExit(t *testing.T) {
	ctx := context.Background()

	_, err := executePasswordCommand(ctx, "sh -c 'exit 1'")
	if err == nil {
		t.Fatal("Expected error for non-zero exit, got nil")
	}
}

func TestInjectPasswordIntoConnStr_PostgresURL(t *testing.T) {
	tests := []struct {
		name     string
		connStr  string
		password string
		expected string
	}{
		{
			name:     "Simple URL without password",
			connStr:  "postgres://user@localhost/db",
			password: "secret",
			expected: "postgres://user:secret@localhost/db",
		},
		{
			name:     "URL with port",
			connStr:  "postgres://user@localhost:5432/db",
			password: "secret",
			expected: "postgres://user:secret@localhost:5432/db",
		},
		{
			name:     "URL with existing password",
			connStr:  "postgres://user:oldpass@localhost/db",
			password: "newpass",
			expected: "postgres://user:newpass@localhost/db",
		},
		{
			name:     "URL with query parameters",
			connStr:  "postgres://user@localhost/db?sslmode=disable",
			password: "secret",
			expected: "postgres://user:secret@localhost/db?sslmode=disable",
		},
		{
			name:     "Empty password",
			connStr:  "postgres://user@localhost/db",
			password: "",
			expected: "postgres://user@localhost/db",
		},
		{
			name:     "PostgreSQL URL variant",
			connStr:  "postgresql://user@localhost/db",
			password: "secret",
			expected: "postgresql://user:secret@localhost/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := injectPasswordIntoConnStr(tt.connStr, tt.password)
			if err != nil {
				t.Fatalf("injectPasswordIntoConnStr failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestInjectPasswordIntoConnStr_KeyValue(t *testing.T) {
	tests := []struct {
		name     string
		connStr  string
		password string
		expected string
	}{
		{
			name:     "Key-value format without password",
			connStr:  "host=localhost user=myuser dbname=mydb",
			password: "secret",
			expected: "host=localhost user=myuser dbname=mydb password=secret",
		},
		{
			name:     "Key-value format with existing password",
			connStr:  "host=localhost user=myuser dbname=mydb password=oldpass",
			password: "newpass",
			expected: "host=localhost user=myuser dbname=mydb password=oldpass",
		},
		{
			name:     "Empty password",
			connStr:  "host=localhost user=myuser dbname=mydb",
			password: "",
			expected: "host=localhost user=myuser dbname=mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := injectPasswordIntoConnStr(tt.connStr, tt.password)
			if err != nil {
				t.Fatalf("injectPasswordIntoConnStr failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}
