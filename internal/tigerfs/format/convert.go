package format

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ConvertValueToText converts a PostgreSQL value to its text representation
// Handles special PostgreSQL types like JSONB, arrays, and complex types
func ConvertValueToText(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}

	switch v := value.(type) {
	case string:
		return v, nil

	case []byte:
		// BYTEA or text data
		return string(v), nil

	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), nil

	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil

	case float32, float64:
		return fmt.Sprintf("%g", v), nil

	case bool:
		// Use PostgreSQL convention: t/f
		if v {
			return "t", nil
		}
		return "f", nil

	case time.Time:
		// ISO 8601 format with full precision (RFC3339Nano)
		// Must preserve sub-second precision for round-trip queries
		// (e.g., index navigation needs exact timestamp matching)
		return v.Format(time.RFC3339Nano), nil

	case map[string]interface{}:
		// JSONB object - return as compact JSON
		data, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to marshal JSONB object: %w", err)
		}
		return string(data), nil

	case []interface{}:
		// PostgreSQL array or JSONB array - return as JSON array
		data, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to marshal array: %w", err)
		}
		return string(data), nil

	case [16]byte:
		// UUID - pgx returns UUIDs as [16]byte arrays
		u, err := uuid.FromBytes(v[:])
		if err != nil {
			return "", fmt.Errorf("failed to parse UUID bytes: %w", err)
		}
		return u.String(), nil

	default:
		// Check if the value implements fmt.Stringer (e.g., pgtype.UUID)
		if s, ok := value.(fmt.Stringer); ok {
			return s.String(), nil
		}

		// For other types (including pgtype.Numeric), try JSON encoding.
		// pgtype.Numeric doesn't implement fmt.Stringer, but it has MarshalJSON
		// which json.Marshal calls to get the proper decimal representation
		// (e.g., "175.00" instead of the struct internals "{17500 -2 ...}")
		if data, err := json.Marshal(v); err == nil {
			return string(data), nil
		}

		// Fallback: use fmt to convert to string
		return fmt.Sprintf("%v", v), nil
	}
}

// NormalizeForJSON converts PostgreSQL-specific types to JSON-compatible types.
// Unlike ConvertValueToText, this preserves JSON-native types (strings, numbers, bools, nil)
// while converting problematic types like [16]byte UUIDs to strings.
func NormalizeForJSON(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case string, bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64:
		// JSON-native types - keep as-is
		return v

	case [16]byte:
		// UUID - convert to string
		u, err := uuid.FromBytes(v[:])
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return u.String()

	case []byte:
		// Could be text or binary - try as string
		return string(v)

	case time.Time:
		// Convert to ISO 8601 string
		return v.Format(time.RFC3339Nano)

	default:
		// Check if implements Stringer (e.g., pgtype.UUID)
		if s, ok := value.(fmt.Stringer); ok {
			return s.String()
		}
		// Return as-is and let json.Marshal handle it
		return v
	}
}

// BoolToPostgresText converts a boolean to PostgreSQL text format (t/f)
func BoolToPostgresText(value bool) string {
	if value {
		return "t"
	}
	return "f"
}

// BoolToStandardText converts a boolean to standard text format (true/false)
func BoolToStandardText(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

// TimeToText converts a time.Time to RFC3339Nano format (full precision)
func TimeToText(t time.Time) string {
	return t.Format(time.RFC3339Nano)
}

// JSONBToText converts a JSONB value to compact JSON text
func JSONBToText(value interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSONB: %w", err)
	}
	return string(data), nil
}

// ArrayToText converts a PostgreSQL array to JSON array text
func ArrayToText(value []interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("failed to marshal array: %w", err)
	}
	return string(data), nil
}
