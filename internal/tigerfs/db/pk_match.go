package db

import (
	"fmt"
	"strings"
)

// PKMatch holds the column names and values needed to identify a specific row
// in a table, supporting both single-column and composite primary keys.
//
// Created by PrimaryKey.Decode() from a filesystem directory name, and used
// by DB methods (GetRow, UpdateRow, DeleteRow, etc.) to build WHERE clauses.
type PKMatch struct {
	Columns []string // Column names in PK definition order
	Values  []string // Corresponding values as strings
}

// SinglePKMatch creates a PKMatch for a single-column primary key.
// Used by legacy FUSE code that still works with single PK columns.
func SinglePKMatch(column, value string) *PKMatch {
	return &PKMatch{
		Columns: []string{column},
		Values:  []string{value},
	}
}

// WhereClause generates a SQL WHERE clause fragment for this PKMatch.
// The startParam argument specifies the first $N placeholder number.
//
// Examples:
//
//	single-column:  `"id" = $1`
//	composite:      `"customer_id" = $1 AND "product_id" = $2`
func (m *PKMatch) WhereClause(startParam int) string {
	parts := make([]string, len(m.Columns))
	for i, col := range m.Columns {
		parts[i] = fmt.Sprintf("%s = $%d", qi(col), startParam+i)
	}
	return strings.Join(parts, " AND ")
}

// WhereArgs returns the PK values as []interface{} for use as query parameters.
func (m *PKMatch) WhereArgs() []interface{} {
	args := make([]interface{}, len(m.Values))
	for i, v := range m.Values {
		args[i] = v
	}
	return args
}

// ParamCount returns the number of SQL parameters this PKMatch uses.
func (m *PKMatch) ParamCount() int {
	return len(m.Columns)
}

// pkEncode encodes a single PK value for use in a filesystem directory name.
// Escapes characters that are problematic in filenames or as composite PK delimiters:
//   - ',' -> "%2C" (composite PK delimiter)
//   - '%' -> "%25" (escape character itself)
//   - '/' -> "%2F" (path separator, invalid in filenames)
//   - '\x00' -> "%00" (null byte, invalid in filenames)
func pkEncode(value string) string {
	// Fast path: no special characters
	needsEncoding := false
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case ',', '%', '/', 0:
			needsEncoding = true
		}
		if needsEncoding {
			break
		}
	}
	if !needsEncoding {
		return value
	}

	var sb strings.Builder
	sb.Grow(len(value) + 6) // small extra for escapes
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '%':
			sb.WriteString("%25")
		case ',':
			sb.WriteString("%2C")
		case '/':
			sb.WriteString("%2F")
		case 0:
			sb.WriteString("%00")
		default:
			sb.WriteByte(value[i])
		}
	}
	return sb.String()
}

// pkDecode decodes a single PK value from a filesystem directory name,
// reversing the encoding applied by pkEncode.
func pkDecode(encoded string) string {
	// Fast path: no percent signs
	if !strings.Contains(encoded, "%") {
		return encoded
	}

	var sb strings.Builder
	sb.Grow(len(encoded))
	for i := 0; i < len(encoded); i++ {
		if encoded[i] == '%' && i+2 < len(encoded) {
			hex := encoded[i+1 : i+3]
			switch hex {
			case "25":
				sb.WriteByte('%')
			case "2C", "2c":
				sb.WriteByte(',')
			case "2F", "2f":
				sb.WriteByte('/')
			case "00":
				sb.WriteByte(0)
			default:
				// Unknown escape sequence, pass through as-is
				sb.WriteByte('%')
				continue
			}
			i += 2
		} else {
			sb.WriteByte(encoded[i])
		}
	}
	return sb.String()
}

// Encode converts PK values into a filesystem directory name.
//
// For single-column PKs, the value is returned with only filesystem-unsafe
// characters encoded (/, null byte). Commas and percent signs are NOT encoded
// for single-column PKs, preserving backward compatibility.
//
// For multi-column PKs, values are comma-delimited with full encoding of
// commas, percent signs, slashes, and null bytes in individual values.
//
// Examples:
//
//	single-column:  Encode(["123"])         -> "123"
//	single-column:  Encode(["path/to"])     -> "path%2Fto"
//	composite:      Encode(["5", "42"])     -> "5,42"
//	composite:      Encode(["a,b", "42"])   -> "a%2Cb,42"
func (pk *PrimaryKey) Encode(values []string) string {
	if len(pk.Columns) == 1 && len(values) == 1 {
		// Single-column: only encode filesystem-unsafe characters.
		// Do NOT encode commas or percent signs -- backward compatible.
		return pkEncodeSingle(values[0])
	}
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = pkEncode(v)
	}
	return strings.Join(parts, ",")
}

// pkEncodeSingle encodes a single-column PK value, only escaping characters
// that are invalid in FUSE/NFS filenames (slash and null byte).
// Commas and percent signs are left as-is for backward compatibility.
func pkEncodeSingle(value string) string {
	needsEncoding := false
	for i := 0; i < len(value); i++ {
		if value[i] == '/' || value[i] == 0 {
			needsEncoding = true
			break
		}
	}
	if !needsEncoding {
		return value
	}

	var sb strings.Builder
	sb.Grow(len(value) + 6)
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '/':
			sb.WriteString("%2F")
		case 0:
			sb.WriteString("%00")
		default:
			sb.WriteByte(value[i])
		}
	}
	return sb.String()
}

// Decode converts a filesystem directory name back into a PKMatch with
// column names and values.
//
// For single-column PKs, the directory name is used as-is (only
// filesystem-unsafe characters are decoded). For multi-column PKs, the name
// is split on commas and each part is URL-decoded.
//
// Known limitation: for single-column text PKs, values containing literal
// commas are returned as-is (no comma splitting). This means single-column
// PK encoding is fully backward compatible.
func (pk *PrimaryKey) Decode(dirname string) (*PKMatch, error) {
	if len(pk.Columns) == 1 {
		// Single-column: decode only filesystem-unsafe characters.
		return &PKMatch{
			Columns: pk.Columns,
			Values:  []string{pkDecodeSingle(dirname)},
		}, nil
	}

	// Multi-column: split on commas, decode each part.
	parts := strings.Split(dirname, ",")
	if len(parts) != len(pk.Columns) {
		return nil, fmt.Errorf("composite PK %q has %d parts, expected %d columns (%v)",
			dirname, len(parts), len(pk.Columns), pk.Columns)
	}

	values := make([]string, len(parts))
	for i, part := range parts {
		values[i] = pkDecode(part)
	}

	return &PKMatch{
		Columns: pk.Columns,
		Values:  values,
	}, nil
}

// pkDecodeSingle decodes a single-column PK value, only unescaping
// filesystem-unsafe characters (slash and null byte).
func pkDecodeSingle(encoded string) string {
	if !strings.Contains(encoded, "%") {
		return encoded
	}

	var sb strings.Builder
	sb.Grow(len(encoded))
	for i := 0; i < len(encoded); i++ {
		if encoded[i] == '%' && i+2 < len(encoded) {
			hex := encoded[i+1 : i+3]
			switch hex {
			case "2F", "2f":
				sb.WriteByte('/')
			case "00":
				sb.WriteByte(0)
			default:
				// Unknown escape -- pass through as-is (backward compat)
				sb.WriteByte('%')
				continue
			}
			i += 2
		} else {
			sb.WriteByte(encoded[i])
		}
	}
	return sb.String()
}
