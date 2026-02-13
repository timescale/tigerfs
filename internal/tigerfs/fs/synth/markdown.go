package synth

import (
	"fmt"
	"sort"
	"strings"

	"github.com/timescale/tigerfs/internal/tigerfs/format"
	"gopkg.in/yaml.v3"
)

// SynthesizeMarkdown generates a markdown file from a database row.
// The output has YAML frontmatter (from non-filename/body columns) followed
// by the body content. If there are no frontmatter columns, the delimiters
// are omitted.
//
// Example output:
//
//	---
//	author: alice
//	date: 2024-01-15
//	tags: [intro, welcome]
//	---
//
//	# Hello World
//
//	Content from the body column...
func SynthesizeMarkdown(columns []string, values []interface{}, roles *ColumnRoles) ([]byte, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("column count (%d) does not match value count (%d)", len(columns), len(values))
	}

	// Build column→value lookup
	colMap := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		colMap[col] = values[i]
	}

	var sb strings.Builder

	// Write frontmatter if there are frontmatter columns or extra headers
	hasFrontmatter := len(roles.Frontmatter) > 0
	hasExtraHeaders := roles.ExtraHeaders != "" && colMap[roles.ExtraHeaders] != nil
	if hasFrontmatter || hasExtraHeaders {
		fm, err := buildFrontmatter(colMap, roles.Frontmatter, roles.ExtraHeaders)
		if err != nil {
			return nil, fmt.Errorf("failed to build frontmatter: %w", err)
		}
		if len(fm) > 0 {
			sb.WriteString("---\n")
			sb.Write(fm)
			sb.WriteString("---\n")
		}
	}

	// Write body
	body := ValueToString(colMap[roles.Body])
	if body != "" {
		// Add blank line between frontmatter and body if frontmatter was written
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(body)
		// Ensure trailing newline
		if !strings.HasSuffix(body, "\n") {
			sb.WriteString("\n")
		}
	}

	return []byte(sb.String()), nil
}

// GetMarkdownFilename generates the .md filename for a row.
// Uses the filename column value, falling back to the primary key if NULL/empty.
// Ensures the result has a .md extension (avoids double .md).
func GetMarkdownFilename(columns []string, values []interface{}, roles *ColumnRoles) string {
	colMap := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		colMap[col] = values[i]
	}

	name := ValueToString(colMap[roles.Filename])
	if name == "" {
		// Fallback to PK value
		name = ValueToString(colMap[roles.PrimaryKey])
		if name == "" {
			name = "untitled"
		}
	}

	// Sanitize for filesystem
	name = sanitizeFilename(name)

	// Add .md extension (avoid double)
	if !strings.HasSuffix(strings.ToLower(name), ".md") {
		name += ".md"
	}

	return name
}

// buildFrontmatter generates YAML frontmatter from the given columns.
// Uses gopkg.in/yaml.v3 for proper YAML serialization.
// Known frontmatter columns are written first (in schema order), followed by
// extra headers from the JSONB column (sorted alphabetically).
// Returns empty slice if all values are nil/empty.
func buildFrontmatter(colMap map[string]interface{}, frontmatterCols []string, extraHeadersCol string) ([]byte, error) {
	// Build ordered map for YAML output (preserves column order)
	// Use yaml.v3 Node API for ordered output
	doc := &yaml.Node{
		Kind: yaml.MappingNode,
	}

	hasContent := false
	for _, col := range frontmatterCols {
		val := colMap[col]
		if val == nil {
			continue // Skip NULL values in frontmatter
		}

		hasContent = true

		// Key node
		keyNode := &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: col,
		}

		// Value node - convert DB value to YAML-friendly type
		valNode, err := valueToYAMLNode(val)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col, err)
		}

		doc.Content = append(doc.Content, keyNode, valNode)
	}

	// Append extra headers from JSONB column (alphabetically sorted)
	if extraHeadersCol != "" {
		if extras, ok := colMap[extraHeadersCol].(map[string]interface{}); ok && len(extras) > 0 {
			keys := make([]string, 0, len(extras))
			for k := range extras {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				v := extras[k]
				if v == nil {
					continue
				}
				hasContent = true

				keyNode := &yaml.Node{
					Kind:  yaml.ScalarNode,
					Value: k,
				}
				valNode, err := valueToYAMLNode(v)
				if err != nil {
					return nil, fmt.Errorf("extra header %s: %w", k, err)
				}
				doc.Content = append(doc.Content, keyNode, valNode)
			}
		}
	}

	if !hasContent {
		return nil, nil
	}

	data, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal frontmatter: %w", err)
	}

	return data, nil
}

// valueToYAMLNode converts a database value to a yaml.Node.
func valueToYAMLNode(val interface{}) (*yaml.Node, error) {
	// Handle booleans directly — format.ConvertValueToText uses PostgreSQL
	// convention (t/f) which doesn't round-trip through YAML correctly.
	if b, ok := val.(bool); ok {
		v := "false"
		if b {
			v = "true"
		}
		return &yaml.Node{
			Kind:  yaml.ScalarNode,
			Tag:   "!!bool",
			Value: v,
		}, nil
	}

	// Convert DB value to text first, then let YAML handle it
	text, err := format.ConvertValueToText(val)
	if err != nil {
		return nil, err
	}

	// For slices/maps (JSONB), marshal as structured YAML
	switch v := val.(type) {
	case []interface{}:
		node := &yaml.Node{Kind: yaml.SequenceNode}
		for _, item := range v {
			itemText, err := format.ConvertValueToText(item)
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, &yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: itemText,
			})
		}
		return node, nil

	case map[string]interface{}:
		node := &yaml.Node{Kind: yaml.MappingNode}
		for k, item := range v {
			itemText, err := format.ConvertValueToText(item)
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content,
				&yaml.Node{Kind: yaml.ScalarNode, Value: k},
				&yaml.Node{Kind: yaml.ScalarNode, Value: itemText},
			)
		}
		return node, nil
	}

	return &yaml.Node{
		Kind:  yaml.ScalarNode,
		Value: text,
	}, nil
}

// valueToString converts a database value to a string.
// Returns empty string for nil values.
func ValueToString(val interface{}) string {
	if val == nil {
		return ""
	}
	text, err := format.ConvertValueToText(val)
	if err != nil {
		return fmt.Sprintf("%v", val)
	}
	return text
}

// ParsedMarkdown holds the result of parsing a markdown file with YAML frontmatter.
type ParsedMarkdown struct {
	// Frontmatter contains the parsed YAML key-value pairs.
	// Keys are strings, values may be strings, numbers, booleans, arrays, or maps.
	Frontmatter map[string]interface{}

	// Body is the markdown content after the frontmatter delimiters.
	Body string
}

// ParseMarkdown splits a markdown file into frontmatter and body.
// Frontmatter is optional — if no --- delimiters are found, the entire
// content is treated as body.
func ParseMarkdown(content []byte) (*ParsedMarkdown, error) {
	text := string(content)
	result := &ParsedMarkdown{}

	// Check for frontmatter delimiters
	if !strings.HasPrefix(text, "---\n") && !strings.HasPrefix(text, "---\r\n") {
		// No frontmatter — entire content is body
		result.Body = text
		return result, nil
	}

	// Find closing delimiter
	// Skip the opening "---\n"
	rest := text[4:]
	closingIdx := strings.Index(rest, "\n---\n")
	if closingIdx == -1 {
		// Try with \r\n
		closingIdx = strings.Index(rest, "\r\n---\r\n")
		if closingIdx == -1 {
			// Check if it ends with \n---\n at EOF
			if strings.HasSuffix(rest, "\n---") {
				closingIdx = len(rest) - 4
			} else {
				// No closing delimiter — treat everything as body
				result.Body = text
				return result, nil
			}
		}
	}

	// Parse YAML frontmatter
	yamlContent := rest[:closingIdx]
	if err := yaml.Unmarshal([]byte(yamlContent), &result.Frontmatter); err != nil {
		return nil, fmt.Errorf("failed to parse YAML frontmatter: %w", err)
	}

	// Body is everything after the closing delimiter
	afterClosing := rest[closingIdx:]
	// Skip past the "\n---\n"
	bodyStart := strings.Index(afterClosing, "---")
	if bodyStart != -1 {
		bodyStart += 3 // skip "---"
		body := afterClosing[bodyStart:]
		// Trim leading newline
		body = strings.TrimPrefix(body, "\n")
		body = strings.TrimPrefix(body, "\r\n")
		// Trim one leading blank line (convention: blank line after ---)
		body = strings.TrimPrefix(body, "\n")
		body = strings.TrimPrefix(body, "\r\n")
		result.Body = body
	}

	return result, nil
}

// MapToColumns converts parsed markdown back to column values for database writes.
// Frontmatter keys are mapped to column names; the body maps to the body column.
// When ExtraHeaders is set, unknown frontmatter keys are collected into a map
// stored in that column (overwrite semantics — missing keys are removed).
// When ExtraHeaders is empty, unknown keys return an error.
func MapToColumns(parsed *ParsedMarkdown, roles *ColumnRoles) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// Set body column
	result[roles.Body] = parsed.Body

	// Map frontmatter to column values
	if parsed.Frontmatter != nil {
		// Build set of known frontmatter columns for validation
		knownCols := make(map[string]bool, len(roles.Frontmatter))
		for _, col := range roles.Frontmatter {
			knownCols[strings.ToLower(col)] = true
		}

		extraHeaders := make(map[string]interface{})
		for key, val := range parsed.Frontmatter {
			if knownCols[strings.ToLower(key)] {
				result[key] = val
			} else if roles.ExtraHeaders != "" {
				extraHeaders[key] = val
			} else {
				return nil, fmt.Errorf("unknown frontmatter key %q (valid keys: %s)",
					key, strings.Join(roles.Frontmatter, ", "))
			}
		}

		// Always set extra headers (overwrite semantics: removed keys disappear)
		if roles.ExtraHeaders != "" {
			result[roles.ExtraHeaders] = extraHeaders
		}
	} else if roles.ExtraHeaders != "" {
		// No frontmatter at all → empty extra headers
		result[roles.ExtraHeaders] = map[string]interface{}{}
	}

	return result, nil
}

// sanitizeFilename replaces characters invalid in filenames.
// Slashes are preserved to support hierarchical directory paths in synth views.
func sanitizeFilename(name string) string {
	var sb strings.Builder
	for _, r := range name {
		switch r {
		case '\\', '\x00', ':':
			sb.WriteRune('-')
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}
