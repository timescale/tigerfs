package format

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// RowToYAML converts a database row to YAML format.
// Column names are used as keys, producing self-documenting output.
// NULL values are represented as YAML null.
// Output includes leading document separator (---) for proper multi-document concatenation.
//
// Example output:
//
//	---
//	id: 1
//	name: Alice
//	email: alice@example.com
//	created_at: null
func RowToYAML(columns []string, values []interface{}) ([]byte, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("column count (%d) does not match value count (%d)", len(columns), len(values))
	}

	// Build ordered map for YAML output
	// Use yaml.Node to preserve key order
	var doc yaml.Node
	doc.Kind = yaml.MappingNode

	for i, col := range columns {
		// Add key node
		keyNode := &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: col,
		}

		// Add value node
		var valueNode *yaml.Node
		if values[i] == nil {
			valueNode = &yaml.Node{
				Kind:  yaml.ScalarNode,
				Tag:   "!!null",
				Value: "null",
			}
		} else {
			str, err := ConvertValueToText(values[i])
			if err != nil {
				return nil, fmt.Errorf("failed to convert column %s: %w", col, err)
			}
			valueNode = &yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: str,
			}
		}

		doc.Content = append(doc.Content, keyNode, valueNode)
	}

	data, err := yaml.Marshal(&doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal YAML: %w", err)
	}

	// Prepend document separator for proper multi-document concatenation
	result := append([]byte("---\n"), data...)
	return result, nil
}
