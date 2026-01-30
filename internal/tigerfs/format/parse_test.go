package format

import (
	"testing"
)

// TSV Parsing Tests

func TestParseTSV_Basic(t *testing.T) {
	line := "value1\tvalue2\tvalue3"

	columns, values, err := ParseTSV(line)
	if err != nil {
		t.Fatalf("ParseTSV failed: %v", err)
	}

	// TSV doesn't return column names
	if columns != nil {
		t.Errorf("Expected nil columns, got %v", columns)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value1" {
		t.Errorf("Expected values[0]='value1', got %v", values[0])
	}
	if values[1] != "value2" {
		t.Errorf("Expected values[1]='value2', got %v", values[1])
	}
	if values[2] != "value3" {
		t.Errorf("Expected values[2]='value3', got %v", values[2])
	}
}

func TestParseTSV_EmptyFields(t *testing.T) {
	// Empty fields should be treated as NULL
	line := "value1\t\tvalue3"

	_, values, err := ParseTSV(line)
	if err != nil {
		t.Fatalf("ParseTSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value1" {
		t.Errorf("Expected values[0]='value1', got %v", values[0])
	}
	if values[1] != nil {
		t.Errorf("Expected values[1]=nil (NULL), got %v", values[1])
	}
	if values[2] != "value3" {
		t.Errorf("Expected values[2]='value3', got %v", values[2])
	}
}

func TestParseTSV_AllEmptyFields(t *testing.T) {
	line := "\t\t"

	_, values, err := ParseTSV(line)
	if err != nil {
		t.Fatalf("ParseTSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	for i, val := range values {
		if val != nil {
			t.Errorf("Expected values[%d]=nil, got %v", i, val)
		}
	}
}

func TestParseTSV_SingleValue(t *testing.T) {
	line := "singlevalue"

	_, values, err := ParseTSV(line)
	if err != nil {
		t.Fatalf("ParseTSV failed: %v", err)
	}

	if len(values) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(values))
	}

	if values[0] != "singlevalue" {
		t.Errorf("Expected values[0]='singlevalue', got %v", values[0])
	}
}

func TestParseTSV_EmptyString(t *testing.T) {
	line := ""

	_, values, err := ParseTSV(line)
	if err != nil {
		t.Fatalf("ParseTSV failed: %v", err)
	}

	// Empty string splits to one empty element
	if len(values) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(values))
	}

	if values[0] != nil {
		t.Errorf("Expected values[0]=nil, got %v", values[0])
	}
}

func TestParseTSV_SpecialCharacters(t *testing.T) {
	// Test with newlines, quotes, etc. in values
	line := "value with spaces\tvalue\"with\"quotes\tvalue\nwith\nnewlines"

	_, values, err := ParseTSV(line)
	if err != nil {
		t.Fatalf("ParseTSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value with spaces" {
		t.Errorf("Expected values[0]='value with spaces', got %v", values[0])
	}
	if values[1] != "value\"with\"quotes" {
		t.Errorf("Expected values[1]='value\"with\"quotes', got %v", values[1])
	}
	if values[2] != "value\nwith\nnewlines" {
		t.Errorf("Expected values[2] with newlines, got %v", values[2])
	}
}

// CSV Parsing Tests

func TestParseCSV_Basic(t *testing.T) {
	line := "value1,value2,value3"

	columns, values, err := ParseCSV(line)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	// CSV doesn't return column names
	if columns != nil {
		t.Errorf("Expected nil columns, got %v", columns)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value1" {
		t.Errorf("Expected values[0]='value1', got %v", values[0])
	}
	if values[1] != "value2" {
		t.Errorf("Expected values[1]='value2', got %v", values[1])
	}
	if values[2] != "value3" {
		t.Errorf("Expected values[2]='value3', got %v", values[2])
	}
}

func TestParseCSV_QuotedFields(t *testing.T) {
	line := `"value1","value2","value3"`

	_, values, err := ParseCSV(line)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value1" {
		t.Errorf("Expected values[0]='value1', got %v", values[0])
	}
	if values[1] != "value2" {
		t.Errorf("Expected values[1]='value2', got %v", values[1])
	}
	if values[2] != "value3" {
		t.Errorf("Expected values[2]='value3', got %v", values[2])
	}
}

func TestParseCSV_CommasInQuotes(t *testing.T) {
	line := `value1,"value2,with,commas",value3`

	_, values, err := ParseCSV(line)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value1" {
		t.Errorf("Expected values[0]='value1', got %v", values[0])
	}
	if values[1] != "value2,with,commas" {
		t.Errorf("Expected values[1]='value2,with,commas', got %v", values[1])
	}
	if values[2] != "value3" {
		t.Errorf("Expected values[2]='value3', got %v", values[2])
	}
}

func TestParseCSV_EmptyFields(t *testing.T) {
	// Empty fields should be treated as NULL
	line := "value1,,value3"

	_, values, err := ParseCSV(line)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value1" {
		t.Errorf("Expected values[0]='value1', got %v", values[0])
	}
	if values[1] != nil {
		t.Errorf("Expected values[1]=nil (NULL), got %v", values[1])
	}
	if values[2] != "value3" {
		t.Errorf("Expected values[2]='value3', got %v", values[2])
	}
}

func TestParseCSV_QuotedEmptyField(t *testing.T) {
	// Quoted empty string is different from unquoted empty (NULL)
	line := `value1,"",value3`

	_, values, err := ParseCSV(line)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value1" {
		t.Errorf("Expected values[0]='value1', got %v", values[0])
	}
	// Quoted empty string becomes nil after our conversion
	if values[1] != nil {
		t.Errorf("Expected values[1]=nil, got %v", values[1])
	}
	if values[2] != "value3" {
		t.Errorf("Expected values[2]='value3', got %v", values[2])
	}
}

func TestParseCSV_NewlinesInQuotes(t *testing.T) {
	line := "value1,\"value2\nwith\nnewlines\",value3"

	_, values, err := ParseCSV(line)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[0] != "value1" {
		t.Errorf("Expected values[0]='value1', got %v", values[0])
	}
	if values[1] != "value2\nwith\nnewlines" {
		t.Errorf("Expected values[1] with newlines, got %v", values[1])
	}
	if values[2] != "value3" {
		t.Errorf("Expected values[2]='value3', got %v", values[2])
	}
}

func TestParseCSV_QuotesInQuotes(t *testing.T) {
	// Escaped quotes (doubled) inside quoted field
	line := `value1,"value2""with""quotes",value3`

	_, values, err := ParseCSV(line)
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	if values[1] != `value2"with"quotes` {
		t.Errorf("Expected values[1]='value2\"with\"quotes', got %v", values[1])
	}
}

func TestParseCSV_MalformedInput(t *testing.T) {
	// Unclosed quote
	line := `value1,"value2,value3`

	_, _, err := ParseCSV(line)
	if err == nil {
		t.Error("Expected error for malformed CSV with unclosed quote")
	}
}

func TestParseCSV_EmptyString(t *testing.T) {
	line := ""

	_, _, err := ParseCSV(line)
	// Go's csv.Reader returns EOF for empty string
	if err == nil {
		t.Error("Expected error for empty CSV string")
	}
}

// JSON Parsing Tests

func TestParseJSON_Basic(t *testing.T) {
	jsonStr := `{"col1": "value1", "col2": "value2", "col3": "value3"}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(columns))
	}

	if len(values) != 3 {
		t.Fatalf("Expected 3 values, got %d", len(values))
	}

	// JSON parsing order is not guaranteed, so we need to find each column
	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	if colMap["col1"] != "value1" {
		t.Errorf("Expected col1='value1', got %v", colMap["col1"])
	}
	if colMap["col2"] != "value2" {
		t.Errorf("Expected col2='value2', got %v", colMap["col2"])
	}
	if colMap["col3"] != "value3" {
		t.Errorf("Expected col3='value3', got %v", colMap["col3"])
	}
}

func TestParseJSON_NullValues(t *testing.T) {
	jsonStr := `{"col1": "value1", "col2": null, "col3": "value3"}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(columns))
	}

	// Build map for easy checking
	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	if colMap["col1"] != "value1" {
		t.Errorf("Expected col1='value1', got %v", colMap["col1"])
	}
	if colMap["col2"] != nil {
		t.Errorf("Expected col2=nil, got %v", colMap["col2"])
	}
	if colMap["col3"] != "value3" {
		t.Errorf("Expected col3='value3', got %v", colMap["col3"])
	}
}

func TestParseJSON_Numbers(t *testing.T) {
	jsonStr := `{"id": 123, "price": 45.67, "count": -10}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(columns))
	}

	// Build map
	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	// JSON numbers are parsed as float64
	if id, ok := colMap["id"].(float64); !ok || id != 123 {
		t.Errorf("Expected id=123 (float64), got %v (%T)", colMap["id"], colMap["id"])
	}
	if price, ok := colMap["price"].(float64); !ok || price != 45.67 {
		t.Errorf("Expected price=45.67, got %v", colMap["price"])
	}
	if count, ok := colMap["count"].(float64); !ok || count != -10 {
		t.Errorf("Expected count=-10, got %v", colMap["count"])
	}
}

func TestParseJSON_Boolean(t *testing.T) {
	jsonStr := `{"active": true, "deleted": false}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	if active, ok := colMap["active"].(bool); !ok || !active {
		t.Errorf("Expected active=true, got %v", colMap["active"])
	}
	if deleted, ok := colMap["deleted"].(bool); !ok || deleted {
		t.Errorf("Expected deleted=false, got %v", colMap["deleted"])
	}
}

func TestParseJSON_Arrays(t *testing.T) {
	jsonStr := `{"tags": ["tag1", "tag2", "tag3"]}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	if len(columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(columns))
	}

	if columns[0] != "tags" {
		t.Errorf("Expected column 'tags', got %v", columns[0])
	}

	// Array should be preserved as []interface{}
	arr, ok := values[0].([]interface{})
	if !ok {
		t.Fatalf("Expected array value, got %T", values[0])
	}

	if len(arr) != 3 {
		t.Errorf("Expected array length 3, got %d", len(arr))
	}
}

func TestParseJSON_NestedObjects(t *testing.T) {
	jsonStr := `{"user": {"name": "Alice", "age": 30}}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	if len(columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(columns))
	}

	// Nested object should be preserved as map[string]interface{}
	obj, ok := values[0].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected nested object, got %T", values[0])
	}

	if obj["name"] != "Alice" {
		t.Errorf("Expected nested name='Alice', got %v", obj["name"])
	}
}

func TestParseJSON_InvalidJSON(t *testing.T) {
	// Missing closing brace
	jsonStr := `{"col1": "value1", "col2": "value2"`

	_, _, err := ParseJSON(jsonStr)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestParseJSON_InvalidJSON_TrailingComma(t *testing.T) {
	jsonStr := `{"col1": "value1", "col2": "value2",}`

	_, _, err := ParseJSON(jsonStr)
	if err == nil {
		t.Error("Expected error for JSON with trailing comma")
	}
}

func TestParseJSON_EmptyObject(t *testing.T) {
	jsonStr := `{}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	if len(columns) != 0 {
		t.Errorf("Expected 0 columns, got %d", len(columns))
	}

	if len(values) != 0 {
		t.Errorf("Expected 0 values, got %d", len(values))
	}
}

func TestParseJSON_NotAnObject(t *testing.T) {
	// Array instead of object
	jsonStr := `["value1", "value2"]`

	_, _, err := ParseJSON(jsonStr)
	if err == nil {
		t.Error("Expected error for JSON array instead of object")
	}
}

func TestParseJSON_StringifiedJSON(t *testing.T) {
	// JSON value that is a JSON string (JSONB column case)
	jsonStr := `{"data": "{\"nested\": \"value\"}"}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	// Should be a string, not parsed
	if dataStr, ok := colMap["data"].(string); !ok {
		t.Errorf("Expected data to be string, got %T", colMap["data"])
	} else if dataStr != `{"nested": "value"}` {
		t.Errorf("Expected nested JSON as string, got %v", dataStr)
	}
}

func TestParseJSON_SpecialCharacters(t *testing.T) {
	jsonStr := `{"text": "line1\nline2\ttab"}`

	columns, values, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}

	colMap := make(map[string]interface{})
	for i, col := range columns {
		colMap[col] = values[i]
	}

	expected := "line1\nline2\ttab"
	if colMap["text"] != expected {
		t.Errorf("Expected text=%q, got %v", expected, colMap["text"])
	}
}

// =============================================================================
// No-Headers Bulk Parsing Tests
// =============================================================================

func TestParseCSVBulkNoHeaders_Basic(t *testing.T) {
	columns := []string{"id", "name", "email"}
	data := []byte("1,Alice,alice@example.com\n2,Bob,bob@example.com\n")

	resultColumns, rows, err := ParseCSVBulkNoHeaders(data, columns)
	if err != nil {
		t.Fatalf("ParseCSVBulkNoHeaders failed: %v", err)
	}

	// Should return the provided columns
	if len(resultColumns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(resultColumns))
	}
	for i, col := range columns {
		if resultColumns[i] != col {
			t.Errorf("Expected column %d to be %q, got %q", i, col, resultColumns[i])
		}
	}

	// Should have 2 rows
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	// Check first row
	if rows[0][0] != "1" {
		t.Errorf("Expected rows[0][0]='1', got %v", rows[0][0])
	}
	if rows[0][1] != "Alice" {
		t.Errorf("Expected rows[0][1]='Alice', got %v", rows[0][1])
	}
	if rows[0][2] != "alice@example.com" {
		t.Errorf("Expected rows[0][2]='alice@example.com', got %v", rows[0][2])
	}

	// Check second row
	if rows[1][0] != "2" {
		t.Errorf("Expected rows[1][0]='2', got %v", rows[1][0])
	}
	if rows[1][1] != "Bob" {
		t.Errorf("Expected rows[1][1]='Bob', got %v", rows[1][1])
	}
}

func TestParseCSVBulkNoHeaders_EmptyData(t *testing.T) {
	columns := []string{"id", "name"}
	data := []byte("")

	resultColumns, rows, err := ParseCSVBulkNoHeaders(data, columns)
	if err != nil {
		t.Fatalf("ParseCSVBulkNoHeaders failed: %v", err)
	}

	if len(resultColumns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(resultColumns))
	}
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(rows))
	}
}

func TestParseCSVBulkNoHeaders_ColumnMismatch(t *testing.T) {
	columns := []string{"id", "name"}
	data := []byte("1,Alice,extra\n")

	_, _, err := ParseCSVBulkNoHeaders(data, columns)
	if err == nil {
		t.Error("Expected error for column count mismatch")
	}
}

func TestParseCSVBulkNoHeaders_NullValues(t *testing.T) {
	columns := []string{"id", "name", "email"}
	data := []byte("1,Alice,\n2,,bob@example.com\n")

	_, rows, err := ParseCSVBulkNoHeaders(data, columns)
	if err != nil {
		t.Fatalf("ParseCSVBulkNoHeaders failed: %v", err)
	}

	// First row: email is NULL
	if rows[0][2] != nil {
		t.Errorf("Expected rows[0][2]=nil, got %v", rows[0][2])
	}

	// Second row: name is NULL
	if rows[1][1] != nil {
		t.Errorf("Expected rows[1][1]=nil, got %v", rows[1][1])
	}
}

func TestParseTSVBulkNoHeaders_Basic(t *testing.T) {
	columns := []string{"id", "name", "email"}
	data := []byte("1\tAlice\talice@example.com\n2\tBob\tbob@example.com\n")

	resultColumns, rows, err := ParseTSVBulkNoHeaders(data, columns)
	if err != nil {
		t.Fatalf("ParseTSVBulkNoHeaders failed: %v", err)
	}

	// Should return the provided columns
	if len(resultColumns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(resultColumns))
	}

	// Should have 2 rows
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	// Check first row
	if rows[0][0] != "1" {
		t.Errorf("Expected rows[0][0]='1', got %v", rows[0][0])
	}
	if rows[0][1] != "Alice" {
		t.Errorf("Expected rows[0][1]='Alice', got %v", rows[0][1])
	}
}

func TestParseTSVBulkNoHeaders_EmptyData(t *testing.T) {
	columns := []string{"id", "name"}
	data := []byte("")

	resultColumns, rows, err := ParseTSVBulkNoHeaders(data, columns)
	if err != nil {
		t.Fatalf("ParseTSVBulkNoHeaders failed: %v", err)
	}

	if len(resultColumns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(resultColumns))
	}
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(rows))
	}
}

func TestParseTSVBulkNoHeaders_ColumnMismatch(t *testing.T) {
	columns := []string{"id", "name"}
	data := []byte("1\tAlice\textra\n")

	_, _, err := ParseTSVBulkNoHeaders(data, columns)
	if err == nil {
		t.Error("Expected error for column count mismatch")
	}
}

func TestParseTSVBulkNoHeaders_NullValues(t *testing.T) {
	columns := []string{"id", "name", "email"}
	data := []byte("1\tAlice\t\n2\t\tbob@example.com\n")

	_, rows, err := ParseTSVBulkNoHeaders(data, columns)
	if err != nil {
		t.Fatalf("ParseTSVBulkNoHeaders failed: %v", err)
	}

	// First row: email is NULL
	if rows[0][2] != nil {
		t.Errorf("Expected rows[0][2]=nil, got %v", rows[0][2])
	}

	// Second row: name is NULL
	if rows[1][1] != nil {
		t.Errorf("Expected rows[1][1]=nil, got %v", rows[1][1])
	}
}

func TestParseTSVBulkNoHeaders_SkipsEmptyLines(t *testing.T) {
	columns := []string{"id", "name"}
	data := []byte("1\tAlice\n\n2\tBob\n")

	_, rows, err := ParseTSVBulkNoHeaders(data, columns)
	if err != nil {
		t.Fatalf("ParseTSVBulkNoHeaders failed: %v", err)
	}

	// Should have 2 rows (empty line skipped)
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
}
