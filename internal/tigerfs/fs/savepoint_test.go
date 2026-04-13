package fs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// --- readDirSavepoint unit tests ---

func TestReadDirSavepoint_PassesAscendingForFirst(t *testing.T) {
	parsed, err := ParsePath("/notes/.savepoint/.first/3")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, parsed.Type)
	assert.Equal(t, LimitFirst, parsed.Context.LimitType)
	assert.Equal(t, 3, parsed.Context.Limit)
}

func TestReadDirSavepoint_PassesDescendingForLast(t *testing.T) {
	parsed, err := ParsePath("/notes/.savepoint/.last/5")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, parsed.Type)
	assert.Equal(t, LimitLast, parsed.Context.LimitType)
	assert.Equal(t, 5, parsed.Context.Limit)
}

func TestReadDirSavepoint_PreservesTypeWithFilter(t *testing.T) {
	parsed, err := ParsePath("/notes/.savepoint/.by/user_id/agent-7")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, parsed.Type)
	require.Len(t, parsed.Context.Filters, 1)
	assert.Equal(t, "user_id", parsed.Context.Filters[0].Column)
	assert.Equal(t, "agent-7", parsed.Context.Filters[0].Value)
}

func TestReadDirSavepoint_PreservesTypeWithFilterAndLimit(t *testing.T) {
	parsed, err := ParsePath("/notes/.savepoint/.by/user_id/agent-7/.last/3")
	require.Nil(t, err)
	assert.Equal(t, PathSavepoint, parsed.Type)
	assert.Equal(t, LimitLast, parsed.Context.LimitType)
	assert.Equal(t, 3, parsed.Context.Limit)
}

// --- writeSavepoint unit tests (name as PK, format suffix required) ---

// Helper to extract column map from inserted row
func insertedColMap(t *testing.T, mockDB *mockDBClient) map[string]interface{} {
	t.Helper()
	require.Len(t, mockDB.insertedRows, 1)
	row := mockDB.insertedRows[0]
	m := make(map[string]interface{})
	for i, col := range row.columns {
		m[col] = row.values[i]
	}
	return m
}

func newSavepointMock() *mockDBClient {
	return &mockDBClient{
		primaryKeys: map[string]*mockPK{
			"tigerfs.notes_savepoint": {column: "name"},
		},
		lastInsertReturnPK: "my-checkpoint",
	}
}

func newSavepointMockWithColumns() *mockDBClient {
	m := newSavepointMock()
	m.columns = map[string][]mockColumn{
		"tigerfs.notes_savepoint": {
			{name: "name", dataType: "text"},
			{name: "savepoint_id", dataType: "uuid"},
			{name: "user_id", dataType: "text"},
			{name: "description", dataType: "text"},
		},
	}
	return m
}

// -- Empty body tests (echo "" > name.tsv) --

func TestSynth_Savepoint_TSV_EmptyBody(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)
	ops.SetUserID("agent-7")

	// echo "" > .savepoint/name.tsv -- empty body, should create with just PK + user_id
	data := []byte("\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/quick-mark.tsv", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "quick-mark", m["name"])
	assert.Equal(t, "agent-7", m["user_id"])
}

func TestSynth_Savepoint_JSON_EmptyBody(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)
	ops.SetUserID("agent-7")

	// echo "" > .savepoint/name.json -- empty body
	data := []byte("\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/quick-mark.json", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "quick-mark", m["name"])
	assert.Equal(t, "agent-7", m["user_id"])
}

func TestSynth_Savepoint_JSON_EmptyObject(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	// echo '{}' > .savepoint/name.json -- empty JSON object, name from PK
	data := []byte(`{}`)
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/quick-mark.json", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "quick-mark", m["name"])
}

// -- TSV format tests --

func TestSynth_Savepoint_TSV_NameAndDescription(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	data := []byte("description\nBefore refactoring\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.tsv", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "my-checkpoint", m["name"])
	assert.Equal(t, "Before refactoring", m["description"])
}

func TestSynth_Savepoint_TSV_NameDescriptionUserID(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	data := []byte("description\tuser_id\nBefore refactoring\tagent-9\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.tsv", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "my-checkpoint", m["name"])
	assert.Equal(t, "Before refactoring", m["description"])
	assert.Equal(t, "agent-9", m["user_id"])
}

func TestSynth_Savepoint_TSV_InjectsUserID(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)
	ops.SetUserID("agent-7")

	// user_id NOT in body -- should be auto-injected from mount identity
	data := []byte("description\nBefore refactoring\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.tsv", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "agent-7", m["user_id"])
	assert.Equal(t, "Before refactoring", m["description"])
	assert.Equal(t, "my-checkpoint", m["name"])
}

func TestSynth_Savepoint_TSV_ExplicitUserIDNotOverridden(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)
	ops.SetUserID("agent-7")

	// user_id in body should NOT be overridden by mount identity
	data := []byte("description\tuser_id\nBefore refactoring\tagent-9\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.tsv", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "agent-9", m["user_id"], "explicit user_id should not be overridden")
}

// -- JSON format tests --

func TestSynth_Savepoint_JSON_NameOnly(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	data := []byte(`{}`)
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/quick-mark.json", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "quick-mark", m["name"])
}

func TestSynth_Savepoint_JSON_NameAndDescription(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	data := []byte(`{"description":"Before refactoring"}`)
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.json", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "my-checkpoint", m["name"])
	assert.Equal(t, "Before refactoring", m["description"])
}

func TestSynth_Savepoint_JSON_NameDescriptionUserID(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	data := []byte(`{"description":"Before refactoring","user_id":"agent-9"}`)
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.json", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "my-checkpoint", m["name"])
	assert.Equal(t, "Before refactoring", m["description"])
	assert.Equal(t, "agent-9", m["user_id"])
}

func TestSynth_Savepoint_JSON_InjectsUserID(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)
	ops.SetUserID("agent-7")

	data := []byte(`{"description":"Before refactoring"}`)
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.json", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "agent-7", m["user_id"])
}

// -- CSV format tests --

func TestSynth_Savepoint_CSV_NameAndDescription(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	data := []byte("description\nBefore refactoring\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.csv", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "my-checkpoint", m["name"])
	assert.Equal(t, "Before refactoring", m["description"])
}

func TestSynth_Savepoint_CSV_InjectsUserID(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)
	ops.SetUserID("agent-7")

	data := []byte("description\nBefore refactoring\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.csv", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "agent-7", m["user_id"])
	assert.Equal(t, "my-checkpoint", m["name"])
}

// -- YAML format tests --

func TestSynth_Savepoint_YAML_NameAndDescription(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	data := []byte("description: Before refactoring\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.yaml", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "my-checkpoint", m["name"])
	assert.Equal(t, "Before refactoring", m["description"])
}

func TestSynth_Savepoint_YAML_InjectsUserID(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)
	ops.SetUserID("agent-7")

	data := []byte("description: Before refactoring\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint.yaml", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "agent-7", m["user_id"])
	assert.Equal(t, "my-checkpoint", m["name"])
}

func TestSynth_Savepoint_YAML_EmptyBody(t *testing.T) {
	mockDB := newSavepointMock()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)
	ops.SetUserID("agent-7")

	data := []byte("\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/quick-mark.yaml", data)
	require.Nil(t, fsErr)

	m := insertedColMap(t, mockDB)
	assert.Equal(t, "quick-mark", m["name"])
	assert.Equal(t, "agent-7", m["user_id"])
}

// -- Bare path rejection --

func TestSynth_Savepoint_BarePathRejected(t *testing.T) {
	mockDB := newSavepointMockWithColumns()
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, mockDB)

	data := []byte("Before refactoring\n")
	fsErr := ops.WriteFile(context.Background(), "/notes/.savepoint/my-checkpoint", data)
	require.NotNil(t, fsErr)
	assert.Equal(t, ErrInvalidArgument, fsErr.Code)
	assert.Contains(t, fsErr.Message, "format suffix required")
}
