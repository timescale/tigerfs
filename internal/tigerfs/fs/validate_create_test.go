package fs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tigerfs/internal/tigerfs/config"
)

// newValidateCreateMockDB builds a mock with:
//   - "posts" (public schema): synth markdown view (tigerfs:md), backing table "_posts".
//     Columns match newSynthMockDB so role detection succeeds and the view registers
//     in the synth cache.
//   - "products" (public schema): regular data table, PK "id", one existing row id="existing".
//   - "products_savepoint" (tigerfs schema): regular table, PK "name" (no rows).
//     Exists so /products/.savepoint/xxx can resolve a PK and reach the reject branch.
//
// This lets ValidateCreate exercise: synth-view allow, non-synth new-row reject,
// existing-row allow, and the savepoint-style rejection the original bug reported.
func newValidateCreateMockDB() *mockDBClient {
	return &mockDBClient{
		tables: map[string][]string{
			"public":  {"_posts", "products"},
			"tigerfs": {"products_savepoint"},
		},
		views: map[string][]string{
			"public": {"posts"},
		},
		viewComments: map[string]map[string]string{
			"public": {"posts": "tigerfs:md"},
		},
		primaryKeys: map[string]*mockPK{
			"public._posts":              {column: "id"},
			"public.posts":               {column: "id"},
			"public.products":            {column: "id"},
			"tigerfs.products_savepoint": {column: "name"},
		},
		columns: map[string][]mockColumn{
			"public.posts": {
				{name: "id", dataType: "integer"},
				{name: "filename", dataType: "text"},
				{name: "title", dataType: "text"},
				{name: "author", dataType: "text"},
				{name: "body", dataType: "text"},
			},
			"public.products": {
				{name: "id", dataType: "text"},
				{name: "name", dataType: "text"},
			},
			"tigerfs.products_savepoint": {
				{name: "name", dataType: "text"},
			},
		},
		rowData: map[string]*mockRow{
			// "/products/existing" should be treated as an existing row.
			"public.products.existing": {
				columns: []string{"id", "name"},
				values:  []interface{}{"existing", "already-here"},
			},
		},
	}
}

func TestValidateCreate(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		wantReject bool
		wantPK     string // expected PK in rejection message (used only if wantReject)
	}{
		// Synth view — bare filename is user content, not a PK.
		{name: "synth view bare path", path: "/posts/new-entry", wantReject: false},
		{name: "synth view md suffix", path: "/posts/new-entry.md", wantReject: false},

		// Non-synth data table — bare new row collides with future row-directory inode.
		{name: "data table bare new row", path: "/products/newrow", wantReject: true, wantPK: "newrow"},
		{name: "data table suffixed new row", path: "/products/newrow.tsv", wantReject: false},
		{name: "data table suffixed json", path: "/products/newrow.json", wantReject: false},

		// Existing row — writeRowFile will UPDATE; no inode conflict.
		{name: "data table bare existing row", path: "/products/existing", wantReject: false},

		// Savepoint (the original bug).
		{name: "savepoint bare path", path: "/products/.savepoint/xxx", wantReject: true, wantPK: "xxx"},
		{name: "savepoint json suffix", path: "/products/.savepoint/xxx.json", wantReject: false},

		// Non-PathRow types — ValidateCreate must not fire.
		{name: "column path", path: "/products/existing/name", wantReject: false},

		// Unparseable / unknown paths — fail open.
		{name: "nonexistent table", path: "/nonexistent/foo", wantReject: false},
		{name: "root", path: "/", wantReject: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := NewOperations(&config.Config{DirListingLimit: 1000}, newValidateCreateMockDB())
			fsErr := ops.ValidateCreate(context.Background(), tc.path)
			if tc.wantReject {
				require.NotNil(t, fsErr, "expected rejection for %s", tc.path)
				assert.Equal(t, ErrInvalidArgument, fsErr.Code)
				assert.Contains(t, fsErr.Message, "without a format suffix")
				assert.Contains(t, fsErr.Message, tc.wantPK)
				assert.Contains(t, fsErr.Hint, tc.wantPK+".json")
				assert.Contains(t, fsErr.Hint, tc.wantPK+".tsv")
				assert.Contains(t, fsErr.Hint, tc.wantPK+".csv")
				require.NotNil(t, fsErr.Cause, "Cause must be set to avoid %%!w(<nil>) in NFS log")
				assert.Equal(t, "format suffix required for new rows", fsErr.Cause.Error())
			} else {
				assert.Nil(t, fsErr, "expected allow for %s, got: %+v", tc.path, fsErr)
			}
		})
	}
}

// TestValidateCreate_SharesErrorWithWriteRowFile verifies the Create-time
// rejection and the writeRowFile defense-in-depth rejection return the same
// FSError shape -- they must go through newBarePathRejection in lockstep.
func TestValidateCreate_SharesErrorWithWriteRowFile(t *testing.T) {
	ops := NewOperations(&config.Config{DirListingLimit: 1000}, newValidateCreateMockDB())

	fromValidate := ops.ValidateCreate(context.Background(), "/products/newrow")
	require.NotNil(t, fromValidate)

	fromWrite := ops.WriteFile(context.Background(), "/products/newrow", []byte("ignored"))
	require.NotNil(t, fromWrite)

	assert.Equal(t, fromValidate.Code, fromWrite.Code)
	assert.Equal(t, fromValidate.Message, fromWrite.Message)
	assert.Equal(t, fromValidate.Hint, fromWrite.Hint)
	require.NotNil(t, fromValidate.Cause)
	require.NotNil(t, fromWrite.Cause)
	assert.Equal(t, fromValidate.Cause.Error(), fromWrite.Cause.Error())
}
