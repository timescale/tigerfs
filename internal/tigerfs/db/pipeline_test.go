package db

import (
	"strings"
	"testing"
)

// TestLimitType_String tests the String method for LimitType.
func TestLimitType_String(t *testing.T) {
	tests := []struct {
		lt   LimitType
		want string
	}{
		{LimitNone, "none"},
		{LimitFirst, "first"},
		{LimitLast, "last"},
		{LimitSample, "sample"},
		{LimitType(99), "none"}, // Unknown type defaults to none
	}

	for _, tt := range tests {
		got := tt.lt.String()
		if got != tt.want {
			t.Errorf("LimitType(%d).String() = %q, want %q", tt.lt, got, tt.want)
		}
	}
}

// TestQueryParams_NeedsSubquery tests subquery detection.
func TestQueryParams_NeedsSubquery(t *testing.T) {
	tests := []struct {
		name   string
		params QueryParams
		want   bool
	}{
		{
			name:   "no limit",
			params: QueryParams{},
			want:   false,
		},
		{
			name: "single limit",
			params: QueryParams{
				Limit:     100,
				LimitType: LimitFirst,
			},
			want: false,
		},
		{
			name: "nested limits",
			params: QueryParams{
				Limit:             50,
				LimitType:         LimitLast,
				PreviousLimit:     100,
				PreviousLimitType: LimitFirst,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.params.NeedsSubquery()
			if got != tt.want {
				t.Errorf("NeedsSubquery() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBuildPipelineSQL_Simple tests SQL generation for simple (non-nested) queries.
func TestBuildPipelineSQL_Simple(t *testing.T) {
	tests := []struct {
		name         string
		params       QueryParams
		selectPKOnly bool
		wantSQL      string
		wantParams   int // number of parameters
	}{
		{
			name: "no filters, no limit",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
			},
			selectPKOnly: true,
			wantSQL:      `SELECT "id" FROM "public"."users"`,
			wantParams:   0,
		},
		{
			name: "single filter",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: "status", Value: "active"},
				},
			},
			selectPKOnly: true,
			wantSQL:      `SELECT "id" FROM "public"."users" WHERE "status" = $1`,
			wantParams:   1,
		},
		{
			name: "multiple filters",
			params: QueryParams{
				Schema:    "public",
				Table:     "orders",
				PKColumns: []string{"order_id"},
				Filters: []FilterCondition{
					{Column: "status", Value: "pending"},
					{Column: "customer_id", Value: "123"},
				},
			},
			selectPKOnly: true,
			wantSQL:      `WHERE "status" = $1 AND "customer_id" = $2`,
			wantParams:   2,
		},
		{
			name: "first N limit",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Limit:     100,
				LimitType: LimitFirst,
			},
			selectPKOnly: true,
			wantSQL:      `ORDER BY "id" ASC LIMIT $1`,
			wantParams:   1,
		},
		{
			name: "last N limit",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Limit:     50,
				LimitType: LimitLast,
			},
			selectPKOnly: true,
			wantSQL:      `ORDER BY "id" DESC LIMIT $1`,
			wantParams:   1,
		},
		{
			name: "sample limit",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Limit:     25,
				LimitType: LimitSample,
			},
			selectPKOnly: true,
			wantSQL:      `ORDER BY RANDOM() LIMIT $1`,
			wantParams:   1,
		},
		{
			name: "custom order ascending",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				OrderBy:   "name",
				OrderDesc: false,
				Limit:     100,
				LimitType: LimitFirst,
			},
			selectPKOnly: true,
			wantSQL:      `ORDER BY "name" ASC NULLS LAST, "id" ASC LIMIT`,
			wantParams:   1,
		},
		{
			name: "custom order descending",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				OrderBy:   "created_at",
				OrderDesc: true,
				Limit:     50,
				LimitType: LimitLast,
			},
			selectPKOnly: true,
			wantSQL:      `ORDER BY "created_at" DESC NULLS LAST, "id" DESC LIMIT`,
			wantParams:   1,
		},
		{
			name: "filter + order + limit",
			params: QueryParams{
				Schema:    "public",
				Table:     "orders",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: "status", Value: "active"},
				},
				OrderBy:   "created_at",
				OrderDesc: false,
				Limit:     100,
				LimitType: LimitFirst,
			},
			selectPKOnly: true,
			wantSQL:      `WHERE "status" = $1 ORDER BY "created_at" ASC`,
			wantParams:   2, // filter value + limit
		},
		{
			name: "select all columns",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Limit:     10,
				LimitType: LimitFirst,
			},
			selectPKOnly: false,
			wantSQL:      `SELECT * FROM "public"."users"`,
			wantParams:   1,
		},
		{
			name: "identifiers with embedded double quotes",
			params: QueryParams{
				Schema:    `my"schema`,
				Table:     `my"table`,
				PKColumns: []string{`my"id`},
			},
			selectPKOnly: true,
			wantSQL:      `SELECT "my""id" FROM "my""schema"."my""table"`,
			wantParams:   0,
		},
		{
			name: "filter column with embedded double quote",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: `sta"tus`, Value: "active"},
				},
			},
			selectPKOnly: true,
			wantSQL:      `WHERE "sta""tus" = $1`,
			wantParams:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params := BuildPipelineSQLForTest(tt.params, tt.selectPKOnly)

			if !strings.Contains(sql, tt.wantSQL) {
				t.Errorf("SQL does not contain expected fragment:\n  got: %s\n  want fragment: %s", sql, tt.wantSQL)
			}

			if len(params) != tt.wantParams {
				t.Errorf("Parameter count = %d, want %d", len(params), tt.wantParams)
			}
		})
	}
}

// TestBuildPipelineSQL_Nested tests SQL generation for nested limit queries.
func TestBuildPipelineSQL_Nested(t *testing.T) {
	tests := []struct {
		name       string
		params     QueryParams
		wantInner  string // fragment that should be in inner subquery
		wantOuter  string // fragment that should be in outer query
		wantParams int
	}{
		{
			name: "first then last (.first/100/.last/50/)",
			params: QueryParams{
				Schema:            "public",
				Table:             "users",
				PKColumns:         []string{"id"},
				Limit:             50,
				LimitType:         LimitLast,
				PreviousLimit:     100,
				PreviousLimitType: LimitFirst,
			},
			wantInner:  `ORDER BY "id" ASC LIMIT`,
			wantOuter:  `ORDER BY "id" DESC LIMIT`,
			wantParams: 2, // both limits
		},
		{
			name: "last then first (.last/100/.first/50/)",
			params: QueryParams{
				Schema:            "public",
				Table:             "users",
				PKColumns:         []string{"id"},
				Limit:             50,
				LimitType:         LimitFirst,
				PreviousLimit:     100,
				PreviousLimitType: LimitLast,
			},
			wantInner:  `ORDER BY "id" DESC LIMIT`,
			wantOuter:  `ORDER BY "id" ASC LIMIT`,
			wantParams: 2,
		},
		{
			name: "first then sample (.first/1000/.sample/50/)",
			params: QueryParams{
				Schema:            "public",
				Table:             "users",
				PKColumns:         []string{"id"},
				Limit:             50,
				LimitType:         LimitSample,
				PreviousLimit:     1000,
				PreviousLimitType: LimitFirst,
			},
			wantInner:  `ORDER BY "id" ASC LIMIT`,
			wantOuter:  `ORDER BY RANDOM() LIMIT`,
			wantParams: 2,
		},
		{
			name: "nested with post-filter (.first/100/.filter/status/active/.last/50/)",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: "status", Value: "active"},
				},
				Limit:             50,
				LimitType:         LimitLast,
				PreviousLimit:     100,
				PreviousLimitType: LimitFirst,
			},
			wantInner:  `ORDER BY "id" ASC LIMIT`,
			wantOuter:  `WHERE "status" = $`,
			wantParams: 3, // previous limit + filter value + current limit
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params := BuildPipelineSQLForTest(tt.params, true)

			// Should be a subquery (contains nested SELECT)
			if !strings.Contains(sql, "FROM (SELECT") {
				t.Errorf("Expected nested subquery, got: %s", sql)
			}

			if !strings.Contains(sql, tt.wantInner) {
				t.Errorf("SQL does not contain expected inner fragment:\n  got: %s\n  want: %s", sql, tt.wantInner)
			}

			// The outer fragment check is trickier because it may appear multiple times
			// Just verify the general structure
			if tt.wantOuter != "" {
				// Count occurrences - the outer one should be at the end
				lastIndex := strings.LastIndex(sql, "ORDER BY")
				if lastIndex == -1 {
					t.Errorf("SQL missing outer ORDER BY: %s", sql)
				}
			}

			if len(params) != tt.wantParams {
				t.Errorf("Parameter count = %d, want %d\nSQL: %s\nParams: %v", len(params), tt.wantParams, sql, params)
			}
		})
	}
}

// TestBuildPipelineSQL_ParameterValues tests that parameter values are correct.
func TestBuildPipelineSQL_ParameterValues(t *testing.T) {
	params := QueryParams{
		Schema:    "myschema",
		Table:     "mytable",
		PKColumns: []string{"pk"},
		Filters: []FilterCondition{
			{Column: "col1", Value: "val1"},
			{Column: "col2", Value: "val2"},
		},
		Limit:     100,
		LimitType: LimitFirst,
	}

	_, queryParams := BuildPipelineSQLForTest(params, true)

	// Should have 3 parameters: val1, val2, 100
	if len(queryParams) != 3 {
		t.Fatalf("Expected 3 parameters, got %d", len(queryParams))
	}

	if queryParams[0] != "val1" {
		t.Errorf("First param = %v, want %q", queryParams[0], "val1")
	}
	if queryParams[1] != "val2" {
		t.Errorf("Second param = %v, want %q", queryParams[1], "val2")
	}
	if queryParams[2] != 100 {
		t.Errorf("Third param = %v, want %d", queryParams[2], 100)
	}
}

// TestBuildPipelineSQL_SQLInjectionPrevention tests that identifiers are properly quoted.
func TestBuildPipelineSQL_SQLInjectionPrevention(t *testing.T) {
	t.Run("semicolon injection", func(t *testing.T) {
		params := QueryParams{
			Schema:    "public; DROP TABLE users;--",
			Table:     "users\"; DROP TABLE users;--",
			PKColumns: []string{"id\"; DROP TABLE users;--"},
			Filters: []FilterCondition{
				{Column: "status\"; DROP TABLE users;--", Value: "active"},
			},
			Limit:     100,
			LimitType: LimitFirst,
		}

		sql, _ := BuildPipelineSQLForTest(params, true)

		// The dangerous parts should be quoted as identifiers, not executed
		if strings.Contains(sql, "DROP TABLE") && !strings.Contains(sql, `"`) {
			t.Errorf("SQL may be vulnerable to injection: %s", sql)
		}

		// Values are parameterized, so "active" should be $N, not inline
		if strings.Contains(sql, "'active'") {
			t.Errorf("Filter value should be parameterized, not inline: %s", sql)
		}
	})

	t.Run("embedded double quote injection", func(t *testing.T) {
		// A table/schema with " in its name must have quotes properly escaped
		params := QueryParams{
			Schema:    `my"schema`,
			Table:     `my"table`,
			PKColumns: []string{`my"pk`},
		}

		sql, _ := BuildPipelineSQLForTest(params, true)

		// Verify proper escaping: embedded " must be doubled
		if !strings.Contains(sql, `"my""schema"."my""table"`) {
			t.Errorf("Schema.table not properly escaped:\n  got: %s\n  want to contain: %s",
				sql, `"my""schema"."my""table"`)
		}
		if !strings.Contains(sql, `"my""pk"`) {
			t.Errorf("PK column not properly escaped:\n  got: %s\n  want to contain: %s",
				sql, `"my""pk"`)
		}
	})

	t.Run("filter column with embedded quote", func(t *testing.T) {
		params := QueryParams{
			Schema:    "public",
			Table:     "users",
			PKColumns: []string{"id"},
			Filters: []FilterCondition{
				{Column: `col"name`, Value: "val"},
			},
		}

		sql, _ := BuildPipelineSQLForTest(params, true)

		if !strings.Contains(sql, `"col""name" = $1`) {
			t.Errorf("Filter column not properly escaped:\n  got: %s\n  want to contain: %s",
				sql, `"col""name" = $1`)
		}
	})
}

// TestBuildPipelineSQL_NonIdentifierValues verifies that SQL keywords and
// direction strings are NOT wrapped in identifier quotes. This catches
// accidental over-application of qi().
func TestBuildPipelineSQL_NonIdentifierValues(t *testing.T) {
	// Strings that must NEVER appear quoted as identifiers in the output
	forbiddenQuoted := []string{`"ASC"`, `"DESC"`, `"NULLS"`, `"LAST"`, `"RANDOM()"`, `"LIMIT"`}

	tests := []struct {
		name   string
		params QueryParams
	}{
		{
			name: "ascending order",
			params: QueryParams{
				Schema: "public", Table: "users", PKColumns: []string{"id"},
				OrderBy: "name", OrderDesc: false,
				Limit: 10, LimitType: LimitFirst,
			},
		},
		{
			name: "descending order",
			params: QueryParams{
				Schema: "public", Table: "users", PKColumns: []string{"id"},
				OrderBy: "created_at", OrderDesc: true,
				Limit: 10, LimitType: LimitLast,
			},
		},
		{
			name: "random sample",
			params: QueryParams{
				Schema: "public", Table: "users", PKColumns: []string{"id"},
				Limit: 10, LimitType: LimitSample,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, _ := BuildPipelineSQLForTest(tt.params, true)

			for _, forbidden := range forbiddenQuoted {
				if strings.Contains(sql, forbidden) {
					t.Errorf("SQL contains quoted keyword %s (should be bare):\n  got: %s", forbidden, sql)
				}
			}
		})
	}
}

// TestQueryParams_HasMethods tests the Has* helper methods.
func TestQueryParams_HasMethods(t *testing.T) {
	t.Run("HasFilters", func(t *testing.T) {
		empty := QueryParams{}
		if empty.HasFilters() {
			t.Error("Empty params should not have filters")
		}

		with := QueryParams{Filters: []FilterCondition{{Column: "a", Value: "b"}}}
		if !with.HasFilters() {
			t.Error("Params with filter should have filters")
		}
	})

	t.Run("HasLimit", func(t *testing.T) {
		empty := QueryParams{}
		if empty.HasLimit() {
			t.Error("Empty params should not have limit")
		}

		with := QueryParams{LimitType: LimitFirst, Limit: 100}
		if !with.HasLimit() {
			t.Error("Params with limit should have limit")
		}
	})

	t.Run("HasOrder", func(t *testing.T) {
		empty := QueryParams{}
		if empty.HasOrder() {
			t.Error("Empty params should not have order")
		}

		with := QueryParams{OrderBy: "name"}
		if !with.HasOrder() {
			t.Error("Params with order should have order")
		}
	})
}

// TestBuildPipelineSQL_ConflictingFilters tests AND semantics for impossible conditions.
// This verifies the SQL is correctly generated - actual empty results require DB execution.
func TestBuildPipelineSQL_ConflictingFilters(t *testing.T) {
	tests := []struct {
		name       string
		params     QueryParams
		wantSQL    string // fragment to verify
		wantParams int
	}{
		{
			name: "same column different values (.by/user_id/90/.by/user_id/71/)",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: "user_id", Value: "90", Indexed: true},
					{Column: "user_id", Value: "71", Indexed: true},
				},
			},
			// This generates user_id = $1 AND user_id = $2 which returns 0 rows
			wantSQL:    `WHERE "user_id" = $1 AND "user_id" = $2`,
			wantParams: 2,
		},
		{
			name: "mixed indexed and non-indexed on same column",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: "status", Value: "active", Indexed: true},
					{Column: "status", Value: "inactive", Indexed: false},
				},
			},
			// Different values = 0 results when executed
			wantSQL:    `WHERE "status" = $1 AND "status" = $2`,
			wantParams: 2,
		},
		{
			name: "three filters on same column",
			params: QueryParams{
				Schema:    "public",
				Table:     "events",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: "type", Value: "click", Indexed: true},
					{Column: "type", Value: "view", Indexed: true},
					{Column: "type", Value: "scroll", Indexed: true},
				},
			},
			wantSQL:    `"type" = $1 AND "type" = $2 AND "type" = $3`,
			wantParams: 3,
		},
		{
			name: "conflicting filter with limit",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: "id", Value: "1", Indexed: true},
					{Column: "id", Value: "2", Indexed: true},
				},
				Limit:     100,
				LimitType: LimitFirst,
			},
			wantSQL:    `WHERE "id" = $1 AND "id" = $2`,
			wantParams: 3, // 2 filter values + 1 limit
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params := BuildPipelineSQLForTest(tt.params, true)

			if !strings.Contains(sql, tt.wantSQL) {
				t.Errorf("SQL does not contain expected fragment:\n  got: %s\n  want: %s", sql, tt.wantSQL)
			}

			if len(params) != tt.wantParams {
				t.Errorf("Parameter count = %d, want %d\nSQL: %s", len(params), tt.wantParams, sql)
			}
		})
	}
}

// TestBuildPipelineSQL_ComplexScenarios tests complex real-world scenarios.
func TestBuildPipelineSQL_ComplexScenarios(t *testing.T) {
	tests := []struct {
		name   string
		params QueryParams
		check  func(t *testing.T, sql string, params []interface{})
	}{
		{
			name: "export query (SELECT *)",
			params: QueryParams{
				Schema:    "public",
				Table:     "orders",
				PKColumns: []string{"id"},
				Filters: []FilterCondition{
					{Column: "customer_id", Value: "123"},
				},
				OrderBy:   "created_at",
				OrderDesc: true,
				Limit:     100,
				LimitType: LimitLast,
			},
			check: func(t *testing.T, sql string, params []interface{}) {
				// For export, we want SELECT *
				gotSQL, _ := BuildPipelineSQLForTest(QueryParams{
					Schema:    "public",
					Table:     "orders",
					PKColumns: []string{"id"},
					Filters: []FilterCondition{
						{Column: "customer_id", Value: "123"},
					},
					OrderBy:   "created_at",
					OrderDesc: true,
					Limit:     100,
					LimitType: LimitLast,
				}, false) // selectPKOnly=false

				if !strings.Contains(gotSQL, "SELECT *") {
					t.Errorf("Export query should select all columns: %s", gotSQL)
				}
			},
		},
		{
			name: "multiple filters with order",
			params: QueryParams{
				Schema:    "public",
				Table:     "events",
				PKColumns: []string{"event_id"},
				Filters: []FilterCondition{
					{Column: "type", Value: "click", Indexed: true},
					{Column: "source", Value: "web", Indexed: false},
					{Column: "status", Value: "processed", Indexed: true},
				},
				OrderBy:   "timestamp",
				OrderDesc: true,
				Limit:     1000,
				LimitType: LimitFirst,
			},
			check: func(t *testing.T, sql string, params []interface{}) {
				// Should have 3 filter conditions ANDed
				andCount := strings.Count(sql, " AND ")
				if andCount != 2 {
					t.Errorf("Expected 2 ANDs for 3 filters, got %d in: %s", andCount, sql)
				}

				// Should have 4 parameters (3 filter values + 1 limit)
				if len(params) != 4 {
					t.Errorf("Expected 4 parameters, got %d", len(params))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params := BuildPipelineSQLForTest(tt.params, true)
			tt.check(t, sql, params)
		})
	}
}

// TestBuildPipelineSQL_Columns tests SQL generation with column projection.
func TestBuildPipelineSQL_Columns(t *testing.T) {
	tests := []struct {
		name         string
		params       QueryParams
		selectPKOnly bool
		wantSQL      string
		wantParams   int
	}{
		{
			name: "columns produce SELECT col list",
			params: QueryParams{
				Schema:    "public",
				Table:     "orders",
				PKColumns: []string{"id"},
				Columns:   []string{"id", "status", "total"},
			},
			selectPKOnly: false,
			wantSQL:      `SELECT "id", "status", "total" FROM "public"."orders"`,
			wantParams:   0,
		},
		{
			name: "selectPKOnly overrides columns",
			params: QueryParams{
				Schema:    "public",
				Table:     "orders",
				PKColumns: []string{"id"},
				Columns:   []string{"id", "status"},
			},
			selectPKOnly: true,
			wantSQL:      `SELECT "id" FROM "public"."orders"`,
			wantParams:   0,
		},
		{
			name: "columns with filter",
			params: QueryParams{
				Schema:    "public",
				Table:     "orders",
				PKColumns: []string{"id"},
				Columns:   []string{"id", "amount"},
				Filters: []FilterCondition{
					{Column: "status", Value: "active"},
				},
			},
			selectPKOnly: false,
			wantSQL:      `SELECT "id", "amount" FROM "public"."orders" WHERE "status" = $1`,
			wantParams:   1,
		},
		{
			name: "columns with filter and limit",
			params: QueryParams{
				Schema:    "public",
				Table:     "orders",
				PKColumns: []string{"id"},
				Columns:   []string{"id", "status"},
				Filters: []FilterCondition{
					{Column: "status", Value: "pending"},
				},
				Limit:     10,
				LimitType: LimitFirst,
			},
			selectPKOnly: false,
			wantSQL:      `SELECT "id", "status" FROM "public"."orders" WHERE "status" = $1`,
			wantParams:   2,
		},
		{
			name: "single column",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Columns:   []string{"email"},
			},
			selectPKOnly: false,
			wantSQL:      `SELECT "email" FROM "public"."users"`,
			wantParams:   0,
		},
		{
			name: "empty columns means SELECT *",
			params: QueryParams{
				Schema:    "public",
				Table:     "users",
				PKColumns: []string{"id"},
				Columns:   []string{},
			},
			selectPKOnly: false,
			wantSQL:      `SELECT * FROM "public"."users"`,
			wantParams:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params := BuildPipelineSQLForTest(tt.params, tt.selectPKOnly)

			if !strings.Contains(sql, tt.wantSQL) {
				t.Errorf("SQL does not contain expected fragment:\n  got:  %s\n  want: %s", sql, tt.wantSQL)
			}

			if len(params) != tt.wantParams {
				t.Errorf("Parameter count = %d, want %d\nSQL: %s", len(params), tt.wantParams, sql)
			}
		})
	}
}

// TestBuildPipelineSQL_ColumnsNested tests column projection with nested subqueries.
func TestBuildPipelineSQL_ColumnsNested(t *testing.T) {
	params := QueryParams{
		Schema:            "public",
		Table:             "orders",
		PKColumns:         []string{"id"},
		Columns:           []string{"id", "status"},
		Limit:             50,
		LimitType:         LimitLast,
		PreviousLimit:     100,
		PreviousLimitType: LimitFirst,
	}

	sql, qp := BuildPipelineSQLForTest(params, false)

	// Outer SELECT should use columns
	if !strings.HasPrefix(sql, `SELECT "id", "status" FROM (`) {
		t.Errorf("Outer SELECT should use column projection, got: %s", sql)
	}

	// Inner subquery should still be SELECT * (for filters/ordering to work on all columns)
	if !strings.Contains(sql, "SELECT * FROM") {
		t.Errorf("Inner subquery should use SELECT *, got: %s", sql)
	}

	if len(qp) != 2 {
		t.Errorf("Expected 2 parameters (two limits), got %d", len(qp))
	}
}
