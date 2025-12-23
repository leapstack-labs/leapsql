package lineage

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
	"github.com/stretchr/testify/assert"
)

func TestPL01_PassthroughBloat(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		threshold int
		wantDiags int
	}{
		{
			name: "model with many passthrough columns",
			models: map[string]*project.ModelInfo{
				"marts.users": {
					Path:     "marts.users",
					Name:     "dim_users",
					FilePath: "/models/marts/dim_users.sql",
					Type:     lint.ModelTypeMarts,
					Columns:  makePassthroughColumns(25),
				},
			},
			threshold: 20,
			wantDiags: 1,
		},
		{
			name: "model with few passthrough columns - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.users": {
					Path:     "marts.users",
					Name:     "dim_users",
					FilePath: "/models/marts/dim_users.sql",
					Type:     lint.ModelTypeMarts,
					Columns:  makePassthroughColumns(10),
				},
			},
			threshold: 20,
			wantDiags: 0,
		},
		{
			name: "model with transformed columns - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.users": {
					Path:     "marts.users",
					Name:     "dim_users",
					FilePath: "/models/marts/dim_users.sql",
					Type:     lint.ModelTypeMarts,
					Columns:  makeTransformedColumns(25),
				},
			},
			threshold: 20,
			wantDiags: 0,
		},
		{
			name: "model with no columns - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.users": {
					Path:     "marts.users",
					Name:     "dim_users",
					FilePath: "/models/marts/dim_users.sql",
					Type:     lint.ModelTypeMarts,
					Columns:  nil,
				},
			},
			threshold: 20,
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := lint.DefaultProjectHealthConfig()
			cfg.PassthroughColumnThreshold = tt.threshold
			ctx := project.NewContext(tt.models, nil, nil, cfg)
			diags := checkPassthroughBloat(ctx)

			assert.Len(t, diags, tt.wantDiags)
		})
	}
}

// makePassthroughColumns creates n passthrough columns (direct, no transformation).
func makePassthroughColumns(n int) []lint.ColumnInfo {
	cols := make([]lint.ColumnInfo, n)
	for i := 0; i < n; i++ {
		cols[i] = lint.ColumnInfo{
			Name:          "col_" + string(rune('a'+i%26)),
			TransformType: "", // Direct passthrough
			Function:      "",
			Sources: []lint.SourceRef{{
				Table:  "source_table",
				Column: "col_" + string(rune('a'+i%26)),
			}},
		}
	}
	return cols
}

// makeTransformedColumns creates n columns with transformations.
func makeTransformedColumns(n int) []lint.ColumnInfo {
	cols := make([]lint.ColumnInfo, n)
	for i := 0; i < n; i++ {
		cols[i] = lint.ColumnInfo{
			Name:          "col_" + string(rune('a'+i%26)),
			TransformType: "EXPR",
			Function:      "sum",
			Sources: []lint.SourceRef{{
				Table:  "source_table",
				Column: "col_" + string(rune('a'+i%26)),
			}},
		}
	}
	return cols
}

func TestPL02_OrphanedColumns(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		children  map[string][]string
		wantDiags int
	}{
		{
			name: "non-leaf model with orphaned columns",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     lint.ModelTypeStaging,
					Columns: []lint.ColumnInfo{
						{Name: "id", Sources: []lint.SourceRef{{Table: "raw", Column: "id"}}},
						{Name: "name", Sources: []lint.SourceRef{{Table: "raw", Column: "name"}}},
						{Name: "unused_col", Sources: []lint.SourceRef{{Table: "raw", Column: "unused"}}},
					},
				},
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "dim_customers",
					FilePath: "/models/marts/dim_customers.sql",
					Type:     lint.ModelTypeMarts,
					Columns: []lint.ColumnInfo{
						// Only uses id and name from staging.customers, not unused_col
						{Name: "customer_id", Sources: []lint.SourceRef{{Table: "staging.customers", Column: "id"}}},
						{Name: "customer_name", Sources: []lint.SourceRef{{Table: "staging.customers", Column: "name"}}},
					},
				},
			},
			children: map[string][]string{
				"staging.customers": {"marts.customers"},
			},
			wantDiags: 1,
		},
		{
			name: "leaf model - should not flag (no downstream consumers)",
			models: map[string]*project.ModelInfo{
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "dim_customers",
					FilePath: "/models/marts/dim_customers.sql",
					Type:     lint.ModelTypeMarts,
					Columns: []lint.ColumnInfo{
						{Name: "id", Sources: []lint.SourceRef{{Table: "staging", Column: "id"}}},
					},
				},
			},
			children:  map[string][]string{},
			wantDiags: 0,
		},
		{
			name: "all columns consumed - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     lint.ModelTypeStaging,
					Columns: []lint.ColumnInfo{
						{Name: "id", Sources: []lint.SourceRef{{Table: "raw", Column: "id"}}},
						{Name: "name", Sources: []lint.SourceRef{{Table: "raw", Column: "name"}}},
					},
				},
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "dim_customers",
					FilePath: "/models/marts/dim_customers.sql",
					Type:     lint.ModelTypeMarts,
					Columns: []lint.ColumnInfo{
						{Name: "customer_id", Sources: []lint.SourceRef{{Table: "staging.customers", Column: "id"}}},
						{Name: "customer_name", Sources: []lint.SourceRef{{Table: "staging.customers", Column: "name"}}},
					},
				},
			},
			children: map[string][]string{
				"staging.customers": {"marts.customers"},
			},
			wantDiags: 0,
		},
		{
			name: "model with no column info - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     lint.ModelTypeStaging,
					Columns:  nil,
				},
			},
			children: map[string][]string{
				"staging.customers": {"marts.customers"},
			},
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, nil, tt.children, lint.DefaultProjectHealthConfig())
			diags := checkOrphanedColumns(ctx)

			assert.Len(t, diags, tt.wantDiags)
			if tt.wantDiags > 0 {
				assert.Equal(t, "PL02", diags[0].RuleID)
			}
		})
	}
}

func TestPL04_ImplicitCrossJoin(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		wantDiags int
	}{
		{
			name: "model with unbridged source pairs - potential cross join",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:     "marts.report",
					Name:     "fct_report",
					FilePath: "/models/marts/fct_report.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"table_a", "table_b"},
					Columns: []lint.ColumnInfo{
						// Column from table_a only
						{Name: "col_a", Sources: []lint.SourceRef{{Table: "table_a", Column: "a"}}},
						// Column from table_b only - no bridging column!
						{Name: "col_b", Sources: []lint.SourceRef{{Table: "table_b", Column: "b"}}},
					},
				},
			},
			wantDiags: 1,
		},
		{
			name: "model with bridging column - proper join",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:     "marts.report",
					Name:     "fct_report",
					FilePath: "/models/marts/fct_report.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"table_a", "table_b"},
					Columns: []lint.ColumnInfo{
						{Name: "col_a", Sources: []lint.SourceRef{{Table: "table_a", Column: "a"}}},
						{Name: "col_b", Sources: []lint.SourceRef{{Table: "table_b", Column: "b"}}},
						// Bridging column that references both tables
						{Name: "joined_key", Sources: []lint.SourceRef{
							{Table: "table_a", Column: "key"},
							{Table: "table_b", Column: "key"},
						}},
					},
				},
			},
			wantDiags: 0,
		},
		{
			name: "single source model - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.simple": {
					Path:     "marts.simple",
					Name:     "simple_model",
					FilePath: "/models/marts/simple.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"table_a"},
					Columns: []lint.ColumnInfo{
						{Name: "col_a", Sources: []lint.SourceRef{{Table: "table_a", Column: "a"}}},
					},
				},
			},
			wantDiags: 0,
		},
		{
			name: "model with no columns - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.empty": {
					Path:     "marts.empty",
					Name:     "empty_model",
					FilePath: "/models/marts/empty.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"table_a", "table_b"},
					Columns:  nil,
				},
			},
			wantDiags: 0,
		},
		{
			name: "three sources with partial bridging - flags unbridged pair",
			models: map[string]*project.ModelInfo{
				"marts.complex": {
					Path:     "marts.complex",
					Name:     "complex_model",
					FilePath: "/models/marts/complex.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"table_a", "table_b", "table_c"},
					Columns: []lint.ColumnInfo{
						// Bridges A and B
						{Name: "ab_key", Sources: []lint.SourceRef{
							{Table: "table_a", Column: "key"},
							{Table: "table_b", Column: "key"},
						}},
						// C is not bridged to A or B
						{Name: "col_c", Sources: []lint.SourceRef{{Table: "table_c", Column: "c"}}},
					},
				},
			},
			wantDiags: 1, // table_c is not bridged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, nil, nil, lint.DefaultProjectHealthConfig())
			diags := checkImplicitCrossJoin(ctx)

			assert.Len(t, diags, tt.wantDiags)
			if tt.wantDiags > 0 {
				assert.Equal(t, "PL04", diags[0].RuleID)
			}
		})
	}
}

func TestGetSourceTablesFromColumns(t *testing.T) {
	cols := []lint.ColumnInfo{
		{Name: "a", Sources: []lint.SourceRef{{Table: "t1", Column: "a"}}},
		{Name: "b", Sources: []lint.SourceRef{{Table: "t2", Column: "b"}}},
		{Name: "c", Sources: []lint.SourceRef{{Table: "t1", Column: "c"}}}, // duplicate table
		{Name: "d", Sources: []lint.SourceRef{{Table: "", Column: "d"}}},   // empty table
	}

	tables := getSourceTablesFromColumns(cols)
	assert.Len(t, tables, 2)
	assert.Contains(t, tables, "t1")
	assert.Contains(t, tables, "t2")
}

func TestFindUnbridgedSourcePairs(t *testing.T) {
	tests := []struct {
		name     string
		columns  []lint.ColumnInfo
		sources  []string
		wantLen  int
		wantPair [][2]string
	}{
		{
			name: "all pairs bridged",
			columns: []lint.ColumnInfo{
				{Name: "key", Sources: []lint.SourceRef{
					{Table: "a", Column: "id"},
					{Table: "b", Column: "id"},
				}},
			},
			sources:  []string{"a", "b"},
			wantLen:  0,
			wantPair: nil,
		},
		{
			name: "no bridging columns",
			columns: []lint.ColumnInfo{
				{Name: "col_a", Sources: []lint.SourceRef{{Table: "a", Column: "x"}}},
				{Name: "col_b", Sources: []lint.SourceRef{{Table: "b", Column: "y"}}},
			},
			sources:  []string{"a", "b"},
			wantLen:  1,
			wantPair: [][2]string{{"a", "b"}},
		},
		{
			name: "three sources with one bridged pair",
			columns: []lint.ColumnInfo{
				{Name: "ab_key", Sources: []lint.SourceRef{
					{Table: "a", Column: "id"},
					{Table: "b", Column: "id"},
				}},
				{Name: "col_c", Sources: []lint.SourceRef{{Table: "c", Column: "z"}}},
			},
			sources: []string{"a", "b", "c"},
			wantLen: 2, // (a,c) and (b,c) are unbridged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findUnbridgedSourcePairs(tt.columns, tt.sources)
			assert.Len(t, result, tt.wantLen)
			if tt.wantPair != nil {
				assert.Equal(t, tt.wantPair, result)
			}
		})
	}
}
