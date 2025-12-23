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
