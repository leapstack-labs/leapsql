package projectrules

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
	"github.com/stretchr/testify/assert"
)

// mockSnapshotStore implements project.SnapshotStore for testing.
type mockSnapshotStore struct {
	snapshots map[string]map[string][]string // modelPath -> sourceName -> columns
}

func (m *mockSnapshotStore) GetColumnSnapshot(modelPath string, sourceTable string) (columns []string, runID string, err error) {
	if m.snapshots == nil {
		return nil, "", nil
	}
	sources, ok := m.snapshots[modelPath]
	if !ok {
		return nil, "", nil
	}
	cols, ok := sources[sourceTable]
	if !ok {
		return nil, "", nil
	}
	return cols, "test-run-id", nil
}

func TestPL05_SchemaDrift(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		snapshots map[string]map[string][]string
		wantDiags int
		wantMsg   string
	}{
		{
			name: "model without SELECT * - no diagnostic",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:           "marts.report",
					Name:           "fct_report",
					FilePath:       "/models/marts/fct_report.sql",
					Type:           core.ModelTypeMarts,
					UsesSelectStar: false,
					Sources:        []string{"staging.customers"},
					Columns: []core.ColumnInfo{
						{Name: "id", Sources: []core.SourceRef{{Table: "staging.customers", Column: "id"}}},
						{Name: "name", Sources: []core.SourceRef{{Table: "staging.customers", Column: "name"}}},
					},
				},
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Columns: []core.ColumnInfo{
						{Name: "id"},
						{Name: "name"},
						{Name: "email"}, // New column, but parent doesn't use SELECT *
					},
				},
			},
			snapshots: map[string]map[string][]string{
				"marts.report": {
					"staging.customers": {"id", "name"},
				},
			},
			wantDiags: 0,
		},
		{
			name: "model with SELECT * but no snapshot - no diagnostic",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:           "marts.report",
					Name:           "fct_report",
					FilePath:       "/models/marts/fct_report.sql",
					Type:           core.ModelTypeMarts,
					UsesSelectStar: true,
					Sources:        []string{"staging.customers"},
					Columns: []core.ColumnInfo{
						{Name: "id", Sources: []core.SourceRef{{Table: "staging.customers", Column: "id"}}},
					},
				},
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Columns: []core.ColumnInfo{
						{Name: "id"},
						{Name: "name"},
					},
				},
			},
			snapshots: nil, // No snapshots
			wantDiags: 0,
		},
		{
			name: "model with SELECT * and matching snapshot - no diagnostic",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:           "marts.report",
					Name:           "fct_report",
					FilePath:       "/models/marts/fct_report.sql",
					Type:           core.ModelTypeMarts,
					UsesSelectStar: true,
					Sources:        []string{"staging.customers"},
					Columns: []core.ColumnInfo{
						{Name: "id", Sources: []core.SourceRef{{Table: "staging.customers", Column: "id"}}},
					},
				},
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Columns: []core.ColumnInfo{
						{Name: "id"},
						{Name: "name"},
					},
				},
			},
			snapshots: map[string]map[string][]string{
				"marts.report": {
					"staging.customers": {"id", "name"}, // Matches current columns
				},
			},
			wantDiags: 0,
		},
		{
			name: "model with SELECT * and column added - warning",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:           "marts.report",
					Name:           "fct_report",
					FilePath:       "/models/marts/fct_report.sql",
					Type:           core.ModelTypeMarts,
					UsesSelectStar: true,
					Sources:        []string{"staging.customers"},
					Columns: []core.ColumnInfo{
						{Name: "id", Sources: []core.SourceRef{{Table: "staging.customers", Column: "id"}}},
					},
				},
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Columns: []core.ColumnInfo{
						{Name: "id"},
						{Name: "name"},
						{Name: "email"}, // New column added
					},
				},
			},
			snapshots: map[string]map[string][]string{
				"marts.report": {
					"staging.customers": {"id", "name"}, // Old snapshot without email
				},
			},
			wantDiags: 1,
			wantMsg:   "added: email",
		},
		{
			name: "model with SELECT * and column removed - warning",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:           "marts.report",
					Name:           "fct_report",
					FilePath:       "/models/marts/fct_report.sql",
					Type:           core.ModelTypeMarts,
					UsesSelectStar: true,
					Sources:        []string{"staging.customers"},
					Columns: []core.ColumnInfo{
						{Name: "id", Sources: []core.SourceRef{{Table: "staging.customers", Column: "id"}}},
					},
				},
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Columns: []core.ColumnInfo{
						{Name: "id"},
						// name column removed
					},
				},
			},
			snapshots: map[string]map[string][]string{
				"marts.report": {
					"staging.customers": {"id", "name"}, // Old snapshot with name
				},
			},
			wantDiags: 1,
			wantMsg:   "removed: name",
		},
		{
			name: "model with SELECT * and columns both added and removed - warning",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:           "marts.report",
					Name:           "fct_report",
					FilePath:       "/models/marts/fct_report.sql",
					Type:           core.ModelTypeMarts,
					UsesSelectStar: true,
					Sources:        []string{"staging.customers"},
					Columns: []core.ColumnInfo{
						{Name: "id", Sources: []core.SourceRef{{Table: "staging.customers", Column: "id"}}},
					},
				},
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Columns: []core.ColumnInfo{
						{Name: "id"},
						{Name: "email"}, // Added
						// name removed
					},
				},
			},
			snapshots: map[string]map[string][]string{
				"marts.report": {
					"staging.customers": {"id", "name"}, // Old snapshot
				},
			},
			wantDiags: 1,
			wantMsg:   "added: email",
		},
		{
			name: "source is not a model - no diagnostic (external sources not yet supported)",
			models: map[string]*project.ModelInfo{
				"marts.report": {
					Path:           "marts.report",
					Name:           "fct_report",
					FilePath:       "/models/marts/fct_report.sql",
					Type:           core.ModelTypeMarts,
					UsesSelectStar: true,
					Sources:        []string{"external_table"}, // Not a model
					Columns: []core.ColumnInfo{
						{Name: "id", Sources: []core.SourceRef{{Table: "external_table", Column: "id"}}},
					},
				},
			},
			snapshots: map[string]map[string][]string{
				"marts.report": {
					"external_table": {"id", "name"},
				},
			},
			wantDiags: 0, // External sources not yet supported
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &mockSnapshotStore{snapshots: tt.snapshots}
			ctx := project.NewContextWithStore(tt.models, nil, nil, lint.DefaultProjectHealthConfig(), store)
			diags := checkSchemaDrift(ctx)

			assert.Len(t, diags, tt.wantDiags)
			if tt.wantDiags > 0 {
				assert.Equal(t, "PL05", diags[0].RuleID)
				assert.Equal(t, core.SeverityWarning, diags[0].Severity)
				if tt.wantMsg != "" {
					assert.Contains(t, diags[0].Message, tt.wantMsg)
				}
			}
		})
	}
}

func TestDiffColumns(t *testing.T) {
	tests := []struct {
		name        string
		old         []string
		current     []string
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:        "no changes",
			old:         []string{"a", "b", "c"},
			current:     []string{"a", "b", "c"},
			wantAdded:   nil,
			wantRemoved: nil,
		},
		{
			name:        "column added",
			old:         []string{"a", "b"},
			current:     []string{"a", "b", "c"},
			wantAdded:   []string{"c"},
			wantRemoved: nil,
		},
		{
			name:        "column removed",
			old:         []string{"a", "b", "c"},
			current:     []string{"a", "b"},
			wantAdded:   nil,
			wantRemoved: []string{"c"},
		},
		{
			name:        "columns added and removed",
			old:         []string{"a", "b"},
			current:     []string{"a", "c"},
			wantAdded:   []string{"c"},
			wantRemoved: []string{"b"},
		},
		{
			name:        "multiple changes",
			old:         []string{"a", "b", "c"},
			current:     []string{"b", "d", "e"},
			wantAdded:   []string{"d", "e"},
			wantRemoved: []string{"a", "c"},
		},
		{
			name:        "empty old",
			old:         []string{},
			current:     []string{"a", "b"},
			wantAdded:   []string{"a", "b"},
			wantRemoved: nil,
		},
		{
			name:        "empty current",
			old:         []string{"a", "b"},
			current:     []string{},
			wantAdded:   nil,
			wantRemoved: []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			added, removed := diffColumns(tt.old, tt.current)
			assert.ElementsMatch(t, tt.wantAdded, added)
			assert.ElementsMatch(t, tt.wantRemoved, removed)
		})
	}
}

func TestBuildDriftMessage(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		added   []string
		removed []string
		want    string
	}{
		{
			name:    "only added",
			source:  "users",
			added:   []string{"email"},
			removed: nil,
			want:    "Source 'users' schema changed; added: email",
		},
		{
			name:    "only removed",
			source:  "users",
			added:   nil,
			removed: []string{"name"},
			want:    "Source 'users' schema changed; removed: name",
		},
		{
			name:    "both added and removed",
			source:  "users",
			added:   []string{"email"},
			removed: []string{"name"},
			want:    "Source 'users' schema changed; added: email; removed: name",
		},
		{
			name:    "multiple added (<=3)",
			source:  "users",
			added:   []string{"a", "b", "c"},
			removed: nil,
			want:    "Source 'users' schema changed; added: a, b, c",
		},
		{
			name:    "many added (>3)",
			source:  "users",
			added:   []string{"a", "b", "c", "d"},
			removed: nil,
			want:    "Source 'users' schema changed; added 4 columns",
		},
		{
			name:    "many removed (>3)",
			source:  "users",
			added:   nil,
			removed: []string{"a", "b", "c", "d", "e"},
			want:    "Source 'users' schema changed; removed 5 columns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDriftMessage(tt.source, tt.added, tt.removed)
			assert.Equal(t, tt.want, got)
		})
	}
}
