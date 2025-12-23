package modeling

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
	"github.com/stretchr/testify/assert"
)

func TestPM01_RootModels(t *testing.T) {
	tests := []struct {
		name       string
		models     map[string]*project.ModelInfo
		wantDiags  int
		wantRuleID string
	}{
		{
			name: "non-staging model with no sources",
			models: map[string]*project.ModelInfo{
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "customers",
					FilePath: "/models/marts/customers.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  nil,
				},
			},
			wantDiags:  1,
			wantRuleID: "PM01",
		},
		{
			name: "staging model with no sources - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     lint.ModelTypeStaging,
					Sources:  nil,
				},
			},
			wantDiags: 0,
		},
		{
			name: "non-staging model with sources - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "customers",
					FilePath: "/models/marts/customers.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
			},
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, nil, nil, lint.DefaultProjectHealthConfig())
			diags := checkRootModels(ctx)

			assert.Len(t, diags, tt.wantDiags)
			if tt.wantDiags > 0 {
				assert.Equal(t, tt.wantRuleID, diags[0].RuleID)
			}
		})
	}
}

func TestPM02_SourceFanout(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		wantDiags int
	}{
		{
			name: "source referenced by multiple non-staging models",
			models: map[string]*project.ModelInfo{
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "customers",
					FilePath: "/models/marts/customers.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"raw_customers"},
				},
				"marts.orders": {
					Path:     "marts.orders",
					Name:     "orders",
					FilePath: "/models/marts/orders.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"raw_customers"},
				},
			},
			wantDiags: 1,
		},
		{
			name: "source referenced by one non-staging model",
			models: map[string]*project.ModelInfo{
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "customers",
					FilePath: "/models/marts/customers.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"raw_customers"},
				},
			},
			wantDiags: 0,
		},
		{
			name: "staging models referencing same source - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     lint.ModelTypeStaging,
					Sources:  []string{"raw_customers"},
				},
			},
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, nil, nil, lint.DefaultProjectHealthConfig())
			diags := checkSourceFanout(ctx)

			assert.Len(t, diags, tt.wantDiags)
		})
	}
}

func TestPM03_StagingDependsStaging(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		wantDiags int
	}{
		{
			name: "staging depends on staging",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     lint.ModelTypeStaging,
					Sources:  []string{"staging.users"},
				},
				"staging.users": {
					Path:     "staging.users",
					Name:     "stg_users",
					FilePath: "/models/staging/stg_users.sql",
					Type:     lint.ModelTypeStaging,
					Sources:  []string{"raw_users"},
				},
			},
			wantDiags: 1,
		},
		{
			name: "staging depends on external source",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     lint.ModelTypeStaging,
					Sources:  []string{"raw_customers"},
				},
			},
			wantDiags: 0,
		},
		{
			name: "marts depends on staging - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "customers",
					FilePath: "/models/marts/customers.sql",
					Type:     lint.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     lint.ModelTypeStaging,
					Sources:  []string{"raw_customers"},
				},
			},
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, nil, nil, lint.DefaultProjectHealthConfig())
			diags := checkStagingDependsStaging(ctx)

			assert.Len(t, diags, tt.wantDiags)
		})
	}
}
