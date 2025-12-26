package modeling

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/core"
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
					Type:     core.ModelTypeMarts,
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
					Type:     core.ModelTypeStaging,
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
					Type:     core.ModelTypeMarts,
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
					Type:     core.ModelTypeMarts,
					Sources:  []string{"raw_customers"},
				},
				"marts.orders": {
					Path:     "marts.orders",
					Name:     "orders",
					FilePath: "/models/marts/orders.sql",
					Type:     core.ModelTypeMarts,
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
					Type:     core.ModelTypeMarts,
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
					Type:     core.ModelTypeStaging,
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
					Type:     core.ModelTypeStaging,
					Sources:  []string{"staging.users"},
				},
				"staging.users": {
					Path:     "staging.users",
					Name:     "stg_users",
					FilePath: "/models/staging/stg_users.sql",
					Type:     core.ModelTypeStaging,
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
					Type:     core.ModelTypeStaging,
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
					Type:     core.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
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

func TestPM04_ModelFanout(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		children  map[string][]string
		threshold int
		wantDiags int
	}{
		{
			name: "model with many downstream consumers",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Sources:  []string{"raw_customers"},
				},
				"marts.dim_users": {
					Path:     "marts.dim_users",
					Name:     "dim_users",
					FilePath: "/models/marts/dim_users.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
				"marts.fct_orders": {
					Path:     "marts.fct_orders",
					Name:     "fct_orders",
					FilePath: "/models/marts/fct_orders.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
				"marts.fct_revenue": {
					Path:     "marts.fct_revenue",
					Name:     "fct_revenue",
					FilePath: "/models/marts/fct_revenue.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
				"marts.fct_signups": {
					Path:     "marts.fct_signups",
					Name:     "fct_signups",
					FilePath: "/models/marts/fct_signups.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
			},
			children: map[string][]string{
				"staging.customers": {"marts.dim_users", "marts.fct_orders", "marts.fct_revenue", "marts.fct_signups"},
			},
			threshold: 3,
			wantDiags: 1,
		},
		{
			name: "model within threshold - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Sources:  []string{"raw_customers"},
				},
				"marts.dim_users": {
					Path:     "marts.dim_users",
					Name:     "dim_users",
					FilePath: "/models/marts/dim_users.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
			},
			children: map[string][]string{
				"staging.customers": {"marts.dim_users"},
			},
			threshold: 3,
			wantDiags: 0,
		},
		{
			name: "model exactly at threshold - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Sources:  []string{"raw_customers"},
				},
			},
			children: map[string][]string{
				"staging.customers": {"a", "b", "c"},
			},
			threshold: 3,
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := lint.DefaultProjectHealthConfig()
			cfg.ModelFanoutThreshold = tt.threshold
			ctx := project.NewContext(tt.models, nil, tt.children, cfg)
			diags := checkModelFanout(ctx)

			assert.Len(t, diags, tt.wantDiags)
			if tt.wantDiags > 0 {
				assert.Equal(t, "PM04", diags[0].RuleID)
			}
		})
	}
}

func TestPM05_TooManyJoins(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		threshold int
		wantDiags int
	}{
		{
			name: "model with too many upstream sources",
			models: map[string]*project.ModelInfo{
				"marts.god_model": {
					Path:     "marts.god_model",
					Name:     "god_model",
					FilePath: "/models/marts/god_model.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"a", "b", "c", "d", "e", "f", "g", "h"},
				},
			},
			threshold: 7,
			wantDiags: 1,
		},
		{
			name: "model within threshold - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.simple_model": {
					Path:     "marts.simple_model",
					Name:     "simple_model",
					FilePath: "/models/marts/simple_model.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"a", "b", "c"},
				},
			},
			threshold: 7,
			wantDiags: 0,
		},
		{
			name: "model exactly at threshold - should not flag",
			models: map[string]*project.ModelInfo{
				"marts.exact_model": {
					Path:     "marts.exact_model",
					Name:     "exact_model",
					FilePath: "/models/marts/exact_model.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"a", "b", "c", "d", "e", "f", "g"},
				},
			},
			threshold: 7,
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := lint.DefaultProjectHealthConfig()
			cfg.TooManyJoinsThreshold = tt.threshold
			ctx := project.NewContext(tt.models, nil, nil, cfg)
			diags := checkTooManyJoins(ctx)

			assert.Len(t, diags, tt.wantDiags)
			if tt.wantDiags > 0 {
				assert.Equal(t, "PM05", diags[0].RuleID)
			}
		})
	}
}

func TestPM06_DownstreamOnSource(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		wantDiags int
	}{
		{
			name: "marts model depends directly on external source",
			models: map[string]*project.ModelInfo{
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "dim_customers",
					FilePath: "/models/marts/dim_customers.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"raw_customers"},
				},
			},
			wantDiags: 1,
		},
		{
			name: "intermediate model depends directly on external source",
			models: map[string]*project.ModelInfo{
				"intermediate.customers": {
					Path:     "intermediate.customers",
					Name:     "int_customers",
					FilePath: "/models/intermediate/int_customers.sql",
					Type:     core.ModelTypeIntermediate,
					Sources:  []string{"raw_customers"},
				},
			},
			wantDiags: 1,
		},
		{
			name: "marts model depends on staging - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Sources:  []string{"raw_customers"},
				},
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "dim_customers",
					FilePath: "/models/marts/dim_customers.sql",
					Type:     core.ModelTypeMarts,
					Sources:  []string{"staging.customers"},
				},
			},
			wantDiags: 0,
		},
		{
			name: "staging model depends on external source - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
					Sources:  []string{"raw_customers"},
				},
			},
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, nil, nil, lint.DefaultProjectHealthConfig())
			diags := checkDownstreamOnSource(ctx)

			assert.Len(t, diags, tt.wantDiags)
			if tt.wantDiags > 0 {
				assert.Equal(t, "PM06", diags[0].RuleID)
			}
		})
	}
}

func TestPM07_RejoiningUpstream(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		parents   map[string][]string
		children  map[string][]string
		wantDiags int
	}{
		{
			name: "unnecessary intermediate: A -> B -> C, A -> C, B has only C as consumer",
			models: map[string]*project.ModelInfo{
				"staging.a": {
					Path:     "staging.a",
					Name:     "stg_a",
					FilePath: "/models/staging/stg_a.sql",
					Type:     core.ModelTypeStaging,
				},
				"intermediate.b": {
					Path:     "intermediate.b",
					Name:     "int_b",
					FilePath: "/models/intermediate/int_b.sql",
					Type:     core.ModelTypeIntermediate,
				},
				"marts.c": {
					Path:     "marts.c",
					Name:     "dim_c",
					FilePath: "/models/marts/dim_c.sql",
					Type:     core.ModelTypeMarts,
				},
			},
			parents: map[string][]string{
				"intermediate.b": {"staging.a"},
				"marts.c":        {"staging.a", "intermediate.b"},
			},
			children: map[string][]string{
				"staging.a":      {"intermediate.b", "marts.c"},
				"intermediate.b": {"marts.c"},
			},
			wantDiags: 1,
		},
		{
			name: "useful intermediate: B has multiple consumers",
			models: map[string]*project.ModelInfo{
				"staging.a": {
					Path:     "staging.a",
					Name:     "stg_a",
					FilePath: "/models/staging/stg_a.sql",
					Type:     core.ModelTypeStaging,
				},
				"intermediate.b": {
					Path:     "intermediate.b",
					Name:     "int_b",
					FilePath: "/models/intermediate/int_b.sql",
					Type:     core.ModelTypeIntermediate,
				},
				"marts.c": {
					Path:     "marts.c",
					Name:     "dim_c",
					FilePath: "/models/marts/dim_c.sql",
					Type:     core.ModelTypeMarts,
				},
				"marts.d": {
					Path:     "marts.d",
					Name:     "dim_d",
					FilePath: "/models/marts/dim_d.sql",
					Type:     core.ModelTypeMarts,
				},
			},
			parents: map[string][]string{
				"intermediate.b": {"staging.a"},
				"marts.c":        {"staging.a", "intermediate.b"},
				"marts.d":        {"intermediate.b"},
			},
			children: map[string][]string{
				"staging.a":      {"intermediate.b", "marts.c"},
				"intermediate.b": {"marts.c", "marts.d"},
			},
			wantDiags: 0,
		},
		{
			name: "no rejoining pattern - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.a": {
					Path:     "staging.a",
					Name:     "stg_a",
					FilePath: "/models/staging/stg_a.sql",
					Type:     core.ModelTypeStaging,
				},
				"marts.b": {
					Path:     "marts.b",
					Name:     "dim_b",
					FilePath: "/models/marts/dim_b.sql",
					Type:     core.ModelTypeMarts,
				},
			},
			parents: map[string][]string{
				"marts.b": {"staging.a"},
			},
			children: map[string][]string{
				"staging.a": {"marts.b"},
			},
			wantDiags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, tt.parents, tt.children, lint.DefaultProjectHealthConfig())
			diags := checkRejoiningUpstream(ctx)

			assert.Len(t, diags, tt.wantDiags)
			if tt.wantDiags > 0 {
				assert.Equal(t, "PM07", diags[0].RuleID)
			}
		})
	}
}
