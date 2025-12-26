package structure

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
	"github.com/stretchr/testify/assert"
)

func TestPS01_ModelNaming(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		wantDiags int
	}{
		{
			name: "staging dir without stg_ prefix",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "customers",
					FilePath: "/models/staging/customers.sql",
					Type:     core.ModelTypeStaging,
				},
			},
			wantDiags: 1,
		},
		{
			name: "staging dir with stg_ prefix - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
				},
			},
			wantDiags: 0,
		},
		{
			name: "intermediate dir without int_ prefix",
			models: map[string]*project.ModelInfo{
				"intermediate.orders": {
					Path:     "intermediate.orders",
					Name:     "orders",
					FilePath: "/models/intermediate/orders.sql",
					Type:     core.ModelTypeIntermediate,
				},
			},
			wantDiags: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, nil, nil, lint.DefaultProjectHealthConfig())
			diags := checkModelNaming(ctx)

			assert.Len(t, diags, tt.wantDiags)
		})
	}
}

func TestPS02_ModelDirectory(t *testing.T) {
	tests := []struct {
		name      string
		models    map[string]*project.ModelInfo
		wantDiags int
	}{
		{
			name: "stg_ prefix not in staging dir",
			models: map[string]*project.ModelInfo{
				"marts.customers": {
					Path:     "marts.customers",
					Name:     "stg_customers",
					FilePath: "/models/marts/stg_customers.sql",
					Type:     core.ModelTypeMarts,
				},
			},
			wantDiags: 1,
		},
		{
			name: "stg_ prefix in staging dir - should not flag",
			models: map[string]*project.ModelInfo{
				"staging.customers": {
					Path:     "staging.customers",
					Name:     "stg_customers",
					FilePath: "/models/staging/stg_customers.sql",
					Type:     core.ModelTypeStaging,
				},
			},
			wantDiags: 0,
		},
		{
			name: "fct_ prefix not in marts dir",
			models: map[string]*project.ModelInfo{
				"staging.orders": {
					Path:     "staging.orders",
					Name:     "fct_orders",
					FilePath: "/models/staging/fct_orders.sql",
					Type:     core.ModelTypeStaging,
				},
			},
			wantDiags: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := project.NewContext(tt.models, nil, nil, lint.DefaultProjectHealthConfig())
			diags := checkModelDirectory(ctx)

			assert.Len(t, diags, tt.wantDiags)
		})
	}
}
