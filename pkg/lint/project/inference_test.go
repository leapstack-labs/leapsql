package project

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/stretchr/testify/assert"
)

func TestInferModelType_Frontmatter(t *testing.T) {
	tests := []struct {
		name     string
		model    *ModelInfo
		expected core.ModelType
	}{
		{
			name: "frontmatter staging override",
			model: &ModelInfo{
				Name:     "customers",
				FilePath: "/models/marts/customers.sql",
				Meta:     map[string]any{"type": "staging"},
			},
			expected: core.ModelTypeStaging,
		},
		{
			name: "frontmatter intermediate override",
			model: &ModelInfo{
				Name:     "orders",
				FilePath: "/models/staging/orders.sql",
				Meta:     map[string]any{"type": "intermediate"},
			},
			expected: core.ModelTypeIntermediate,
		},
		{
			name: "frontmatter marts override",
			model: &ModelInfo{
				Name:     "users",
				FilePath: "/models/staging/users.sql",
				Meta:     map[string]any{"type": "marts"},
			},
			expected: core.ModelTypeMarts,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferModelType(tt.model)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferModelType_Path(t *testing.T) {
	tests := []struct {
		name     string
		model    *ModelInfo
		expected core.ModelType
	}{
		{
			name: "staging path",
			model: &ModelInfo{
				Name:     "customers",
				FilePath: "/models/staging/customers.sql",
			},
			expected: core.ModelTypeStaging,
		},
		{
			name: "intermediate path",
			model: &ModelInfo{
				Name:     "orders",
				FilePath: "/models/intermediate/orders.sql",
			},
			expected: core.ModelTypeIntermediate,
		},
		{
			name: "marts path",
			model: &ModelInfo{
				Name:     "users",
				FilePath: "/models/marts/users.sql",
			},
			expected: core.ModelTypeMarts,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferModelType(tt.model)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferModelType_Prefix(t *testing.T) {
	tests := []struct {
		name     string
		model    *ModelInfo
		expected core.ModelType
	}{
		{
			name: "stg_ prefix",
			model: &ModelInfo{
				Name:     "stg_customers",
				FilePath: "/models/stg_customers.sql",
			},
			expected: core.ModelTypeStaging,
		},
		{
			name: "int_ prefix",
			model: &ModelInfo{
				Name:     "int_orders",
				FilePath: "/models/int_orders.sql",
			},
			expected: core.ModelTypeIntermediate,
		},
		{
			name: "fct_ prefix",
			model: &ModelInfo{
				Name:     "fct_sales",
				FilePath: "/models/fct_sales.sql",
			},
			expected: core.ModelTypeMarts,
		},
		{
			name: "dim_ prefix",
			model: &ModelInfo{
				Name:     "dim_users",
				FilePath: "/models/dim_users.sql",
			},
			expected: core.ModelTypeMarts,
		},
		{
			name: "no match",
			model: &ModelInfo{
				Name:     "customers",
				FilePath: "/models/customers.sql",
			},
			expected: core.ModelTypeOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferModelType(tt.model)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferModelType_Priority(t *testing.T) {
	// Frontmatter should take priority over path
	model := &ModelInfo{
		Name:     "stg_users",
		FilePath: "/models/staging/stg_users.sql",
		Meta:     map[string]any{"type": "marts"},
	}
	assert.Equal(t, core.ModelTypeMarts, InferModelType(model))

	// Path should take priority over prefix
	model = &ModelInfo{
		Name:     "fct_orders",
		FilePath: "/models/staging/fct_orders.sql",
	}
	assert.Equal(t, core.ModelTypeStaging, InferModelType(model))
}
