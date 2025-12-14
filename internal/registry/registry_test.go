package registry

import (
	"testing"

	"github.com/leapstack-labs/leapsql/internal/parser"
	"github.com/stretchr/testify/assert"
)

func TestModelRegistry_Register(t *testing.T) {
	r := NewModelRegistry()

	model := &parser.ModelConfig{
		Path: "staging.stg_customers",
		Name: "stg_customers",
	}

	r.Register(model)

	assert.Equal(t, 1, r.Count(), "expected count 1")

	// Should be retrievable by path
	got, ok := r.GetModel("staging.stg_customers")
	assert.True(t, ok, "expected to find model by path")
	assert.Equal(t, model, got, "expected same model instance")
}

func TestModelRegistry_Resolve(t *testing.T) {
	r := NewModelRegistry()

	// Register multiple models
	r.Register(&parser.ModelConfig{Path: "staging.stg_customers", Name: "stg_customers"})
	r.Register(&parser.ModelConfig{Path: "staging.stg_orders", Name: "stg_orders"})
	r.Register(&parser.ModelConfig{Path: "marts.customer_summary", Name: "customer_summary"})

	tests := []struct {
		name      string
		tableName string
		wantPath  string
		wantFound bool
	}{
		// Exact path match
		{
			name:      "exact path match",
			tableName: "staging.stg_customers",
			wantPath:  "staging.stg_customers",
			wantFound: true,
		},
		// Unqualified name
		{
			name:      "unqualified name",
			tableName: "stg_customers",
			wantPath:  "staging.stg_customers",
			wantFound: true,
		},
		// Different schema prefix (simulating database schema vs model path)
		{
			name:      "different schema prefix",
			tableName: "public.stg_customers",
			wantPath:  "staging.stg_customers",
			wantFound: true,
		},
		// Non-existent model
		{
			name:      "non-existent model",
			tableName: "raw_customers",
			wantPath:  "",
			wantFound: false,
		},
		// Non-existent qualified
		{
			name:      "non-existent qualified",
			tableName: "other.unknown_table",
			wantPath:  "",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotFound := r.Resolve(tt.tableName)
			assert.Equal(t, tt.wantFound, gotFound, "Resolve(%q) found", tt.tableName)
			assert.Equal(t, tt.wantPath, gotPath, "Resolve(%q) path", tt.tableName)
		})
	}
}

func TestModelRegistry_ResolveDependencies(t *testing.T) {
	r := NewModelRegistry()

	// Register models
	r.Register(&parser.ModelConfig{Path: "staging.stg_customers", Name: "stg_customers"})
	r.Register(&parser.ModelConfig{Path: "staging.stg_orders", Name: "stg_orders"})

	// Input: mix of model references and external sources
	tableNames := []string{
		"staging.stg_customers",
		"staging.stg_orders",
		"raw_customers", // external
		"raw_orders",    // external
	}

	deps, external := r.ResolveDependencies(tableNames)

	// Verify dependencies
	assert.Len(t, deps, 2, "expected 2 dependencies")

	// Verify external sources
	assert.Len(t, external, 2, "expected 2 external sources")

	// Check specific values
	assert.Contains(t, deps, "staging.stg_customers", "expected staging.stg_customers in dependencies")
	assert.Contains(t, deps, "staging.stg_orders", "expected staging.stg_orders in dependencies")

	assert.Contains(t, external, "raw_customers", "expected raw_customers in external sources")
	assert.Contains(t, external, "raw_orders", "expected raw_orders in external sources")
}

func TestModelRegistry_ExternalSources(t *testing.T) {
	r := NewModelRegistry()

	r.RegisterExternalSource("raw_customers")
	r.RegisterExternalSource("raw_orders")

	assert.True(t, r.IsExternalSource("raw_customers"), "expected raw_customers to be external")
	assert.True(t, r.IsExternalSource("raw_orders"), "expected raw_orders to be external")
	assert.False(t, r.IsExternalSource("stg_customers"), "expected stg_customers to not be external")
}

func TestModelRegistry_AllModels(t *testing.T) {
	r := NewModelRegistry()

	r.Register(&parser.ModelConfig{Path: "staging.stg_customers", Name: "stg_customers"})
	r.Register(&parser.ModelConfig{Path: "staging.stg_orders", Name: "stg_orders"})

	models := r.AllModels()
	assert.Len(t, models, 2, "expected 2 models")
}

func TestModelRegistry_Deduplication(t *testing.T) {
	r := NewModelRegistry()

	r.Register(&parser.ModelConfig{Path: "staging.stg_customers", Name: "stg_customers"})

	// Input with duplicates
	tableNames := []string{
		"staging.stg_customers",
		"stg_customers",         // same model, different reference
		"staging.stg_customers", // duplicate
		"raw_customers",
		"raw_customers", // duplicate
	}

	deps, external := r.ResolveDependencies(tableNames)

	// Should deduplicate
	assert.Len(t, deps, 1, "expected 1 dependency (deduplicated)")
	assert.Len(t, external, 1, "expected 1 external source (deduplicated)")
}
