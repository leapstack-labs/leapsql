package registry

import (
	"testing"

	"github.com/user/dbgo/internal/parser"
)

func TestModelRegistry_Register(t *testing.T) {
	r := NewModelRegistry()

	model := &parser.ModelConfig{
		Path: "staging.stg_customers",
		Name: "stg_customers",
	}

	r.Register(model)

	if r.Count() != 1 {
		t.Errorf("expected count 1, got %d", r.Count())
	}

	// Should be retrievable by path
	got, ok := r.GetModel("staging.stg_customers")
	if !ok {
		t.Error("expected to find model by path")
	}
	if got != model {
		t.Error("expected same model instance")
	}
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
			if gotFound != tt.wantFound {
				t.Errorf("Resolve(%q) found = %v, want %v", tt.tableName, gotFound, tt.wantFound)
			}
			if gotPath != tt.wantPath {
				t.Errorf("Resolve(%q) path = %q, want %q", tt.tableName, gotPath, tt.wantPath)
			}
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
	if len(deps) != 2 {
		t.Errorf("expected 2 dependencies, got %d", len(deps))
	}

	// Verify external sources
	if len(external) != 2 {
		t.Errorf("expected 2 external sources, got %d", len(external))
	}

	// Check specific values
	depsMap := make(map[string]bool)
	for _, d := range deps {
		depsMap[d] = true
	}
	if !depsMap["staging.stg_customers"] {
		t.Error("expected staging.stg_customers in dependencies")
	}
	if !depsMap["staging.stg_orders"] {
		t.Error("expected staging.stg_orders in dependencies")
	}

	externalMap := make(map[string]bool)
	for _, e := range external {
		externalMap[e] = true
	}
	if !externalMap["raw_customers"] {
		t.Error("expected raw_customers in external sources")
	}
	if !externalMap["raw_orders"] {
		t.Error("expected raw_orders in external sources")
	}
}

func TestModelRegistry_ExternalSources(t *testing.T) {
	r := NewModelRegistry()

	r.RegisterExternalSource("raw_customers")
	r.RegisterExternalSource("raw_orders")

	if !r.IsExternalSource("raw_customers") {
		t.Error("expected raw_customers to be external")
	}
	if !r.IsExternalSource("raw_orders") {
		t.Error("expected raw_orders to be external")
	}
	if r.IsExternalSource("stg_customers") {
		t.Error("expected stg_customers to not be external")
	}
}

func TestModelRegistry_AllModels(t *testing.T) {
	r := NewModelRegistry()

	r.Register(&parser.ModelConfig{Path: "staging.stg_customers", Name: "stg_customers"})
	r.Register(&parser.ModelConfig{Path: "staging.stg_orders", Name: "stg_orders"})

	models := r.AllModels()
	if len(models) != 2 {
		t.Errorf("expected 2 models, got %d", len(models))
	}
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
	if len(deps) != 1 {
		t.Errorf("expected 1 dependency (deduplicated), got %d: %v", len(deps), deps)
	}
	if len(external) != 1 {
		t.Errorf("expected 1 external source (deduplicated), got %d: %v", len(external), external)
	}
}
