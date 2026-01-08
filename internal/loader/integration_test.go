package loader

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/dag"
	"github.com/leapstack-labs/leapsql/internal/lineage"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"

	// Import duckdb dialect so it registers itself
	_ "github.com/leapstack-labs/leapsql/pkg/dialects/duckdb"
)

// getTestDialect returns the DuckDB dialect for integration tests.
func getTestDialect(t *testing.T) *core.Dialect {
	t.Helper()
	d, ok := dialect.Get("duckdb")
	if !ok {
		t.Fatal("DuckDB dialect not found - ensure duckdb/dialect package is imported")
	}
	return d
}

// integrationLineageExtractor implements LineageExtractor for integration tests.
type integrationLineageExtractor struct{}

func (e *integrationLineageExtractor) Extract(sql string, d *core.Dialect) (*LineageResult, error) {
	result, err := lineage.ExtractLineageWithOptions(sql, lineage.ExtractLineageOptions{
		Dialect: d,
	})
	if err != nil {
		return nil, err
	}

	// Convert lineage.ColumnLineage to core.ColumnInfo
	columns := make([]core.ColumnInfo, 0, len(result.Columns))
	for i, col := range result.Columns {
		columns = append(columns, core.ColumnInfo{
			Name:          col.Name,
			Index:         i,
			TransformType: col.Transform,
			Function:      col.Function,
			Sources:       col.Sources,
		})
	}

	return &LineageResult{
		Sources:        result.Sources,
		Columns:        columns,
		UsesSelectStar: result.UsesSelectStar,
	}, nil
}

// testScanner creates a Scanner with lineage extraction enabled for testing.
func testScanner(t *testing.T, baseDir string) *Scanner {
	t.Helper()
	scanner := NewScanner(baseDir, getTestDialect(t))
	scanner.GetLoader().LineageExtractor = &integrationLineageExtractor{}
	return scanner
}

// TestIntegration_ScanTestDataset tests the parser with the actual test dataset.
func TestIntegration_ScanTestDataset(t *testing.T) {
	// Get the path to testdata relative to this file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}
	testdataPath := filepath.Join(filepath.Dir(filename), "..", "..", "testdata", "models")

	scanner := testScanner(t, testdataPath)
	models, err := scanner.ScanDir(testdataPath)
	if err != nil {
		t.Fatalf("failed to scan testdata: %v", err)
	}

	// Should have 6 models: 3 staging + 4 marts = 7 total
	// Wait, let me count: stg_customers, stg_orders, stg_products, customer_summary, product_summary, executive_dashboard, order_facts
	expectedCount := 7
	if len(models) != expectedCount {
		t.Errorf("expected %d models, got %d", expectedCount, len(models))
		for _, m := range models {
			t.Logf("  found: %s", m.Path)
		}
	}

	// Verify specific models
	modelMap := make(map[string]*core.Model)
	for _, m := range models {
		modelMap[m.Path] = m
	}

	// Check staging models
	if m, ok := modelMap["staging.stg_customers"]; !ok {
		t.Error("missing staging.stg_customers")
	} else if m.Materialized != "table" {
		t.Errorf("stg_customers materialized = %q, expected 'table'", m.Materialized)
	}

	// Check mart with dependencies (now using auto-detected sources)
	if m, ok := modelMap["marts.customer_summary"]; !ok {
		t.Error("missing marts.customer_summary")
	} else if len(m.Sources) != 2 {
		t.Errorf("customer_summary sources = %d, expected 2 (auto-detected from SQL)", len(m.Sources))
	}

	// Check incremental model
	if m, ok := modelMap["marts.order_facts"]; !ok {
		t.Error("missing marts.order_facts")
	} else {
		if m.Materialized != "incremental" {
			t.Errorf("order_facts materialized = %q, expected 'incremental'", m.Materialized)
		}
		if m.UniqueKey != "order_id" {
			t.Errorf("order_facts unique_key = %q, expected 'order_id'", m.UniqueKey)
		}
		// Note: We no longer use #if conditionals - incremental logic is handled by engine
		// based on unique_key and materialized type
		if !m.HasFrontmatter {
			t.Error("order_facts should have frontmatter")
		}
	}

	// Check view
	if m, ok := modelMap["marts.executive_dashboard"]; !ok {
		t.Error("missing marts.executive_dashboard")
	} else if m.Materialized != "view" {
		t.Errorf("executive_dashboard materialized = %q, expected 'view'", m.Materialized)
	}
}

// TestIntegration_BuildDAG builds and validates the DAG from test dataset.
// Uses auto-detected Sources instead of @import pragmas.
func TestIntegration_BuildDAG(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}
	testdataPath := filepath.Join(filepath.Dir(filename), "..", "..", "testdata", "models")

	scanner := testScanner(t, testdataPath)
	models, err := scanner.ScanDir(testdataPath)
	if err != nil {
		t.Fatalf("failed to scan testdata: %v", err)
	}

	// Build DAG
	g := dag.NewGraph()
	for _, m := range models {
		g.AddNode(m.Path, m)
	}

	// Build a lookup to resolve table references to model paths
	// Maps: "staging.stg_customers" → "staging.stg_customers"
	// Also: "stg_customers" → "staging.stg_customers"
	tableLookup := make(map[string]string)
	for _, m := range models {
		tableLookup[m.Path] = m.Path
		// Also add unqualified name
		parts := splitModelPath(m.Path)
		if len(parts) == 2 {
			tableLookup[parts[1]] = m.Path
		}
	}

	// Add edges from auto-detected Sources
	for _, m := range models {
		for _, src := range m.Sources {
			if modelPath, ok := tableLookup[src]; ok {
				if err := g.AddEdge(modelPath, m.Path); err != nil {
					t.Errorf("failed to add edge %s -> %s: %v", modelPath, m.Path, err)
				}
			}
		}
	}

	// Check for cycles
	if hasCycle, cyclePath := g.HasCycle(); hasCycle {
		t.Errorf("unexpected cycle detected: %v", cyclePath)
	}

	// Get execution levels
	levels, err := g.GetExecutionLevels()
	if err != nil {
		t.Fatalf("failed to get execution levels: %v", err)
	}

	// Should have at least 3 levels: staging -> marts -> executive dashboard
	if len(levels) < 3 {
		t.Errorf("expected at least 3 execution levels, got %d", len(levels))
	}

	// Level 0 should contain staging models (no dependencies)
	t.Logf("Execution levels:")
	for i, level := range levels {
		t.Logf("  Level %d: %v", i, level)
	}

	// First level should have staging models
	stagingCount := 0
	for _, id := range levels[0] {
		if len(g.GetParents(id)) == 0 {
			stagingCount++
		}
	}
	if stagingCount != len(levels[0]) {
		t.Error("level 0 should only contain models with no dependencies")
	}

	// executive_dashboard should be in the last level (depends on marts)
	lastLevel := levels[len(levels)-1]
	found := false
	for _, id := range lastLevel {
		if id == "marts.executive_dashboard" {
			found = true
			break
		}
	}
	if !found {
		t.Error("executive_dashboard should be in the last level")
	}

	// Check topological sort
	sorted, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("failed topological sort: %v", err)
	}

	// Verify order: all dependencies should come before dependents
	positions := make(map[string]int)
	for i, node := range sorted {
		positions[node.ID] = i
	}

	// Verify using Sources (auto-detected)
	for _, m := range models {
		for _, src := range m.Sources {
			if modelPath, ok := tableLookup[src]; ok {
				if pos, exists := positions[modelPath]; exists {
					if pos >= positions[m.Path] {
						t.Errorf("dependency %s (pos %d) should come before %s (pos %d)",
							modelPath, pos, m.Path, positions[m.Path])
					}
				}
			}
		}
	}
}

// splitModelPath splits "schema.model" into ["schema", "model"]
func splitModelPath(path string) []string {
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			return []string{path[:i], path[i+1:]}
		}
	}
	return []string{path}
}

// TestIntegration_AffectedNodes tests change impact analysis.
// Uses auto-detected Sources instead of @import pragmas.
func TestIntegration_AffectedNodes(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}
	testdataPath := filepath.Join(filepath.Dir(filename), "..", "..", "testdata", "models")

	scanner := testScanner(t, testdataPath)
	models, err := scanner.ScanDir(testdataPath)
	if err != nil {
		t.Fatalf("failed to scan testdata: %v", err)
	}

	// Build DAG
	g := dag.NewGraph()
	for _, m := range models {
		g.AddNode(m.Path, m)
	}

	// Build a lookup to resolve table references to model paths
	tableLookup := make(map[string]string)
	for _, m := range models {
		tableLookup[m.Path] = m.Path
		parts := splitModelPath(m.Path)
		if len(parts) == 2 {
			tableLookup[parts[1]] = m.Path
		}
	}

	// Add edges from auto-detected Sources
	for _, m := range models {
		for _, src := range m.Sources {
			if modelPath, ok := tableLookup[src]; ok {
				_ = g.AddEdge(modelPath, m.Path)
			}
		}
	}

	// If stg_customers changes, what models are affected?
	affected := g.GetAffectedNodes([]string{"staging.stg_customers"})
	t.Logf("Models affected by stg_customers change: %v", affected)

	// Should include: stg_customers itself, customer_summary, order_facts, executive_dashboard
	expectedAffected := map[string]bool{
		"staging.stg_customers":     true,
		"marts.customer_summary":    true,
		"marts.order_facts":         true,
		"marts.executive_dashboard": true,
	}

	for _, id := range affected {
		if !expectedAffected[id] {
			// Might be affected through other paths, that's OK
			t.Logf("  also affected: %s", id)
		}
	}

	for expected := range expectedAffected {
		found := false
		for _, id := range affected {
			if id == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected %s to be affected", expected)
		}
	}
}
