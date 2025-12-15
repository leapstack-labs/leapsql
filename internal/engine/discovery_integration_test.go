//go:build integration

// Package engine provides integration tests for the discovery system.
// Run with: go test -tags=integration ./internal/engine/
package engine

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/leapstack-labs/leapsql/internal/state"
)

// testdataDir returns path to local testdata directory for a scenario.
func testdataDir(scenario string) string {
	return filepath.Join("testdata", scenario)
}

// copyTestdata copies testdata to a temp dir for modification.
func copyTestdata(t *testing.T, scenario string) string {
	t.Helper()
	tmpDir := t.TempDir()

	// Walk source and copy
	srcDir := testdataDir(scenario)
	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Compute relative path
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(tmpDir, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, 0755)
		}

		// Copy file
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()

		dst, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer dst.Close()

		_, err = io.Copy(dst, src)
		return err
	})

	if err != nil {
		t.Fatalf("Failed to copy testdata: %v", err)
	}

	return tmpDir
}

// assertModelInSQLite verifies model exists in state store.
func assertModelInSQLite(t *testing.T, store state.Store, modelPath string) {
	t.Helper()
	model, err := store.GetModelByPath(modelPath)
	if err != nil || model == nil {
		t.Errorf("Model %q not found in SQLite", modelPath)
	}
}

// assertHashInSQLite verifies content hash exists.
func assertHashInSQLite(t *testing.T, store state.Store, filePath string) {
	t.Helper()
	hash, err := store.GetContentHash(filePath)
	if err != nil || hash == "" {
		t.Errorf("Content hash for %q not found in SQLite", filePath)
	}
}

// TestIntegration_FullDiscoveryCycle tests the complete incremental discovery workflow.
func TestIntegration_FullDiscoveryCycle(t *testing.T) {
	// Copy testdata to temp dir for modification
	tmpDir := copyTestdata(t, "basic")
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir: filepath.Join(tmpDir, "models"),
		SeedsDir:  filepath.Join(tmpDir, "seeds"),
		MacrosDir: filepath.Join(tmpDir, "macros"),
		StatePath: statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	// Step 1: First discovery - all files should be parsed
	t.Log("Step 1: First discovery - all files parsed")
	result1, err := engine.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("First Discover() failed: %v", err)
	}

	if result1.ModelsTotal != 3 {
		t.Errorf("Step 1: Expected 3 total models, got %d", result1.ModelsTotal)
	}
	if result1.ModelsChanged != 3 {
		t.Errorf("Step 1: Expected 3 changed models, got %d", result1.ModelsChanged)
	}
	if result1.ModelsSkipped != 0 {
		t.Errorf("Step 1: Expected 0 skipped models, got %d", result1.ModelsSkipped)
	}
	if result1.MacrosTotal != 1 {
		t.Errorf("Step 1: Expected 1 macro, got %d", result1.MacrosTotal)
	}

	// Verify SQLite state
	assertModelInSQLite(t, engine.store, "staging.stg_customers")
	assertModelInSQLite(t, engine.store, "staging.stg_orders")
	assertModelInSQLite(t, engine.store, "marts.customer_summary")

	// Verify in-memory models
	models := engine.GetModels()
	if len(models) != 3 {
		t.Errorf("Step 1: Expected 3 in-memory models, got %d", len(models))
	}

	// Verify DAG
	graph := engine.GetGraph()
	if graph.NodeCount() != 3 {
		t.Errorf("Step 1: Expected 3 DAG nodes, got %d", graph.NodeCount())
	}

	// Step 2: Second discovery - no changes, should skip all
	t.Log("Step 2: Second discovery - no changes")
	start := time.Now()
	result2, err := engine.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Second Discover() failed: %v", err)
	}
	duration := time.Since(start)

	if result2.ModelsChanged != 0 {
		t.Errorf("Step 2: Expected 0 changed models, got %d", result2.ModelsChanged)
	}
	if result2.ModelsSkipped != 3 {
		t.Errorf("Step 2: Expected 3 skipped models, got %d", result2.ModelsSkipped)
	}

	// Fast path should be quick (< 100ms for 3 models)
	if duration > 100*time.Millisecond {
		t.Logf("Step 2: Warning - second discovery took %v (expected < 100ms)", duration)
	}

	// Step 3: Modify one file
	t.Log("Step 3: Modify one file")
	stgCustomersPath := filepath.Join(tmpDir, "models", "staging", "stg_customers.sql")
	modifiedContent := `/*---
name: stg_customers
materialized: table
---*/

SELECT 
    id,
    name,
    email,
    'modified' as flag
FROM raw_customers
`
	if err := os.WriteFile(stgCustomersPath, []byte(modifiedContent), 0644); err != nil {
		t.Fatalf("Failed to modify file: %v", err)
	}

	result3, err := engine.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Third Discover() failed: %v", err)
	}

	if result3.ModelsChanged != 1 {
		t.Errorf("Step 3: Expected 1 changed model, got %d", result3.ModelsChanged)
	}
	if result3.ModelsSkipped != 2 {
		t.Errorf("Step 3: Expected 2 skipped models, got %d", result3.ModelsSkipped)
	}

	// Step 4: Add new file
	t.Log("Step 4: Add new file")
	newModelPath := filepath.Join(tmpDir, "models", "staging", "stg_new.sql")
	newModelContent := `/*---
name: stg_new
materialized: table
---*/

SELECT 1 as id, 'new' as name
`
	if err := os.WriteFile(newModelPath, []byte(newModelContent), 0644); err != nil {
		t.Fatalf("Failed to create new file: %v", err)
	}

	result4, err := engine.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Fourth Discover() failed: %v", err)
	}

	if result4.ModelsTotal != 4 {
		t.Errorf("Step 4: Expected 4 total models, got %d", result4.ModelsTotal)
	}
	if result4.ModelsChanged != 1 {
		t.Errorf("Step 4: Expected 1 changed model (new file), got %d", result4.ModelsChanged)
	}
	if result4.ModelsSkipped != 3 {
		t.Errorf("Step 4: Expected 3 skipped models, got %d", result4.ModelsSkipped)
	}

	// Verify new model in memory
	if _, ok := engine.GetModels()["staging.stg_new"]; !ok {
		t.Error("Step 4: New model not found in memory")
	}

	// Step 5: Delete a file
	t.Log("Step 5: Delete a file")
	if err := os.Remove(newModelPath); err != nil {
		t.Fatalf("Failed to delete file: %v", err)
	}

	result5, err := engine.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Fifth Discover() failed: %v", err)
	}

	if result5.ModelsTotal != 3 {
		t.Errorf("Step 5: Expected 3 total models, got %d", result5.ModelsTotal)
	}
	if result5.ModelsDeleted != 1 {
		t.Errorf("Step 5: Expected 1 deleted model, got %d", result5.ModelsDeleted)
	}

	// Verify deleted model removed from memory
	if _, ok := engine.GetModels()["staging.stg_new"]; ok {
		t.Error("Step 5: Deleted model should not be in memory")
	}

	// Step 6: Force refresh
	t.Log("Step 6: Force refresh")
	result6, err := engine.Discover(DiscoveryOptions{ForceFullRefresh: true})
	if err != nil {
		t.Fatalf("Sixth Discover() with force failed: %v", err)
	}

	if result6.ModelsChanged != 3 {
		t.Errorf("Step 6: Expected 3 changed models with force=true, got %d", result6.ModelsChanged)
	}
	if result6.ModelsSkipped != 0 {
		t.Errorf("Step 6: Expected 0 skipped models with force=true, got %d", result6.ModelsSkipped)
	}
	if result6.MacrosChanged != 1 {
		t.Errorf("Step 6: Expected 1 changed macro with force=true, got %d", result6.MacrosChanged)
	}
}

// TestIntegration_DiscoveryThenRun tests the workflow from discovery through execution.
func TestIntegration_DiscoveryThenRun(t *testing.T) {
	// Copy testdata to temp dir
	tmpDir := copyTestdata(t, "basic")
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(tmpDir, "models"),
		SeedsDir:     filepath.Join(tmpDir, "seeds"),
		MacrosDir:    filepath.Join(tmpDir, "macros"),
		DatabasePath: "", // in-memory DuckDB
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	// Step 1: Create fresh engine - verify db is nil (lazy init)
	if engine.db != nil {
		t.Error("Step 1: db should be nil initially (lazy initialization)")
	}

	// Step 2: Run discovery
	t.Log("Running discovery...")
	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Step 3: Verify db is still nil after discovery
	if engine.db != nil {
		t.Error("Step 3: db should still be nil after discovery (discovery doesn't need DB)")
	}

	// Step 4: Load seeds
	t.Log("Loading seeds...")
	ctx := context.Background()
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}

	// Step 5: Verify db is now connected
	if engine.db == nil {
		t.Error("Step 5: db should be connected after LoadSeeds()")
	}

	// Step 6: Run all models
	t.Log("Running all models...")
	run, err := engine.Run(ctx, "integration-test")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	if run.Status != "completed" {
		t.Errorf("Step 6: Run status = %q, want %q. Error: %s", run.Status, "completed", run.Error)
	}

	// Step 7: Query result tables to verify data
	t.Log("Verifying results...")

	// Check staging tables
	rows, err := engine.db.Query(ctx, "SELECT COUNT(*) FROM staging.stg_customers")
	if err != nil {
		t.Errorf("Query stg_customers failed: %v", err)
	} else {
		var count int
		if rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()
		if count != 3 {
			t.Errorf("stg_customers has %d rows, want 3", count)
		}
	}

	rows, err = engine.db.Query(ctx, "SELECT COUNT(*) FROM staging.stg_orders")
	if err != nil {
		t.Errorf("Query stg_orders failed: %v", err)
	} else {
		var count int
		if rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()
		if count != 4 {
			t.Errorf("stg_orders has %d rows, want 4", count)
		}
	}

	// Check marts table
	rows, err = engine.db.Query(ctx, "SELECT customer_id, customer_name, total_orders, total_amount FROM marts.customer_summary ORDER BY customer_id")
	if err != nil {
		t.Errorf("Query customer_summary failed: %v", err)
	} else {
		var customers int
		for rows.Next() {
			var id int
			var name string
			var orders, amount int
			rows.Scan(&id, &name, &orders, &amount)
			t.Logf("  Customer %d (%s): %d orders, $%d", id, name, orders, amount)
			customers++
		}
		rows.Close()
		if customers != 3 {
			t.Errorf("customer_summary has %d rows, want 3", customers)
		}
	}
}

// TestIntegration_DiscoveryStateConsistency tests that in-memory state matches SQLite.
func TestIntegration_DiscoveryStateConsistency(t *testing.T) {
	tmpDir := copyTestdata(t, "basic")
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir: filepath.Join(tmpDir, "models"),
		SeedsDir:  filepath.Join(tmpDir, "seeds"),
		MacrosDir: filepath.Join(tmpDir, "macros"),
		StatePath: statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	// Run discovery
	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// For each model in memory, verify SQLite record exists
	models := engine.GetModels()
	for modelPath := range models {
		model, err := engine.store.GetModelByPath(modelPath)
		if err != nil {
			t.Errorf("Model %q not found in SQLite: %v", modelPath, err)
			continue
		}
		if model == nil {
			t.Errorf("Model %q returned nil from SQLite", modelPath)
			continue
		}
		if model.Path != modelPath {
			t.Errorf("SQLite model path = %q, want %q", model.Path, modelPath)
		}
		if model.FilePath == "" {
			t.Errorf("Model %q has empty FilePath in SQLite", modelPath)
		}

		// Verify content hash exists
		hash, err := engine.store.GetContentHash(model.FilePath)
		if err != nil || hash == "" {
			t.Errorf("Content hash for %q not found in SQLite", model.FilePath)
		}
	}

	// Verify no orphan records in SQLite
	sqliteModels, err := engine.store.ListModels()
	if err != nil {
		t.Fatalf("ListModels() failed: %v", err)
	}

	for _, sqliteModel := range sqliteModels {
		if _, ok := models[sqliteModel.Path]; !ok {
			t.Errorf("Orphan model in SQLite: %q (not in memory)", sqliteModel.Path)
		}
	}

	// Verify dependency edges in graph match SQLite
	graph := engine.GetGraph()
	for modelPath := range models {
		model, _ := engine.store.GetModelByPath(modelPath)
		if model == nil {
			continue
		}

		// Get parents from graph
		graphParents := graph.GetParents(modelPath)

		// Get dependencies from SQLite
		sqliteDeps, err := engine.store.GetDependencies(model.ID)
		if err != nil {
			t.Errorf("GetDependencies(%s) failed: %v", modelPath, err)
			continue
		}

		// Convert SQLite dep IDs to paths for comparison
		if len(graphParents) != len(sqliteDeps) {
			t.Errorf("Model %q: graph has %d parents, SQLite has %d dependencies",
				modelPath, len(graphParents), len(sqliteDeps))
		}
	}
}

// TestIntegration_DiscoveryWithMacroChanges tests macro change detection.
func TestIntegration_DiscoveryWithMacroChanges(t *testing.T) {
	tmpDir := copyTestdata(t, "basic")
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir: filepath.Join(tmpDir, "models"),
		SeedsDir:  filepath.Join(tmpDir, "seeds"),
		MacrosDir: filepath.Join(tmpDir, "macros"),
		StatePath: statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	// Step 1: Initial discovery
	result1, err := engine.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("First Discover() failed: %v", err)
	}

	if result1.MacrosTotal != 1 {
		t.Errorf("Step 1: Expected 1 macro, got %d", result1.MacrosTotal)
	}
	if result1.MacrosChanged != 1 {
		t.Errorf("Step 1: Expected 1 changed macro, got %d", result1.MacrosChanged)
	}

	// Step 2: Modify macro file
	macroPath := filepath.Join(tmpDir, "macros", "utils.star")
	modifiedMacro := `# Updated utility macros

def safe_divide(numerator, denominator, default="0"):
    """Safely divide two values, returning default if denominator is zero. (UPDATED)"""
    return "COALESCE(NULLIF({}, 0), {})".format(denominator, default)

def cents_to_dollars(column):
    """Convert cents to dollars."""
    return "({} / 100.0)".format(column)

def new_function():
    """A new function added."""
    return "NEW"
`
	if err := os.WriteFile(macroPath, []byte(modifiedMacro), 0644); err != nil {
		t.Fatalf("Failed to modify macro: %v", err)
	}

	// Step 3: Re-discover
	result2, err := engine.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Second Discover() failed: %v", err)
	}

	if result2.MacrosChanged != 1 {
		t.Errorf("Step 3: Expected 1 changed macro, got %d", result2.MacrosChanged)
	}
	if result2.MacrosSkipped != 0 {
		t.Errorf("Step 3: Expected 0 skipped macros, got %d", result2.MacrosSkipped)
	}

	// Step 4: Create a model that uses the macro and run
	modelWithMacro := filepath.Join(tmpDir, "models", "staging", "stg_with_macro.sql")
	modelContent := `/*---
name: stg_with_macro
materialized: table
---*/

SELECT 
    1 as id,
    {{ utils.cents_to_dollars("100") }} as amount_dollars
`
	if err := os.WriteFile(modelWithMacro, []byte(modelContent), 0644); err != nil {
		t.Fatalf("Failed to create model with macro: %v", err)
	}

	result3, err := engine.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Third Discover() failed: %v", err)
	}

	if result3.ModelsTotal != 4 {
		t.Errorf("Step 4: Expected 4 total models, got %d", result3.ModelsTotal)
	}

	// Step 5: Run model and verify macro expansion
	ctx := context.Background()
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}

	run, err := engine.Run(ctx, "test")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	if run.Status != "completed" {
		t.Errorf("Step 5: Run status = %q, want %q. Error: %s", run.Status, "completed", run.Error)
	}

	// Verify macro was expanded correctly
	rows, err := engine.db.Query(ctx, "SELECT amount_dollars FROM staging.stg_with_macro")
	if err != nil {
		t.Errorf("Query stg_with_macro failed: %v", err)
	} else {
		if rows.Next() {
			var amount float64
			rows.Scan(&amount)
			// 100 cents / 100.0 = 1.0 dollars
			if amount != 1.0 {
				t.Errorf("amount_dollars = %v, want 1.0 (macro should compute 100/100.0)", amount)
			}
		}
		rows.Close()
	}
}

// TestIntegration_DAGDependencies tests that DAG dependencies are correctly resolved.
func TestIntegration_DAGDependencies(t *testing.T) {
	tmpDir := copyTestdata(t, "basic")
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir: filepath.Join(tmpDir, "models"),
		SeedsDir:  filepath.Join(tmpDir, "seeds"),
		MacrosDir: filepath.Join(tmpDir, "macros"),
		StatePath: statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	graph := engine.GetGraph()

	// Staging models should be roots (no dependencies)
	roots := graph.GetRoots()
	t.Logf("DAG roots: %v", roots)

	// Verify customer_summary depends on stg_customers and stg_orders
	parents := graph.GetParents("marts.customer_summary")
	t.Logf("customer_summary parents: %v", parents)

	if len(parents) < 2 {
		t.Errorf("customer_summary should have at least 2 parents, got %d: %v", len(parents), parents)
	}

	// Check that parents contain expected models
	parentSet := make(map[string]bool)
	for _, p := range parents {
		parentSet[p] = true
	}

	if !parentSet["staging.stg_customers"] {
		t.Error("customer_summary should depend on staging.stg_customers")
	}
	if !parentSet["staging.stg_orders"] {
		t.Error("customer_summary should depend on staging.stg_orders")
	}

	// Verify topological sort works
	sorted, err := graph.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort() failed: %v", err)
	}

	t.Log("Topological order:")
	for i, node := range sorted {
		t.Logf("  %d. %s", i+1, node.ID)
	}

	// customer_summary should be last (depends on staging models)
	lastModel := sorted[len(sorted)-1].ID
	if lastModel != "marts.customer_summary" {
		t.Errorf("Expected marts.customer_summary to be last in topological sort, got %s", lastModel)
	}
}

// TestIntegration_LineageExtraction verifies that lineage extraction integrates correctly with discovery.
// Detailed lineage parsing is tested in the parser package; this test verifies the integration.
func TestIntegration_LineageExtraction(t *testing.T) {
	tmpDir := copyTestdata(t, "basic")
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir: filepath.Join(tmpDir, "models"),
		SeedsDir:  filepath.Join(tmpDir, "seeds"),
		MacrosDir: filepath.Join(tmpDir, "macros"),
		StatePath: statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	models := engine.GetModels()

	// Verify all models have Sources populated (lineage extraction worked)
	for path, model := range models {
		if len(model.Sources) == 0 {
			t.Errorf("Model %q has empty Sources - lineage extraction failed", path)
		}
		if len(model.Columns) == 0 {
			t.Errorf("Model %q has empty Columns - column lineage extraction failed", path)
		}
	}

	// Verify Sources feeds into DAG correctly
	// customer_summary depends on staging tables via Sources
	customerSummary := models["marts.customer_summary"]
	if customerSummary == nil {
		t.Fatal("marts.customer_summary not found")
	}

	// Sources should contain the staging tables
	sourcesSet := make(map[string]bool)
	for _, s := range customerSummary.Sources {
		sourcesSet[s] = true
	}

	if !sourcesSet["staging.stg_customers"] {
		t.Errorf("customer_summary.Sources should contain staging.stg_customers, got %v", customerSummary.Sources)
	}
	if !sourcesSet["staging.stg_orders"] {
		t.Errorf("customer_summary.Sources should contain staging.stg_orders, got %v", customerSummary.Sources)
	}

	// Verify DAG was built from Sources (not empty)
	graph := engine.GetGraph()
	parents := graph.GetParents("marts.customer_summary")
	if len(parents) < 2 {
		t.Errorf("DAG should have dependencies from Sources, got %d parents: %v", len(parents), parents)
	}
}
