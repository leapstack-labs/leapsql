//go:build integration

// Package engine provides integration tests for the engine execution system.
// Run with: go test -tags=integration ./internal/engine/
package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestIntegration_FullPipeline tests the complete pipeline from seeds to run.
func TestIntegration_FullPipeline(t *testing.T) {
	tmpDir := copyTestdata(t)
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(tmpDir, "models"),
		SeedsDir:     filepath.Join(tmpDir, "seeds"),
		MacrosDir:    filepath.Join(tmpDir, "macros"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Step 1: Load seeds
	t.Log("Loading seeds...")
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}

	// Step 2: Discover models
	t.Log("Discovering models...")
	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	models := engine.GetModels()
	t.Logf("Discovered %d models", len(models))

	// Step 3: Get execution order
	graph := engine.GetGraph()
	sorted, err := graph.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort() failed: %v", err)
	}

	t.Log("Execution order:")
	for i, node := range sorted {
		t.Logf("  %d. %s", i+1, node.ID)
	}

	// Step 4: Run all models
	t.Log("Running all models...")
	run, err := engine.Run(ctx, "test")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	t.Logf("Run ID: %s", run.ID)
	t.Logf("Run Status: %s", run.Status)

	if run.Status != "completed" {
		t.Fatalf("Run failed with status %s: %s", run.Status, run.Error)
	}

	// Step 5: Verify data in tables
	t.Log("Verifying table data...")

	// Check customer summary
	rows, err := engine.db.Query(ctx, "SELECT customer_id, customer_name, total_orders FROM marts.customer_summary ORDER BY customer_id")
	if err != nil {
		t.Fatalf("Query customer_summary failed: %v", err)
	}

	var customerCount int
	for rows.Next() {
		var id int
		var name string
		var orders int
		if err := rows.Scan(&id, &name, &orders); err != nil {
			rows.Close()
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("  Customer %d: %s (%d orders)", id, name, orders)
		customerCount++
	}
	rows.Close()

	if customerCount == 0 {
		t.Error("No customers found in customer_summary")
	}
}

// TestIntegration_FrontmatterParsing tests models with YAML frontmatter.
func TestIntegration_FrontmatterParsing(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	seedsDir := filepath.Join(tmpDir, "seeds")

	// Create directories
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatalf("Failed to create models dir: %v", err)
	}
	if err := os.MkdirAll(seedsDir, 0755); err != nil {
		t.Fatalf("Failed to create seeds dir: %v", err)
	}

	// Create a simple seed
	seedContent := "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n3,Carol,carol@example.com\n"
	if err := os.WriteFile(filepath.Join(seedsDir, "users.csv"), []byte(seedContent), 0644); err != nil {
		t.Fatalf("Failed to write seed: %v", err)
	}

	// Create a model with YAML frontmatter
	modelContent := `/*---
name: active_users
materialized: table
owner: data-team
schema: analytics
tags:
  - users
  - active
meta:
  priority: high
---*/

SELECT id, name, email FROM users
`
	if err := os.WriteFile(filepath.Join(modelsDir, "active_users.sql"), []byte(modelContent), 0644); err != nil {
		t.Fatalf("Failed to write model: %v", err)
	}

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     seedsDir,
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Load seeds
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}

	// Discover models
	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	models := engine.GetModels()
	if len(models) != 1 {
		t.Errorf("Expected 1 model, got %d", len(models))
	}

	// Check that frontmatter was parsed correctly
	model, ok := models["active_users"]
	if !ok {
		t.Fatal("Model 'active_users' not found")
	}

	if model.Name != "active_users" {
		t.Errorf("Model name = %q, want %q", model.Name, "active_users")
	}
	if model.Materialized != "table" {
		t.Errorf("Model materialized = %q, want %q", model.Materialized, "table")
	}
	if model.Owner != "data-team" {
		t.Errorf("Model owner = %q, want %q", model.Owner, "data-team")
	}
	if model.Schema != "analytics" {
		t.Errorf("Model schema = %q, want %q", model.Schema, "analytics")
	}
	if len(model.Tags) != 2 {
		t.Errorf("Model tags count = %d, want 2", len(model.Tags))
	}
	if model.Meta["priority"] != "high" {
		t.Errorf("Model meta.priority = %v, want %q", model.Meta["priority"], "high")
	}
	if !model.HasFrontmatter {
		t.Error("Model should have HasFrontmatter = true")
	}

	// Run
	run, err := engine.Run(ctx, "dev")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	if run.Status != "completed" {
		t.Errorf("Run status = %q, want %q. Error: %s", run.Status, "completed", run.Error)
	}

	// Verify table was created
	rows, err := engine.db.Query(ctx, "SELECT COUNT(*) FROM active_users")
	if err != nil {
		t.Fatalf("Query active_users failed: %v", err)
	}

	var count int
	if rows.Next() {
		rows.Scan(&count)
	}
	rows.Close()

	if count != 3 {
		t.Errorf("active_users has %d rows, want 3", count)
	}
}

// TestIntegration_TemplateRendering tests template rendering with Starlark expressions.
func TestIntegration_TemplateRendering(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	seedsDir := filepath.Join(tmpDir, "seeds")

	// Create directories
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatalf("Failed to create models dir: %v", err)
	}
	if err := os.MkdirAll(seedsDir, 0755); err != nil {
		t.Fatalf("Failed to create seeds dir: %v", err)
	}

	// Create seed
	seedContent := "id,name,status\n1,Alice,active\n2,Bob,inactive\n3,Carol,active\n"
	if err := os.WriteFile(filepath.Join(seedsDir, "users.csv"), []byte(seedContent), 0644); err != nil {
		t.Fatalf("Failed to write seed: %v", err)
	}

	// Create a model with template expressions using 'this'
	modelContent := `/*---
name: user_report
materialized: view
---*/

-- This model uses template expressions
SELECT 
    id,
    name,
    status,
    '{{ this.name }}' as source_model
FROM users
WHERE status = 'active'
`
	if err := os.WriteFile(filepath.Join(modelsDir, "user_report.sql"), []byte(modelContent), 0644); err != nil {
		t.Fatalf("Failed to write model: %v", err)
	}

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     seedsDir,
		DatabasePath: "",
		StatePath:    statePath,
		Environment:  "prod",
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Load seeds
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}

	// Discover models
	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Run
	run, err := engine.Run(ctx, "prod")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	if run.Status != "completed" {
		t.Errorf("Run status = %q, want %q. Error: %s", run.Status, "completed", run.Error)
	}

	// Verify view was created and template was rendered
	rows, err := engine.db.Query(ctx, "SELECT id, source_model FROM user_report ORDER BY id")
	if err != nil {
		t.Fatalf("Query user_report failed: %v", err)
	}

	var count int
	for rows.Next() {
		var id int
		var sourceModel string
		if err := rows.Scan(&id, &sourceModel); err != nil {
			rows.Close()
			t.Fatalf("Scan failed: %v", err)
		}
		// Template expression {{ this.name }} should be rendered
		if sourceModel != "user_report" {
			t.Errorf("source_model = %q, want %q", sourceModel, "user_report")
		}
		count++
	}
	rows.Close()

	// Should have 2 active users
	if count != 2 {
		t.Errorf("user_report has %d rows, want 2", count)
	}
}

// TestIntegration_MacroExpansion tests macro expansion in SQL.
func TestIntegration_MacroExpansion(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	seedsDir := filepath.Join(tmpDir, "seeds")
	macrosDir := filepath.Join(tmpDir, "macros")

	// Create directories
	for _, dir := range []string{modelsDir, seedsDir, macrosDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create dir %s: %v", dir, err)
		}
	}

	// Create seed
	seedContent := "id,name,amount\n1,Alice,100\n2,Bob,200\n3,Carol,150\n"
	if err := os.WriteFile(filepath.Join(seedsDir, "sales.csv"), []byte(seedContent), 0644); err != nil {
		t.Fatalf("Failed to write seed: %v", err)
	}

	// Create a macro file
	macroContent := `# Macro for generating a filter condition
def safe_divide(numerator, denominator, default="0"):
    return "CASE WHEN {} = 0 THEN {} ELSE {} / {} END".format(denominator, default, numerator, denominator)
`
	if err := os.WriteFile(filepath.Join(macrosDir, "utils.star"), []byte(macroContent), 0644); err != nil {
		t.Fatalf("Failed to write macro: %v", err)
	}

	// Create a model that uses the macro
	modelContent := `/*---
name: sales_report
materialized: table
---*/

SELECT 
    id,
    name,
    amount,
    {{ utils.safe_divide("amount", "100") }} as amount_scaled
FROM sales
`
	if err := os.WriteFile(filepath.Join(modelsDir, "sales_report.sql"), []byte(modelContent), 0644); err != nil {
		t.Fatalf("Failed to write model: %v", err)
	}

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     seedsDir,
		MacrosDir:    macrosDir,
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	// Verify macro was loaded
	if engine.macroRegistry == nil {
		t.Fatal("macroRegistry should not be nil")
	}

	// Check if utils namespace exists
	namespaces := engine.macroRegistry.Namespaces()
	found := false
	for _, ns := range namespaces {
		if ns == "utils" {
			found = true
			break
		}
	}
	if !found {
		t.Logf("Available namespaces: %v", namespaces)
		t.Error("Expected 'utils' namespace to be loaded")
	}

	ctx := context.Background()

	// Load seeds
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}

	// Discover models
	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Run
	run, err := engine.Run(ctx, "dev")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	if run.Status != "completed" {
		t.Errorf("Run status = %q, want %q. Error: %s", run.Status, "completed", run.Error)
	}

	// Verify table was created and macro was expanded
	rows, err := engine.db.Query(ctx, "SELECT id, amount_scaled FROM sales_report ORDER BY id")
	if err != nil {
		t.Fatalf("Query sales_report failed: %v", err)
	}

	expectedScaled := []float64{1.0, 2.0, 1.5} // 100/100, 200/100, 150/100
	i := 0
	for rows.Next() {
		var id int
		var scaled float64
		if err := rows.Scan(&id, &scaled); err != nil {
			rows.Close()
			t.Fatalf("Scan failed: %v", err)
		}
		if i < len(expectedScaled) && scaled != expectedScaled[i] {
			t.Errorf("Row %d: amount_scaled = %v, want %v", i, scaled, expectedScaled[i])
		}
		i++
	}
	rows.Close()

	if i != 3 {
		t.Errorf("sales_report has %d rows, want 3", i)
	}
}

// TestIntegration_RunSelected tests running only selected models.
func TestIntegration_RunSelected(t *testing.T) {
	tmpDir := copyTestdata(t)
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(tmpDir, "models"),
		SeedsDir:     filepath.Join(tmpDir, "seeds"),
		MacrosDir:    filepath.Join(tmpDir, "macros"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Load seeds first
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}

	// Discover models
	if _, err := engine.Discover(DiscoveryOptions{}); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Run only staging models
	selectedModels := []string{
		"staging.stg_customers",
		"staging.stg_orders",
	}

	run, err := engine.RunSelected(ctx, "test", selectedModels, false)
	if err != nil {
		t.Fatalf("RunSelected() failed: %v", err)
	}

	if run.Status != "completed" {
		t.Errorf("Run status = %q, want %q. Error: %s", run.Status, "completed", run.Error)
	}

	// Verify staging tables were created
	for _, model := range selectedModels {
		rows, err := engine.db.Query(ctx, "SELECT COUNT(*) FROM "+model)
		if err != nil {
			t.Errorf("Query %s failed: %v", model, err)
			continue
		}
		var count int
		if rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()
		if count == 0 {
			t.Errorf("Table %s has 0 rows", model)
		}
	}
}
