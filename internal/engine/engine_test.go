// Package engine provides tests for the SQL model execution engine.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/user/dbgo/internal/parser"
	"github.com/user/dbgo/internal/state"
)

// testdataDir returns the path to the testdata directory.
func testdataDir() string {
	// Go up two levels from internal/engine to repo root, then into testdata
	return filepath.Join("..", "..", "testdata")
}

func TestNew(t *testing.T) {
	// Create temp directory for state
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
		DatabasePath: "", // in-memory DuckDB
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	if engine.db == nil {
		t.Error("engine.db should not be nil")
	}
	if engine.store == nil {
		t.Error("engine.store should not be nil")
	}
	if engine.modelsDir != cfg.ModelsDir {
		t.Errorf("engine.modelsDir = %q, want %q", engine.modelsDir, cfg.ModelsDir)
	}
	if engine.seedsDir != cfg.SeedsDir {
		t.Errorf("engine.seedsDir = %q, want %q", engine.seedsDir, cfg.SeedsDir)
	}
}

func TestNew_InvalidStatePath(t *testing.T) {
	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
		DatabasePath: "",
		StatePath:    "/nonexistent/path/state.db",
	}

	_, err := New(cfg)
	if err == nil {
		t.Fatal("New() should fail with invalid state path")
	}
}

func TestLoadSeeds(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Fatalf("LoadSeeds() failed: %v", err)
	}

	// Verify seeds were loaded by querying the tables
	testCases := []struct {
		table    string
		minCount int
	}{
		{"raw_customers", 5},
		{"raw_orders", 1},
		{"raw_products", 1},
	}

	for _, tc := range testCases {
		rows, err := engine.db.Query(ctx, "SELECT COUNT(*) FROM "+tc.table)
		if err != nil {
			t.Errorf("Query %s failed: %v", tc.table, err)
			continue
		}

		var count int
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
				rows.Close()
				t.Errorf("Scan failed for %s: %v", tc.table, err)
				continue
			}
		}
		rows.Close()

		if count < tc.minCount {
			t.Errorf("Table %s has %d rows, want at least %d", tc.table, count, tc.minCount)
		}
	}
}

func TestLoadSeeds_EmptySeedsDir(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     "", // No seeds dir
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Errorf("LoadSeeds() should succeed with empty seeds dir, got: %v", err)
	}
}

func TestLoadSeeds_NonexistentSeedsDir(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(tmpDir, "nonexistent"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()
	// Should not error, just skip loading
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Errorf("LoadSeeds() should succeed with nonexistent seeds dir, got: %v", err)
	}
}

func TestDiscover(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	if err := engine.Discover(); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Check that models were discovered
	models := engine.GetModels()
	if len(models) == 0 {
		t.Fatal("No models discovered")
	}

	// Expected models from testdata
	expectedModels := []string{
		"staging.stg_customers",
		"staging.stg_orders",
		"staging.stg_products",
		"marts.customer_summary",
		"marts.product_summary",
		"marts.executive_dashboard",
		"marts.order_facts",
	}

	for _, expected := range expectedModels {
		if _, ok := models[expected]; !ok {
			t.Errorf("Expected model %q not found", expected)
		}
	}

	// Check DAG was built
	graph := engine.GetGraph()
	if graph.NodeCount() != len(expectedModels) {
		t.Errorf("Graph has %d nodes, want %d", graph.NodeCount(), len(expectedModels))
	}
}

func TestDiscover_DAGDependencies(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	if err := engine.Discover(); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	graph := engine.GetGraph()

	// Staging models should be roots (no dependencies)
	roots := graph.GetRoots()
	t.Logf("DAG roots: %v", roots)

	// customer_summary depends on stg_customers and stg_orders
	parents := graph.GetParents("marts.customer_summary")
	t.Logf("customer_summary parents: %v", parents)

	if len(parents) < 2 {
		t.Errorf("customer_summary should have at least 2 parents, got %d: %v", len(parents), parents)
	}
}

func TestRun(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
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
	if err := engine.Discover(); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Run all models
	run, err := engine.Run(ctx, "test")
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}

	if run == nil {
		t.Fatal("Run() returned nil run")
	}

	if run.Status != "completed" {
		t.Errorf("Run status = %q, want %q. Error: %s", run.Status, "completed", run.Error)
	}

	// Verify tables were created by querying them
	tables := []string{
		"staging.stg_customers",
		"staging.stg_orders",
		"staging.stg_products",
		"marts.customer_summary",
		"marts.product_summary",
	}

	for _, table := range tables {
		rows, err := engine.db.Query(ctx, "SELECT COUNT(*) FROM "+table)
		if err != nil {
			t.Errorf("Query %s failed: %v", table, err)
			continue
		}
		var count int
		if rows.Next() {
			rows.Scan(&count)
		}
		rows.Close()
		t.Logf("Table %s has %d rows", table, count)
	}
}

func TestRunSelected(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
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
	if err := engine.Discover(); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Run only staging models
	selectedModels := []string{
		"staging.stg_customers",
		"staging.stg_orders",
		"staging.stg_products",
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

func TestPathToTableName(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"staging.customers", "staging.customers"},
		{"marts.summary", "marts.summary"},
		{"simple", "simple"},
	}

	for _, tc := range tests {
		result := pathToTableName(tc.path)
		if result != tc.expected {
			t.Errorf("pathToTableName(%q) = %q, want %q", tc.path, result, tc.expected)
		}
	}
}

func TestHashContent(t *testing.T) {
	content1 := "SELECT * FROM users"
	content2 := "SELECT * FROM orders"

	hash1 := hashContent(content1)
	hash2 := hashContent(content2)
	hash1Again := hashContent(content1)

	if hash1 == hash2 {
		t.Error("Different content should produce different hashes")
	}

	if hash1 != hash1Again {
		t.Error("Same content should produce same hash")
	}

	// Check hash is a reasonable length (16 hex chars for 8 bytes)
	if len(hash1) != 16 {
		t.Errorf("Hash length = %d, want 16", len(hash1))
	}
}

func TestBuildSQL(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	// Create a mock model config
	modelCfg := &parser.ModelConfig{
		Path:         "marts.summary",
		Name:         "summary",
		Materialized: "table",
		SQL:          "SELECT * FROM {{ ref('staging.customers') }} JOIN {{ this }}",
		Imports:      []string{"staging.customers"},
	}

	model := &state.Model{
		ID:   "test-id",
		Path: "marts.summary",
	}

	sql := engine.buildSQL(modelCfg, model)

	// Check that {{ this }} was replaced
	if strings.Contains(sql, "{{ this }}") {
		t.Error("{{ this }} should be replaced")
	}

	// Check that {{ ref('staging.customers') }} was replaced
	if strings.Contains(sql, "{{ ref('staging.customers') }}") {
		t.Error("{{ ref('staging.customers') }} should be replaced")
	}

	// Check the actual replacements
	if !strings.Contains(sql, "marts.summary") {
		t.Error("{{ this }} should be replaced with 'marts.summary'")
	}

	if !strings.Contains(sql, "staging.customers") {
		t.Error("ref should be replaced with table name")
	}
}

func TestEngine_Close(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Close should not error
	if err := engine.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Second close should also not panic/error
	if err := engine.Close(); err != nil {
		t.Errorf("Second Close() failed: %v", err)
	}
}

// Integration test: Full pipeline
func TestIntegration_FullPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    filepath.Join(testdataDir(), "models"),
		SeedsDir:     filepath.Join(testdataDir(), "seeds"),
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
	if err := engine.Discover(); err != nil {
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

// Test with custom models directory
func TestEngine_CustomModels(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	seedsDir := filepath.Join(tmpDir, "seeds")

	// Create models directory structure
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatalf("Failed to create models dir: %v", err)
	}
	if err := os.MkdirAll(seedsDir, 0755); err != nil {
		t.Fatalf("Failed to create seeds dir: %v", err)
	}

	// Create a simple seed
	seedContent := "id,name\n1,Alice\n2,Bob\n"
	if err := os.WriteFile(filepath.Join(seedsDir, "users.csv"), []byte(seedContent), 0644); err != nil {
		t.Fatalf("Failed to write seed: %v", err)
	}

	// Create a simple model
	modelContent := `-- @config(materialized='table')
SELECT id, name FROM users
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
	if err := engine.Discover(); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	models := engine.GetModels()
	if len(models) != 1 {
		t.Errorf("Expected 1 model, got %d", len(models))
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

	if count != 2 {
		t.Errorf("active_users has %d rows, want 2", count)
	}
}
