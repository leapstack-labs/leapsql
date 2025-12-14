// Package engine provides tests for the SQL model execution engine.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/parser"
	"github.com/leapstack-labs/leapsql/internal/state"
)

// createTestProject creates a minimal test project in a temp directory.
// Returns the temp dir path. Use t.TempDir() for automatic cleanup.
func createTestProject(t *testing.T) (tmpDir, modelsDir, seedsDir, macrosDir string) {
	t.Helper()
	tmpDir = t.TempDir()
	modelsDir = filepath.Join(tmpDir, "models")
	seedsDir = filepath.Join(tmpDir, "seeds")
	macrosDir = filepath.Join(tmpDir, "macros")

	for _, dir := range []string{modelsDir, seedsDir, macrosDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create dir %s: %v", dir, err)
		}
	}

	// Create a simple seed
	seedContent := "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n"
	if err := os.WriteFile(filepath.Join(seedsDir, "users.csv"), []byte(seedContent), 0644); err != nil {
		t.Fatalf("Failed to write seed: %v", err)
	}

	// Create a simple model
	modelContent := `/*---
name: active_users
materialized: table
---*/

SELECT id, name, email FROM users
`
	if err := os.WriteFile(filepath.Join(modelsDir, "active_users.sql"), []byte(modelContent), 0644); err != nil {
		t.Fatalf("Failed to write model: %v", err)
	}

	return
}

// testContext returns a context for unit tests.
func testContext() context.Context {
	return context.Background()
}

func TestNew(t *testing.T) {
	tmpDir, modelsDir, seedsDir, macrosDir := createTestProject(t)
	statePath := filepath.Join(tmpDir, "state.db")

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     seedsDir,
		MacrosDir:    macrosDir,
		DatabasePath: "", // in-memory DuckDB
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	if engine.db != nil {
		t.Error("engine.db should be nil (lazy initialization)")
	}
	if engine.dbConnected {
		t.Error("engine.dbConnected should be false initially")
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
	tmpDir, modelsDir, seedsDir, macrosDir := createTestProject(t)
	_ = tmpDir

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     seedsDir,
		MacrosDir:    macrosDir,
		DatabasePath: "",
		StatePath:    "/nonexistent/path/state.db",
	}

	_, err := New(cfg)
	if err == nil {
		t.Fatal("New() should fail with invalid state path")
	}
}

func TestLoadSeeds_EmptySeedsDir(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     "", // No seeds dir
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := testContext()
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Errorf("LoadSeeds() should succeed with empty seeds dir, got: %v", err)
	}
}

func TestLoadSeeds_NonexistentSeedsDir(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     filepath.Join(tmpDir, "nonexistent"),
		DatabasePath: "",
		StatePath:    statePath,
	}

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer engine.Close()

	ctx := testContext()
	// Should not error, just skip loading
	if err := engine.LoadSeeds(ctx); err != nil {
		t.Errorf("LoadSeeds() should succeed with nonexistent seeds dir, got: %v", err)
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
	tmpDir, modelsDir, seedsDir, macrosDir := createTestProject(t)
	statePath := filepath.Join(tmpDir, "state.db")

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

	// Create a mock model config - LeapSQL uses pure SQL, dependencies are auto-detected
	modelCfg := &parser.ModelConfig{
		Path:         "marts.summary",
		Name:         "summary",
		Materialized: "table",
		SQL:          "SELECT * FROM staging.customers JOIN {{ this.schema }}.{{ this.name }}",
		Sources:      []string{"staging.customers"},
		FilePath:     filepath.Join(modelsDir, "marts", "summary.sql"),
	}

	model := &state.Model{
		ID:   "test-id",
		Path: "marts.summary",
	}

	sql := engine.buildSQL(modelCfg, model)

	// Check that {{ this.schema }}.{{ this.name }} was replaced
	if strings.Contains(sql, "{{ this.schema }}") || strings.Contains(sql, "{{ this.name }}") {
		t.Error("{{ this.schema }}.{{ this.name }} should be replaced")
	}

	// Check the actual replacements - should produce marts.summary
	if !strings.Contains(sql, "marts.summary") {
		t.Errorf("{{ this.schema }}.{{ this.name }} should be replaced with 'marts.summary', got: %s", sql)
	}

	// Dependencies are auto-detected from SQL, not via ref()
	if !strings.Contains(sql, "staging.customers") {
		t.Error("SQL should contain the source table reference")
	}
}

func TestEngine_Close(t *testing.T) {
	tmpDir, modelsDir, seedsDir, macrosDir := createTestProject(t)
	statePath := filepath.Join(tmpDir, "state.db")

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

	// Close should not error
	if err := engine.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Second close should also not panic/error
	if err := engine.Close(); err != nil {
		t.Errorf("Second Close() failed: %v", err)
	}
}

// TestEngine_CustomModels tests with a minimal custom models directory.
func TestEngine_CustomModels(t *testing.T) {
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

	ctx := testContext()

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
