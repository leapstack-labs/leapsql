package engine

import (
	"os"
	"path/filepath"
	"testing"
)

// TestShouldParseFile_NewFile tests that new files are parsed.
func TestShouldParseFile_NewFile(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	// Create a test model file
	modelPath := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelPath, []byte("SELECT 1"), 0644)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// File not in SQLite -> should parse
	needsParse, hash, content := eng.shouldParseFile(modelPath, false)
	if !needsParse {
		t.Error("Expected needsParse=true for new file")
	}
	if hash == "" {
		t.Error("Expected non-empty hash")
	}
	if len(content) == 0 {
		t.Error("Expected non-empty content")
	}
}

// TestShouldParseFile_UnchangedFile tests that unchanged files are skipped.
func TestShouldParseFile_UnchangedFile(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	// Create a test model file
	modelPath := filepath.Join(modelsDir, "test.sql")
	content := []byte("SELECT 1")
	os.WriteFile(modelPath, content, 0644)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// First check - should parse
	needsParse, hash, _ := eng.shouldParseFile(modelPath, false)
	if !needsParse {
		t.Error("Expected needsParse=true for new file")
	}

	// Store the hash
	if err := eng.store.SetContentHash(modelPath, hash, "model"); err != nil {
		t.Fatalf("SetContentHash failed: %v", err)
	}

	// Second check with same content - should skip
	needsParse, _, _ = eng.shouldParseFile(modelPath, false)
	if needsParse {
		t.Error("Expected needsParse=false for unchanged file")
	}
}

// TestShouldParseFile_ChangedFile tests that changed files are re-parsed.
func TestShouldParseFile_ChangedFile(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	// Create a test model file
	modelPath := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelPath, []byte("SELECT 1"), 0644)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// Store initial hash
	_, hash, _ := eng.shouldParseFile(modelPath, false)
	eng.store.SetContentHash(modelPath, hash, "model")

	// Modify the file
	os.WriteFile(modelPath, []byte("SELECT 2"), 0644)

	// Should parse because content changed
	needsParse, newHash, _ := eng.shouldParseFile(modelPath, false)
	if !needsParse {
		t.Error("Expected needsParse=true for changed file")
	}
	if newHash == hash {
		t.Error("Expected different hash for changed content")
	}
}

// TestShouldParseFile_ForceRefresh tests that force flag always triggers parse.
func TestShouldParseFile_ForceRefresh(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	// Create a test model file
	modelPath := filepath.Join(modelsDir, "test.sql")
	os.WriteFile(modelPath, []byte("SELECT 1"), 0644)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// Store hash
	_, hash, _ := eng.shouldParseFile(modelPath, false)
	eng.store.SetContentHash(modelPath, hash, "model")

	// Force flag should always parse
	needsParse, _, _ := eng.shouldParseFile(modelPath, true)
	if !needsParse {
		t.Error("Expected needsParse=true when force=true")
	}
}

// TestDiscoverModels_IncrementalSkip tests that unchanged models are skipped.
func TestDiscoverModels_IncrementalSkip(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	// Create test models
	os.WriteFile(filepath.Join(modelsDir, "model1.sql"), []byte(`---
name: model1
materialized: table
---
SELECT 1`), 0644)
	os.WriteFile(filepath.Join(modelsDir, "model2.sql"), []byte(`---
name: model2
materialized: view
---
SELECT 2`), 0644)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// First discovery - should parse all
	result1, err := eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("First Discover() failed: %v", err)
	}

	if result1.ModelsTotal != 2 {
		t.Errorf("Expected 2 total models, got %d", result1.ModelsTotal)
	}
	if result1.ModelsChanged != 2 {
		t.Errorf("Expected 2 changed models on first run, got %d", result1.ModelsChanged)
	}
	if result1.ModelsSkipped != 0 {
		t.Errorf("Expected 0 skipped models on first run, got %d", result1.ModelsSkipped)
	}

	// Second discovery with no changes - should skip all
	result2, err := eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Second Discover() failed: %v", err)
	}

	if result2.ModelsTotal != 2 {
		t.Errorf("Expected 2 total models, got %d", result2.ModelsTotal)
	}
	if result2.ModelsChanged != 0 {
		t.Errorf("Expected 0 changed models on second run, got %d", result2.ModelsChanged)
	}
	if result2.ModelsSkipped != 2 {
		t.Errorf("Expected 2 skipped models on second run, got %d", result2.ModelsSkipped)
	}
}

// TestDiscoverModels_DeletedFileCleanup tests that deleted files are removed from state.
func TestDiscoverModels_DeletedFileCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	// Create test model
	modelPath := filepath.Join(modelsDir, "to_delete.sql")
	os.WriteFile(modelPath, []byte(`---
name: to_delete
materialized: table
---
SELECT 1`), 0644)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// First discovery
	result1, err := eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("First Discover() failed: %v", err)
	}

	if result1.ModelsTotal != 1 {
		t.Errorf("Expected 1 model, got %d", result1.ModelsTotal)
	}

	// Delete the file
	os.Remove(modelPath)

	// Second discovery - should detect deletion
	result2, err := eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Second Discover() failed: %v", err)
	}

	if result2.ModelsTotal != 0 {
		t.Errorf("Expected 0 total models after deletion, got %d", result2.ModelsTotal)
	}
	if result2.ModelsDeleted != 1 {
		t.Errorf("Expected 1 deleted model, got %d", result2.ModelsDeleted)
	}

	// Verify model is removed from in-memory state
	models := eng.GetModels()
	if len(models) != 0 {
		t.Errorf("Expected 0 models in memory, got %d", len(models))
	}
}

// TestDiscoverModels_GracefulDegradation tests that parse errors don't stop discovery.
func TestDiscoverModels_GracefulDegradation(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	// Create one valid model
	os.WriteFile(filepath.Join(modelsDir, "valid.sql"), []byte(`---
name: valid_model
materialized: table
---
SELECT 1`), 0644)

	// Create one file with unreadable content (we'll make it unreadable)
	invalidPath := filepath.Join(modelsDir, "unreadable")
	os.MkdirAll(invalidPath, 0755) // Create as directory instead of file
	// Rename to .sql extension - it will fail when trying to walk
	os.Rename(invalidPath, filepath.Join(modelsDir, "unreadable.sql"))

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// Discovery should succeed - the directory named .sql will be skipped
	result, err := eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Should have 1 valid model
	if result.ModelsTotal < 1 {
		t.Errorf("Expected at least 1 model, got %d", result.ModelsTotal)
	}

	// Valid model should still be registered
	models := eng.GetModels()
	if len(models) < 1 {
		t.Errorf("Expected at least 1 valid model in memory, got %d", len(models))
	}
}

// TestDiscoverMacros_IncrementalSkip tests incremental macro discovery.
func TestDiscoverMacros_IncrementalSkip(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	macrosDir := filepath.Join(tmpDir, "macros")
	os.MkdirAll(macrosDir, 0755)

	// Create test macro
	os.WriteFile(filepath.Join(macrosDir, "utils.star"), []byte(`
def hello(name):
    """Say hello to someone."""
    return "Hello, " + name
`), 0644)

	cfg := Config{
		MacrosDir: macrosDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// First discovery
	result1, err := eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("First Discover() failed: %v", err)
	}

	if result1.MacrosTotal != 1 {
		t.Errorf("Expected 1 macro, got %d", result1.MacrosTotal)
	}
	if result1.MacrosChanged != 1 {
		t.Errorf("Expected 1 changed macro on first run, got %d", result1.MacrosChanged)
	}

	// Second discovery - should skip
	result2, err := eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Second Discover() failed: %v", err)
	}

	if result2.MacrosSkipped != 1 {
		t.Errorf("Expected 1 skipped macro on second run, got %d", result2.MacrosSkipped)
	}
	if result2.MacrosChanged != 0 {
		t.Errorf("Expected 0 changed macros on second run, got %d", result2.MacrosChanged)
	}
}

// TestDiscover_ForceFullRefresh tests that --force re-parses everything.
func TestDiscover_ForceFullRefresh(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	os.MkdirAll(modelsDir, 0755)

	os.WriteFile(filepath.Join(modelsDir, "model.sql"), []byte(`---
name: model
materialized: table
---
SELECT 1`), 0644)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	// First discovery
	_, err = eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("First Discover() failed: %v", err)
	}

	// Second discovery with force - should re-parse
	result, err := eng.Discover(DiscoveryOptions{ForceFullRefresh: true})
	if err != nil {
		t.Fatalf("Second Discover() with force failed: %v", err)
	}

	if result.ModelsChanged != 1 {
		t.Errorf("Expected 1 changed model with force=true, got %d", result.ModelsChanged)
	}
	if result.ModelsSkipped != 0 {
		t.Errorf("Expected 0 skipped models with force=true, got %d", result.ModelsSkipped)
	}
}

// TestDiscover_SeedValidation tests that missing seeds are detected.
func TestDiscover_SeedValidation(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	seedsDir := filepath.Join(tmpDir, "seeds")
	os.MkdirAll(modelsDir, 0755)
	os.MkdirAll(seedsDir, 0755)

	// Create model that references a seed
	os.WriteFile(filepath.Join(modelsDir, "model.sql"), []byte(`---
name: model
materialized: table
---
SELECT * FROM raw_data`), 0644)

	// Create only one seed file (raw_data is missing)
	os.WriteFile(filepath.Join(seedsDir, "other_data.csv"), []byte("id\n1\n"), 0644)

	cfg := Config{
		ModelsDir: modelsDir,
		SeedsDir:  seedsDir,
		StatePath: statePath,
	}

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer eng.Close()

	result, err := eng.Discover(DiscoveryOptions{})
	if err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Model should be discovered
	if result.ModelsTotal != 1 {
		t.Errorf("Expected 1 model, got %d", result.ModelsTotal)
	}
}

// TestDiscoveryResult_Summary tests the Summary() method.
func TestDiscoveryResult_Summary(t *testing.T) {
	result := &DiscoveryResult{
		ModelsTotal:   10,
		ModelsChanged: 3,
		ModelsSkipped: 6,
		ModelsDeleted: 1,
		MacrosTotal:   5,
		MacrosChanged: 2,
		MacrosSkipped: 3,
		MacrosDeleted: 0,
	}

	summary := result.Summary()
	if summary == "" {
		t.Error("Expected non-empty summary")
	}

	// Check that summary contains key info
	if !containsAll(summary, "10 total", "3 changed", "6 skipped", "1 deleted") {
		t.Errorf("Summary missing expected content: %s", summary)
	}
}

// TestDiscoveryResult_HasErrors tests the HasErrors() method.
func TestDiscoveryResult_HasErrors(t *testing.T) {
	result := &DiscoveryResult{}
	if result.HasErrors() {
		t.Error("Expected HasErrors()=false for empty errors")
	}

	result.Errors = append(result.Errors, DiscoveryError{
		Path:    "/test",
		Type:    "parse",
		Message: "test error",
	})
	if !result.HasErrors() {
		t.Error("Expected HasErrors()=true when errors exist")
	}
}

// containsAll checks if s contains all substrings.
func containsAll(s string, substrings ...string) bool {
	for _, sub := range substrings {
		found := false
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
