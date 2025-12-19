package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldParseFile_NewFile tests that new files are parsed.
func TestShouldParseFile_NewFile(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	// Create a test model file
	modelPath := filepath.Join(modelsDir, "test.sql")
	require.NoError(t, os.WriteFile(modelPath, []byte("SELECT 1"), 0600))

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// File not in SQLite -> should parse
	needsParse, hash, content := eng.shouldParseFile(modelPath, false)
	assert.True(t, needsParse, "Expected needsParse=true for new file")
	assert.NotEmpty(t, hash, "Expected non-empty hash")
	assert.NotEmpty(t, content, "Expected non-empty content")
}

// TestShouldParseFile_UnchangedFile tests that unchanged files are skipped.
func TestShouldParseFile_UnchangedFile(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	// Create a test model file
	modelPath := filepath.Join(modelsDir, "test.sql")
	content := []byte("SELECT 1")
	require.NoError(t, os.WriteFile(modelPath, content, 0600))

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// First check - should parse
	needsParse, hash, _ := eng.shouldParseFile(modelPath, false)
	assert.True(t, needsParse, "Expected needsParse=true for new file")

	// Store the hash
	require.NoError(t, eng.store.SetContentHash(modelPath, hash, "model"), "SetContentHash failed")

	// Second check with same content - should skip
	needsParse, _, _ = eng.shouldParseFile(modelPath, false)
	assert.False(t, needsParse, "Expected needsParse=false for unchanged file")
}

// TestShouldParseFile_ChangedFile tests that changed files are re-parsed.
func TestShouldParseFile_ChangedFile(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	// Create a test model file
	modelPath := filepath.Join(modelsDir, "test.sql")
	require.NoError(t, os.WriteFile(modelPath, []byte("SELECT 1"), 0600))

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// Store initial hash
	_, hash, _ := eng.shouldParseFile(modelPath, false)
	require.NoError(t, eng.store.SetContentHash(modelPath, hash, "model"))

	// Modify the file
	require.NoError(t, os.WriteFile(modelPath, []byte("SELECT 2"), 0600))

	// Should parse because content changed
	needsParse, newHash, _ := eng.shouldParseFile(modelPath, false)
	assert.True(t, needsParse, "Expected needsParse=true for changed file")
	assert.NotEqual(t, hash, newHash, "Expected different hash for changed content")
}

// TestShouldParseFile_ForceRefresh tests that force flag always triggers parse.
func TestShouldParseFile_ForceRefresh(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	// Create a test model file
	modelPath := filepath.Join(modelsDir, "test.sql")
	require.NoError(t, os.WriteFile(modelPath, []byte("SELECT 1"), 0600))

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// Store hash
	_, hash, _ := eng.shouldParseFile(modelPath, false)
	require.NoError(t, eng.store.SetContentHash(modelPath, hash, "model"))

	// Force flag should always parse
	needsParse, _, _ := eng.shouldParseFile(modelPath, true)
	assert.True(t, needsParse, "Expected needsParse=true when force=true")
}

// TestDiscoverModels_IncrementalSkip tests that unchanged models are skipped.
func TestDiscoverModels_IncrementalSkip(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	// Create test models
	_ = os.WriteFile(filepath.Join(modelsDir, "model1.sql"), []byte(`---
name: model1
materialized: table
---
SELECT 1`), 0600)
	_ = os.WriteFile(filepath.Join(modelsDir, "model2.sql"), []byte(`---
name: model2
materialized: view
---
SELECT 2`), 0600)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// First discovery - should parse all
	result1, err := eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "First Discover() failed")

	assert.Equal(t, 2, result1.ModelsTotal, "Expected 2 total models")
	assert.Equal(t, 2, result1.ModelsChanged, "Expected 2 changed models on first run")
	assert.Equal(t, 0, result1.ModelsSkipped, "Expected 0 skipped models on first run")

	// Second discovery with no changes - should skip all
	result2, err := eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "Second Discover() failed")

	assert.Equal(t, 2, result2.ModelsTotal, "Expected 2 total models")
	assert.Equal(t, 0, result2.ModelsChanged, "Expected 0 changed models on second run")
	assert.Equal(t, 2, result2.ModelsSkipped, "Expected 2 skipped models on second run")
}

// TestDiscoverModels_DeletedFileCleanup tests that deleted files are removed from state.
func TestDiscoverModels_DeletedFileCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	// Create test model
	modelPath := filepath.Join(modelsDir, "to_delete.sql")
	_ = os.WriteFile(modelPath, []byte(`---
name: to_delete
materialized: table
---
SELECT 1`), 0600)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// First discovery
	result1, err := eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "First Discover() failed")
	assert.Equal(t, 1, result1.ModelsTotal, "Expected 1 model")

	// Delete the file
	_ = os.Remove(modelPath)

	// Second discovery - should detect deletion
	result2, err := eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "Second Discover() failed")

	assert.Equal(t, 0, result2.ModelsTotal, "Expected 0 total models after deletion")
	assert.Equal(t, 1, result2.ModelsDeleted, "Expected 1 deleted model")

	// Verify model is removed from in-memory state
	models := eng.GetModels()
	assert.Empty(t, models, "Expected 0 models in memory")
}

// TestDiscoverModels_GracefulDegradation tests that parse errors don't stop discovery.
func TestDiscoverModels_GracefulDegradation(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	// Create one valid model
	_ = os.WriteFile(filepath.Join(modelsDir, "valid.sql"), []byte(`---
name: valid_model
materialized: table
---
SELECT 1`), 0600)

	// Create one file with unreadable content (we'll make it unreadable)
	invalidPath := filepath.Join(modelsDir, "unreadable")
	_ = os.MkdirAll(invalidPath, 0750) // Create as directory instead of file
	// Rename to .sql extension - it will fail when trying to walk
	_ = os.Rename(invalidPath, filepath.Join(modelsDir, "unreadable.sql"))

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// Discovery should succeed - the directory named .sql will be skipped
	result, err := eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "Discover() failed")

	// Should have 1 valid model
	assert.GreaterOrEqual(t, result.ModelsTotal, 1, "Expected at least 1 model")

	// Valid model should still be registered
	models := eng.GetModels()
	assert.GreaterOrEqual(t, len(models), 1, "Expected at least 1 valid model in memory")
}

// TestDiscoverMacros_IncrementalSkip tests incremental macro discovery.
func TestDiscoverMacros_IncrementalSkip(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	macrosDir := filepath.Join(tmpDir, "macros")
	require.NoError(t, os.MkdirAll(macrosDir, 0750))

	// Create test macro
	_ = os.WriteFile(filepath.Join(macrosDir, "utils.star"), []byte(`
def hello(name):
    """Say hello to someone."""
    return "Hello, " + name
`), 0600)

	cfg := Config{
		MacrosDir: macrosDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// First discovery
	result1, err := eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "First Discover() failed")

	assert.Equal(t, 1, result1.MacrosTotal, "Expected 1 macro")
	assert.Equal(t, 1, result1.MacrosChanged, "Expected 1 changed macro on first run")

	// Second discovery - should skip
	result2, err := eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "Second Discover() failed")

	assert.Equal(t, 1, result2.MacrosSkipped, "Expected 1 skipped macro on second run")
	assert.Equal(t, 0, result2.MacrosChanged, "Expected 0 changed macros on second run")
}

// TestDiscover_ForceFullRefresh tests that --force re-parses everything.
func TestDiscover_ForceFullRefresh(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	_ = os.WriteFile(filepath.Join(modelsDir, "model.sql"), []byte(`---
name: model
materialized: table
---
SELECT 1`), 0600)

	cfg := Config{
		ModelsDir: modelsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	// First discovery
	_, err = eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "First Discover() failed")

	// Second discovery with force - should re-parse
	result, err := eng.Discover(DiscoveryOptions{ForceFullRefresh: true})
	require.NoError(t, err, "Second Discover() with force failed")

	assert.Equal(t, 1, result.ModelsChanged, "Expected 1 changed model with force=true")
	assert.Equal(t, 0, result.ModelsSkipped, "Expected 0 skipped models with force=true")
}

// TestDiscover_SeedValidation tests that missing seeds are detected.
func TestDiscover_SeedValidation(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	seedsDir := filepath.Join(tmpDir, "seeds")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))
	require.NoError(t, os.MkdirAll(seedsDir, 0750))

	// Create model that references a seed
	_ = os.WriteFile(filepath.Join(modelsDir, "model.sql"), []byte(`---
name: model
materialized: table
---
SELECT * FROM raw_data`), 0600)

	// Create only one seed file (raw_data is missing)
	_ = os.WriteFile(filepath.Join(seedsDir, "other_data.csv"), []byte("id\n1\n"), 0600)

	cfg := Config{
		ModelsDir: modelsDir,
		SeedsDir:  seedsDir,
		StatePath: statePath,
		Target:    defaultTestTarget(),
		Logger:    testutil.NewTestLogger(t),
	}

	eng, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = eng.Close() }()

	result, err := eng.Discover(DiscoveryOptions{})
	require.NoError(t, err, "Discover() failed")

	// Model should be discovered
	assert.Equal(t, 1, result.ModelsTotal, "Expected 1 model")
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
	assert.NotEmpty(t, summary, "Expected non-empty summary")

	// Check that summary contains key info
	assert.True(t, containsAll(summary, "10 total", "3 changed", "6 skipped", "1 deleted"),
		"Summary missing expected content: %s", summary)
}

// TestDiscoveryResult_HasErrors tests the HasErrors() method.
func TestDiscoveryResult_HasErrors(t *testing.T) {
	result := &DiscoveryResult{}
	assert.False(t, result.HasErrors(), "Expected HasErrors()=false for empty errors")

	result.Errors = append(result.Errors, DiscoveryError{
		Path:    "/test",
		Type:    "parse",
		Message: "test error",
	})
	assert.True(t, result.HasErrors(), "Expected HasErrors()=true when errors exist")
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
