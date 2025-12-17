// Package engine provides tests for the SQL model execution engine.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/leapstack-labs/leapsql/internal/parser"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/leapstack-labs/leapsql/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import adapter packages to ensure adapters are registered via init()
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb"
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/postgres"
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
		require.NoError(t, os.MkdirAll(dir, 0750), "Failed to create dir %s", dir)
	}

	// Create a simple seed
	seedContent := "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n"
	require.NoError(t, os.WriteFile(filepath.Join(seedsDir, "users.csv"), []byte(seedContent), 0600), "Failed to write seed")

	// Create a simple model
	modelContent := `/*---
name: active_users
materialized: table
---*/

SELECT id, name, email FROM users
`
	require.NoError(t, os.WriteFile(filepath.Join(modelsDir, "active_users.sql"), []byte(modelContent), 0600), "Failed to write model")

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
		Logger:       testutil.NewTestLogger(t),
	}

	engine, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = engine.Close() }()

	assert.Nil(t, engine.db, "engine.db should be nil (lazy initialization)")
	assert.False(t, engine.dbConnected, "engine.dbConnected should be false initially")
	assert.NotNil(t, engine.store, "engine.store should not be nil")
	assert.Equal(t, cfg.ModelsDir, engine.modelsDir)
	assert.Equal(t, cfg.SeedsDir, engine.seedsDir)
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
		Logger:       testutil.NewTestLogger(t),
	}

	_, err := New(cfg)
	require.Error(t, err, "New() should fail with invalid state path")
}

func TestLoadSeeds_EmptySeedsDir(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     "", // No seeds dir
		DatabasePath: "",
		StatePath:    statePath,
		Logger:       testutil.NewTestLogger(t),
	}

	engine, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = engine.Close() }()

	ctx := testContext()
	assert.NoError(t, engine.LoadSeeds(ctx), "LoadSeeds() should succeed with empty seeds dir")
}

func TestLoadSeeds_NonexistentSeedsDir(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0750))

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     filepath.Join(tmpDir, "nonexistent"),
		DatabasePath: "",
		StatePath:    statePath,
		Logger:       testutil.NewTestLogger(t),
	}

	engine, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = engine.Close() }()

	ctx := testContext()
	// Should not error, just skip loading
	assert.NoError(t, engine.LoadSeeds(ctx), "LoadSeeds() should succeed with nonexistent seeds dir")
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
		assert.Equal(t, tc.expected, result, "pathToTableName(%q)", tc.path)
	}
}

func TestHashContent(t *testing.T) {
	content1 := "SELECT * FROM users"
	content2 := "SELECT * FROM orders"

	hash1 := hashContent(content1)
	hash2 := hashContent(content2)
	hash1Again := hashContent(content1)

	assert.NotEqual(t, hash1, hash2, "Different content should produce different hashes")
	assert.Equal(t, hash1, hash1Again, "Same content should produce same hash")
	// Check hash is a reasonable length (16 hex chars for 8 bytes)
	assert.Len(t, hash1, 16, "Hash length should be 16")
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
		Logger:       testutil.NewTestLogger(t),
	}

	engine, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = engine.Close() }()

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
	assert.NotContains(t, sql, "{{ this.schema }}", "{{ this.schema }} should be replaced")
	assert.NotContains(t, sql, "{{ this.name }}", "{{ this.name }} should be replaced")

	// Check the actual replacements - should produce marts.summary
	assert.Contains(t, sql, "marts.summary", "{{ this.schema }}.{{ this.name }} should be replaced with 'marts.summary'")

	// Dependencies are auto-detected from SQL, not via ref()
	assert.Contains(t, sql, "staging.customers", "SQL should contain the source table reference")
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
		Logger:       testutil.NewTestLogger(t),
	}

	engine, err := New(cfg)
	require.NoError(t, err, "New() failed")

	// Close should not error
	assert.NoError(t, engine.Close(), "Close() failed")

	// Second close should also not panic/error
	assert.NoError(t, engine.Close(), "Second Close() failed")
}

// TestEngine_CustomModels tests with a minimal custom models directory.
func TestEngine_CustomModels(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.db")
	modelsDir := filepath.Join(tmpDir, "models")
	seedsDir := filepath.Join(tmpDir, "seeds")

	// Create directories
	require.NoError(t, os.MkdirAll(modelsDir, 0750), "Failed to create models dir")
	require.NoError(t, os.MkdirAll(seedsDir, 0750), "Failed to create seeds dir")

	// Create a simple seed
	seedContent := "id,name\n1,Alice\n2,Bob\n"
	require.NoError(t, os.WriteFile(filepath.Join(seedsDir, "users.csv"), []byte(seedContent), 0600), "Failed to write seed")

	// Create a simple model
	modelContent := `-- @config(materialized='table')
SELECT id, name FROM users
`
	require.NoError(t, os.WriteFile(filepath.Join(modelsDir, "active_users.sql"), []byte(modelContent), 0600), "Failed to write model")

	cfg := Config{
		ModelsDir:    modelsDir,
		SeedsDir:     seedsDir,
		DatabasePath: "",
		StatePath:    statePath,
		Logger:       testutil.NewTestLogger(t),
	}

	engine, err := New(cfg)
	require.NoError(t, err, "New() failed")
	defer func() { _ = engine.Close() }()

	ctx := testContext()

	// Load seeds
	require.NoError(t, engine.LoadSeeds(ctx), "LoadSeeds() failed")

	// Discover models
	_, err = engine.Discover(DiscoveryOptions{})
	require.NoError(t, err, "Discover() failed")

	models := engine.GetModels()
	assert.Len(t, models, 1, "Expected 1 model")

	// Run
	run, err := engine.Run(ctx, "dev")
	require.NoError(t, err, "Run() failed")
	assert.Equal(t, state.RunStatus("completed"), run.Status, "Run status should be completed. Error: %s", run.Error)

	// Verify table was created
	rows, err := engine.db.Query(ctx, "SELECT COUNT(*) FROM active_users")
	require.NoError(t, err, "Query active_users failed")

	var count int
	if rows.Next() {
		_ = rows.Scan(&count)
	}
	_ = rows.Close()

	assert.Equal(t, 2, count, "active_users should have 2 rows")
}
