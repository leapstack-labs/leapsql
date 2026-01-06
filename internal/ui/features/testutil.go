// Package features provides shared test utilities for UI feature tests.
package features

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/stretchr/testify/require"

	"github.com/leapstack-labs/leapsql/internal/engine"
	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/leapstack-labs/leapsql/internal/testutil"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"

	// Import adapter packages to ensure adapters are registered via init()
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb"
)

// TestModel is a helper to create test models with minimal boilerplate.
type TestModel struct {
	Path         string
	Name         string
	SQL          string
	Materialized string
	Description  string
	Schema       string
	Owner        string
	Tags         []string
}

// TestFixture holds all dependencies needed for UI handler tests.
type TestFixture struct {
	Store        core.Store
	Engine       *engine.Engine
	Notifier     *notifier.Notifier
	SessionStore *sessions.CookieStore

	// For cleanup
	t      *testing.T
	tmpDir string
}

// SetupTestFixture creates a complete test fixture with store, engine, and notifier.
// The engine is configured with an in-memory DuckDB and the provided test models.
func SetupTestFixture(t *testing.T, models ...TestModel) *TestFixture {
	t.Helper()

	logger := testutil.NewTestLogger(t)

	// Create temp directory for test project
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models")
	macrosDir := filepath.Join(tmpDir, "macros")
	statePath := filepath.Join(tmpDir, "state.db")

	require.NoError(t, os.MkdirAll(modelsDir, 0750))
	require.NoError(t, os.MkdirAll(macrosDir, 0750))

	// Write model files, supporting nested paths like "staging.customers"
	for _, m := range models {
		content := buildModelFile(m)

		// Convert path like "staging.customers" to "staging/customers.sql"
		pathParts := strings.Split(m.Path, ".")
		if len(pathParts) > 1 {
			// Create subdirectory
			subDir := filepath.Join(modelsDir, filepath.Join(pathParts[:len(pathParts)-1]...))
			require.NoError(t, os.MkdirAll(subDir, 0750))
			filename := pathParts[len(pathParts)-1] + ".sql"
			require.NoError(t, os.WriteFile(filepath.Join(subDir, filename), []byte(content), 0600))
		} else {
			// Simple filename
			filename := m.Name + ".sql"
			require.NoError(t, os.WriteFile(filepath.Join(modelsDir, filename), []byte(content), 0600))
		}
	}

	// Create engine
	cfg := engine.Config{
		ModelsDir: modelsDir,
		MacrosDir: macrosDir,
		StatePath: statePath,
		Target: &starctx.TargetInfo{
			Type:   "duckdb",
			Schema: "main",
		},
		Logger: logger,
	}

	eng, err := engine.New(cfg)
	require.NoError(t, err)

	// Run discovery to populate store
	_, err = eng.Discover(engine.DiscoveryOptions{})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = eng.Close()
	})

	return &TestFixture{
		Store:        eng.GetStateStore(),
		Engine:       eng,
		Notifier:     notifier.New(),
		SessionStore: sessions.NewCookieStore([]byte("test-secret-key-32-bytes-long!!")),
		t:            t,
		tmpDir:       tmpDir,
	}
}

// SetupTestStore creates an in-memory store with the provided test models.
// Use this when you don't need the engine (e.g., for simpler handler tests).
func SetupTestStore(t *testing.T, models ...TestModel) core.Store {
	t.Helper()

	logger := testutil.NewTestLogger(t)
	store := state.NewSQLiteStore(logger)
	require.NoError(t, store.Open(":memory:"))
	require.NoError(t, store.InitSchema())

	t.Cleanup(func() {
		_ = store.Close()
	})

	// Register models
	for _, m := range models {
		pm := toPersistedModel(m)
		require.NoError(t, store.RegisterModel(pm))
	}

	return store
}

// RequestWithPathParam wraps a request with chi URL params.
func RequestWithPathParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// RequestWithTimeout wraps a request with a context timeout.
func RequestWithTimeout(r *http.Request, timeout time.Duration) *http.Request {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	// Note: caller should handle cleanup, but for tests the timeout will trigger
	_ = cancel // suppress lint warning, context will be cancelled by timeout
	return r.WithContext(ctx)
}

// NewTestNotifier creates a notifier for testing.
func NewTestNotifier() *notifier.Notifier {
	return notifier.New()
}

// NewTestSessionStore creates a session store for testing.
func NewTestSessionStore() *sessions.CookieStore {
	return sessions.NewCookieStore([]byte("test-secret-key-32-bytes-long!!"))
}

// buildModelFile creates a SQL model file content with frontmatter.
func buildModelFile(m TestModel) string {
	materialized := m.Materialized
	if materialized == "" {
		materialized = "view"
	}

	sql := m.SQL
	if sql == "" {
		sql = "SELECT 1 AS id"
	}

	content := "/*---\n"
	content += "name: " + m.Name + "\n"
	content += "materialized: " + materialized + "\n"
	if m.Description != "" {
		content += "description: " + m.Description + "\n"
	}
	if m.Schema != "" {
		content += "schema: " + m.Schema + "\n"
	}
	if m.Owner != "" {
		content += "owner: " + m.Owner + "\n"
	}
	content += "---*/\n\n"
	content += sql + "\n"

	return content
}

// toPersistedModel converts a TestModel to a core.PersistedModel.
func toPersistedModel(m TestModel) *core.PersistedModel {
	materialized := m.Materialized
	if materialized == "" {
		materialized = "view"
	}

	sql := m.SQL
	if sql == "" {
		sql = "SELECT 1 AS id"
	}

	return &core.PersistedModel{
		Model: &core.Model{
			Path:         m.Path,
			Name:         m.Name,
			SQL:          sql,
			Materialized: materialized,
			Description:  m.Description,
			Schema:       m.Schema,
			Owner:        m.Owner,
			Tags:         m.Tags,
		},
		ContentHash: "test-hash",
	}
}
