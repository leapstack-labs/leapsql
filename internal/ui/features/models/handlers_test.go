package models

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/leapstack-labs/leapsql/internal/ui/features"
)

// =============================================================================
// Test Setup Helpers
// =============================================================================

func setupTestHandlers(t *testing.T, models ...features.TestModel) (*Handlers, *features.TestFixture) {
	t.Helper()

	fixture := features.SetupTestFixture(t, models...)

	handlers := NewHandlers(
		fixture.Engine,
		fixture.Store,
		fixture.SessionStore,
		fixture.Notifier,
		true, // isDev
	)

	return handlers, fixture
}

// =============================================================================
// ModelPage Tests - Full HTML page responses
// =============================================================================

func TestModelPage(t *testing.T) {
	testModels := []features.TestModel{
		{
			Path: "staging.customers",
			Name: "customers",
			SQL:  "SELECT id, name FROM raw_customers",
		},
	}

	tests := []struct {
		name       string
		modelPath  string
		wantStatus int
		wantBody   []string // strings that should be present in response
	}{
		{
			name:       "returns HTML with model name in title",
			modelPath:  "staging.customers",
			wantStatus: http.StatusOK,
			wantBody: []string{
				"<!doctype html>",
				"<title>customers - LeapSQL</title>", // Uses model.Name, not path
				"data-init",
				"/models/staging.customers/sse",
			},
		},
		{
			name:       "non-existent model still returns page shell",
			modelPath:  "does.not.exist",
			wantStatus: http.StatusOK,
			wantBody: []string{
				"<!doctype html>",
				"data-init",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := setupTestHandlers(t, testModels...)

			req := httptest.NewRequest(http.MethodGet, "/models/"+tt.modelPath, nil)
			req = features.RequestWithPathParam(req, "path", tt.modelPath)
			rec := httptest.NewRecorder()

			h.ModelPage(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			body := rec.Body.String()
			for _, want := range tt.wantBody {
				assert.Contains(t, body, want, "response should contain %q", want)
			}
		})
	}
}

// =============================================================================
// ModelPageSSE Tests - SSE responses with HTML fragments
// =============================================================================

func TestModelPageSSE(t *testing.T) {
	testModels := []features.TestModel{
		{
			Path:        "staging.customers",
			Name:        "customers",
			SQL:         "SELECT id, name FROM raw_customers",
			Description: "Customer staging model",
		},
		{
			Path: "staging.orders",
			Name: "orders",
			SQL:  "SELECT * FROM raw_orders",
		},
	}

	tests := []struct {
		name      string
		modelPath string
		wantBody  []string // strings that should be present in SSE response
	}{
		{
			name:      "contains model name in response",
			modelPath: "staging.customers",
			wantBody:  []string{"customers"},
		},
		{
			name:      "contains source SQL",
			modelPath: "staging.customers",
			wantBody:  []string{"SELECT id, name FROM raw_customers"},
		},
		{
			name:      "contains explorer tree with model links",
			modelPath: "staging.customers",
			wantBody: []string{
				`href="/models/staging.customers"`,
				`href="/models/staging.orders"`,
			},
		},
		{
			name:      "contains navigation links",
			modelPath: "staging.customers",
			wantBody: []string{
				`href="/graph"`,
				`href="/runs"`,
			},
		},
		{
			name:      "contains tab buttons for model view",
			modelPath: "staging.customers",
			wantBody: []string{
				"Source SQL",
				"Compiled SQL",
				"Preview Data",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := setupTestHandlers(t, testModels...)

			req := httptest.NewRequest(http.MethodGet, "/models/"+tt.modelPath+"/sse", nil)
			req = features.RequestWithPathParam(req, "path", tt.modelPath)

			// Add timeout to prevent test from hanging - use req.Context() to preserve chi params
			ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
			defer cancel()
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()
			h.ModelPageSSE(rec, req)

			body := rec.Body.String()
			for _, want := range tt.wantBody {
				assert.Contains(t, body, want, "SSE response should contain %q", want)
			}
		})
	}
}

func TestModelPageSSE_NonExistentModel(t *testing.T) {
	testModels := []features.TestModel{
		{Path: "staging.customers", Name: "customers", SQL: "SELECT 1"},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/does.not.exist/sse", nil)
	req = features.RequestWithPathParam(req, "path", "does.not.exist")

	ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	h.ModelPageSSE(rec, req)

	// Should still return something (error handling via SSE)
	// The exact behavior depends on implementation - it might show an error or empty state
	body := rec.Body.String()
	assert.NotEmpty(t, body, "should return some response even for non-existent model")
}

// =============================================================================
// State Change Tests - Verify updates propagate
// =============================================================================

func TestModelPageSSE_UpdateOnBroadcast(t *testing.T) {
	testModels := []features.TestModel{
		{
			Path: "staging.customers",
			Name: "customers",
			SQL:  "SELECT 1 AS original",
		},
	}

	h, fixture := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers/sse", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")

	// Use longer timeout to allow for broadcast
	ctx, cancel := context.WithTimeout(req.Context(), 300*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()

	// Run handler in goroutine
	done := make(chan struct{})
	go func() {
		h.ModelPageSSE(rec, req)
		close(done)
	}()

	// Wait for initial render
	time.Sleep(50 * time.Millisecond)

	// Trigger broadcast (simulating a state change)
	fixture.Notifier.Broadcast()

	// Wait for handler to complete (context timeout)
	<-done

	body := rec.Body.String()

	// Should have received at least 2 SSE events (initial + broadcast)
	// Each event in datastar format contains "event:" line
	eventCount := strings.Count(body, "event:")
	assert.GreaterOrEqual(t, eventCount, 2, "should have at least 2 SSE events (initial + broadcast)")
}

func TestModelPageSSE_MultipleModelsInExplorer(t *testing.T) {
	// Test with multiple models to verify explorer tree shows all
	testModels := []features.TestModel{
		{Path: "staging.customers", Name: "customers", SQL: "SELECT 1"},
		{Path: "staging.orders", Name: "orders", SQL: "SELECT 2"},
		{Path: "marts.summary", Name: "summary", SQL: "SELECT 3"},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers/sse", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")

	ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	h.ModelPageSSE(rec, req)

	body := rec.Body.String()

	// All models should appear in explorer links
	assert.Contains(t, body, "/models/staging.customers")
	assert.Contains(t, body, "/models/staging.orders")
	assert.Contains(t, body, "/models/marts.summary")
}

// =============================================================================
// Context Panel Tests
// =============================================================================

func TestModelPageSSE_ContextPanel(t *testing.T) {
	testModels := []features.TestModel{
		{
			Path:         "staging.customers",
			Name:         "customers",
			SQL:          "SELECT id, name FROM raw_customers",
			Materialized: "view",
			Schema:       "analytics",
		},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers/sse", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")

	ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	h.ModelPageSSE(rec, req)

	body := rec.Body.String()

	// Context panel should show model metadata
	assert.Contains(t, body, "customers", "should show model name in context")
	assert.Contains(t, body, "View", "should show materialization type")
}

// =============================================================================
// Compiled SQL Tests
// =============================================================================

func TestModelPageSSE_CompiledSQL(t *testing.T) {
	// Test that compiled SQL is included in response
	testModels := []features.TestModel{
		{
			Path: "staging.customers",
			Name: "customers",
			SQL:  "SELECT id, name FROM raw_customers WHERE active = true",
		},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers/sse", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")

	ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	h.ModelPageSSE(rec, req)

	body := rec.Body.String()

	// The compiled SQL tab section should exist
	assert.Contains(t, body, "Compiled SQL", "should have compiled SQL tab")

	// The actual SQL should appear (either source or compiled version)
	assert.Contains(t, body, "SELECT id, name FROM raw_customers", "should contain SQL content")
}
