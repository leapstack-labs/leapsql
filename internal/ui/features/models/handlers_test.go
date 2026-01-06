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
// ModelPage Tests - Full HTML page responses with server-rendered content
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
			name:       "returns HTML with model name in title and full content",
			modelPath:  "staging.customers",
			wantStatus: http.StatusOK,
			wantBody: []string{
				"<!doctype html>",
				"<title>customers - LeapSQL</title>", // Uses model.Name, not path
				"data-init",
				"/models/staging.customers/updates",
				"customers",                          // Model name in content
				"SELECT id, name FROM raw_customers", // Source SQL rendered
			},
		},
		{
			name:       "non-existent model still returns page with empty state",
			modelPath:  "does.not.exist",
			wantStatus: http.StatusOK,
			wantBody: []string{
				"<!doctype html>",
				"data-init",
				"/models/does.not.exist/updates",
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

func TestModelPage_FullContent(t *testing.T) {
	// Test that full content is server-rendered (no flicker)
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

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")
	rec := httptest.NewRecorder()

	h.ModelPage(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	// Full content should be server-rendered
	assert.Contains(t, body, "customers", "should contain model name")
	assert.Contains(t, body, "SELECT id, name FROM raw_customers", "should contain source SQL")
	assert.Contains(t, body, `href="/models/staging.customers"`, "explorer should contain model links")
	assert.Contains(t, body, `href="/models/staging.orders"`, "explorer should contain model links")
	assert.Contains(t, body, `href="/graph"`, "should contain navigation links")
	assert.Contains(t, body, `href="/runs"`, "should contain navigation links")
	assert.Contains(t, body, "Source SQL", "should contain tab buttons")
	assert.Contains(t, body, "Compiled SQL", "should contain tab buttons")
	assert.Contains(t, body, "Preview Data", "should contain tab buttons")
}

// =============================================================================
// ModelPageUpdates Tests - SSE endpoint for live updates only
// =============================================================================

func TestModelPageUpdates_SendsUpdateOnBroadcast(t *testing.T) {
	testModels := []features.TestModel{
		{
			Path: "staging.customers",
			Name: "customers",
			SQL:  "SELECT 1 AS original",
		},
	}

	h, fixture := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers/updates", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")

	// Use longer timeout to allow for broadcast
	ctx, cancel := context.WithTimeout(req.Context(), 300*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()

	// Run handler in goroutine
	done := make(chan struct{})
	go func() {
		h.ModelPageUpdates(rec, req)
		close(done)
	}()

	// Wait a bit then trigger broadcast
	time.Sleep(50 * time.Millisecond)
	fixture.Notifier.Broadcast()

	// Wait for handler to complete (context timeout)
	<-done

	body := rec.Body.String()

	// Should have received at least 1 SSE event from the broadcast
	// (No initial event - content is server-rendered by ModelPage)
	eventCount := strings.Count(body, "event:")
	assert.GreaterOrEqual(t, eventCount, 1, "should have at least 1 SSE event from broadcast")

	// The update should contain the model content
	assert.Contains(t, body, "customers", "update should contain model name")
}

func TestModelPageUpdates_NoInitialState(t *testing.T) {
	// Verify that ModelPageUpdates does NOT send initial state
	testModels := []features.TestModel{
		{Path: "staging.customers", Name: "customers", SQL: "SELECT 1"},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers/updates", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")

	// Short timeout - no broadcast, so should timeout with no events
	ctx, cancel := context.WithTimeout(req.Context(), 50*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	h.ModelPageUpdates(rec, req)

	body := rec.Body.String()

	// Should NOT have any SSE events (no initial state, no broadcast)
	eventCount := strings.Count(body, "event:")
	assert.Equal(t, 0, eventCount, "should have no SSE events without broadcast")
}

func TestModelPageUpdates_MultipleModelsOnBroadcast(t *testing.T) {
	// Test with multiple models to verify explorer tree shows all on update
	testModels := []features.TestModel{
		{Path: "staging.customers", Name: "customers", SQL: "SELECT 1"},
		{Path: "staging.orders", Name: "orders", SQL: "SELECT 2"},
		{Path: "marts.summary", Name: "summary", SQL: "SELECT 3"},
	}

	h, fixture := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers/updates", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")

	ctx, cancel := context.WithTimeout(req.Context(), 300*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.ModelPageUpdates(rec, req)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	fixture.Notifier.Broadcast()

	<-done

	body := rec.Body.String()

	// All models should appear in the update
	assert.Contains(t, body, "/models/staging.customers")
	assert.Contains(t, body, "/models/staging.orders")
	assert.Contains(t, body, "/models/marts.summary")
}

// =============================================================================
// Context Panel Tests (via ModelPage - server-rendered)
// =============================================================================

func TestModelPage_ContextPanel(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")
	rec := httptest.NewRecorder()

	h.ModelPage(rec, req)

	body := rec.Body.String()

	// Context panel should show model metadata
	assert.Contains(t, body, "customers", "should show model name in context")
	assert.Contains(t, body, "View", "should show materialization type")
}

// =============================================================================
// Compiled SQL Tests (via ModelPage - server-rendered)
// =============================================================================

func TestModelPage_CompiledSQL(t *testing.T) {
	// Test that compiled SQL is included in server-rendered response
	testModels := []features.TestModel{
		{
			Path: "staging.customers",
			Name: "customers",
			SQL:  "SELECT id, name FROM raw_customers WHERE active = true",
		},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/models/staging.customers", nil)
	req = features.RequestWithPathParam(req, "path", "staging.customers")
	rec := httptest.NewRecorder()

	h.ModelPage(rec, req)

	body := rec.Body.String()

	// The compiled SQL tab section should exist
	assert.Contains(t, body, "Compiled SQL", "should have compiled SQL tab")

	// The actual SQL should appear (either source or compiled version)
	assert.Contains(t, body, "SELECT id, name FROM raw_customers", "should contain SQL content")
}
