package home

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
// HomePage Tests - Full HTML page responses with server-rendered content
// =============================================================================

func TestHomePage(t *testing.T) {
	tests := []struct {
		name       string
		wantStatus int
		wantBody   []string // strings that should be present in response
	}{
		{
			name:       "returns HTML with dashboard title and full content",
			wantStatus: http.StatusOK,
			wantBody: []string{
				"<!doctype html>",
				"<title>Dashboard - LeapSQL</title>",
				"data-init",
				"/updates",
				"ui-content", // Content is now rendered immediately
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := setupTestHandlers(t)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()

			h.HomePage(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			body := rec.Body.String()
			for _, want := range tt.wantBody {
				assert.Contains(t, body, want, "response should contain %q", want)
			}
		})
	}
}

func TestHomePage_WithModels(t *testing.T) {
	// Test that models appear in the server-rendered content
	testModels := []features.TestModel{
		{
			Path: "staging.customers",
			Name: "customers",
			SQL:  "SELECT id, name FROM raw_customers",
		},
		{
			Path: "staging.orders",
			Name: "orders",
			SQL:  "SELECT * FROM raw_orders",
		},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	h.HomePage(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	// Full content should be server-rendered (no flicker)
	assert.Contains(t, body, `href="/models/staging.customers"`, "explorer should contain model links")
	assert.Contains(t, body, `href="/models/staging.orders"`, "explorer should contain model links")
	assert.Contains(t, body, `href="/graph"`, "should contain navigation links")
	assert.Contains(t, body, `href="/runs"`, "should contain navigation links")
	assert.Contains(t, body, "ui-content", "should contain dashboard content area")
}

// =============================================================================
// HomePageUpdates Tests - SSE endpoint for live updates only
// =============================================================================

func TestHomePageUpdates_SendsUpdateOnBroadcast(t *testing.T) {
	testModels := []features.TestModel{
		{
			Path: "staging.customers",
			Name: "customers",
			SQL:  "SELECT 1",
		},
	}

	h, fixture := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/updates", nil)

	// Use longer timeout to allow for broadcast
	ctx, cancel := context.WithTimeout(req.Context(), 300*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()

	// Run handler in goroutine
	done := make(chan struct{})
	go func() {
		h.HomePageUpdates(rec, req)
		close(done)
	}()

	// Wait a bit then trigger broadcast (simulating a state change)
	time.Sleep(50 * time.Millisecond)
	fixture.Notifier.Broadcast()

	// Wait for handler to complete (context timeout)
	<-done

	body := rec.Body.String()

	// Should have received at least 1 SSE event from the broadcast
	// (No initial event - content is server-rendered by HomePage)
	eventCount := strings.Count(body, "event:")
	assert.GreaterOrEqual(t, eventCount, 1, "should have at least 1 SSE event from broadcast")

	// The update should contain the app content
	assert.Contains(t, body, "/models/staging.customers", "update should contain model links")
}

func TestHomePageUpdates_NoInitialState(t *testing.T) {
	// Verify that HomePageUpdates does NOT send initial state
	// (that's now handled by HomePage's server render)
	testModels := []features.TestModel{
		{Path: "staging.customers", Name: "customers", SQL: "SELECT 1"},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/updates", nil)

	// Short timeout - no broadcast, so should timeout with no events
	ctx, cancel := context.WithTimeout(req.Context(), 50*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	h.HomePageUpdates(rec, req)

	body := rec.Body.String()

	// Should NOT have any SSE events (no initial state, no broadcast)
	eventCount := strings.Count(body, "event:")
	assert.Equal(t, 0, eventCount, "should have no SSE events without broadcast")
}

func TestHomePageUpdates_MultipleModelsOnBroadcast(t *testing.T) {
	// Test with multiple models in different folders
	testModels := []features.TestModel{
		{Path: "staging.customers", Name: "customers", SQL: "SELECT 1"},
		{Path: "staging.orders", Name: "orders", SQL: "SELECT 2"},
		{Path: "marts.summary", Name: "summary", SQL: "SELECT 3"},
	}

	h, fixture := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/updates", nil)

	ctx, cancel := context.WithTimeout(req.Context(), 300*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.HomePageUpdates(rec, req)
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

	// Folders should be present
	assert.Contains(t, body, "staging")
	assert.Contains(t, body, "marts")
}
