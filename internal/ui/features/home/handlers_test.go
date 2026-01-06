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
// HomePage Tests - Full HTML page responses
// =============================================================================

func TestHomePage(t *testing.T) {
	tests := []struct {
		name       string
		wantStatus int
		wantBody   []string // strings that should be present in response
	}{
		{
			name:       "returns HTML with dashboard title",
			wantStatus: http.StatusOK,
			wantBody: []string{
				"<!doctype html>",
				"<title>Dashboard - LeapSQL</title>",
				"data-init",
				"/sse",
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

// =============================================================================
// HomePageSSE Tests - SSE responses with HTML fragments
// =============================================================================

func TestHomePageSSE(t *testing.T) {
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

	tests := []struct {
		name     string
		wantBody []string // strings that should be present in SSE response
	}{
		{
			name: "contains explorer tree with model links",
			wantBody: []string{
				`href="/models/staging.customers"`,
				`href="/models/staging.orders"`,
			},
		},
		{
			name: "contains navigation links",
			wantBody: []string{
				`href="/graph"`,
				`href="/runs"`,
			},
		},
		{
			name: "contains dashboard content area",
			wantBody: []string{
				"ui-content",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := setupTestHandlers(t, testModels...)

			req := httptest.NewRequest(http.MethodGet, "/sse", nil)

			// Add timeout to prevent test from hanging
			ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
			defer cancel()
			req = req.WithContext(ctx)

			rec := httptest.NewRecorder()
			h.HomePageSSE(rec, req)

			body := rec.Body.String()
			for _, want := range tt.wantBody {
				assert.Contains(t, body, want, "SSE response should contain %q", want)
			}
		})
	}
}

func TestHomePageSSE_EmptyProject(t *testing.T) {
	// Test with no models
	h, _ := setupTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/sse", nil)

	ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	h.HomePageSSE(rec, req)

	// Should still return a valid response
	body := rec.Body.String()
	assert.NotEmpty(t, body, "should return some response even for empty project")
	assert.Contains(t, body, "event:", "should be a valid SSE response")
}

// =============================================================================
// State Change Tests - Verify updates propagate
// =============================================================================

func TestHomePageSSE_UpdateOnBroadcast(t *testing.T) {
	testModels := []features.TestModel{
		{
			Path: "staging.customers",
			Name: "customers",
			SQL:  "SELECT 1",
		},
	}

	h, fixture := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/sse", nil)

	// Use longer timeout to allow for broadcast
	ctx, cancel := context.WithTimeout(req.Context(), 300*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()

	// Run handler in goroutine
	done := make(chan struct{})
	go func() {
		h.HomePageSSE(rec, req)
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

func TestHomePageSSE_MultipleModelsInExplorer(t *testing.T) {
	// Test with multiple models in different folders
	testModels := []features.TestModel{
		{Path: "staging.customers", Name: "customers", SQL: "SELECT 1"},
		{Path: "staging.orders", Name: "orders", SQL: "SELECT 2"},
		{Path: "marts.summary", Name: "summary", SQL: "SELECT 3"},
	}

	h, _ := setupTestHandlers(t, testModels...)

	req := httptest.NewRequest(http.MethodGet, "/sse", nil)

	ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	h.HomePageSSE(rec, req)

	body := rec.Body.String()

	// All models should appear in explorer links
	assert.Contains(t, body, "/models/staging.customers")
	assert.Contains(t, body, "/models/staging.orders")
	assert.Contains(t, body, "/models/marts.summary")

	// Folders should be present
	assert.Contains(t, body, "staging")
	assert.Contains(t, body, "marts")
}
