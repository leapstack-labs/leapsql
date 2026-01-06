// Package runs provides run history handlers for the UI.
package runs

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers the runs history feature routes.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
	notify *notifier.Notifier,
	isDev bool,
) error {
	handlers := NewHandlers(eng, store, sessionStore, notify, isDev)

	// Page route (full page render with content)
	router.Get("/runs", handlers.RunsPage)

	// SSE route (live updates only)
	router.Get("/runs/updates", handlers.RunsPageUpdates)

	// API routes for run history (kept for backward compatibility)
	router.Route("/api/runs", func(r chi.Router) {
		r.Get("/", handlers.RunsListSSE)             // List recent runs
		r.Get("/{id}", handlers.RunDetailSSE)        // Run details
		r.Get("/{id}/models", handlers.RunModelsSSE) // Model runs for a run
	})

	return nil
}
