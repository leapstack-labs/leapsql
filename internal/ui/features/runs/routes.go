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

	// Page routes - each run is a resource
	router.Get("/runs", handlers.RunsPage)
	router.Get("/runs/{id}", handlers.RunsPage)

	// SSE routes - live updates for both list and detail views
	router.Get("/runs/updates", handlers.RunsPageUpdates)
	router.Get("/runs/{id}/updates", handlers.RunsPageUpdates)

	return nil
}
