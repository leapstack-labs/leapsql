// Package statequery provides handlers for querying the state database.
package statequery

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers the state query feature routes.
func SetupRoutes(
	router chi.Router,
	store core.Store,
	sessionStore sessions.Store,
	notify *notifier.Notifier,
	isDev bool,
) error {
	handlers := NewHandlers(store, sessionStore, notify, isDev)

	// Page routes (full page render with content)
	router.Get("/query", handlers.QueryPage)

	// SSE routes (live updates only)
	router.Get("/query/updates", handlers.QueryPageUpdates)

	// API routes for query execution
	router.Route("/api/query", func(r chi.Router) {
		r.Post("/execute", handlers.ExecuteQuerySSE)
		r.Get("/tables", handlers.TablesSSE)
		r.Get("/schema/{name}", handlers.SchemaSSE)
		r.Get("/search", handlers.SearchSSE)
	})

	return nil
}
