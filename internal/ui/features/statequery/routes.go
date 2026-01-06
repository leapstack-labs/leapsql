// Package statequery provides handlers for querying the state database.
package statequery

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers the state query feature routes.
func SetupRoutes(
	router chi.Router,
	store core.Store,
	sessionStore sessions.Store,
) error {
	handlers := NewHandlers(store, sessionStore)

	// Page routes
	router.Get("/query", handlers.QueryPage)
	router.Get("/query/sse", handlers.QueryPageSSE)

	// API routes for query execution
	router.Route("/api/query", func(r chi.Router) {
		r.Post("/execute", handlers.ExecuteQuerySSE)
		r.Get("/tables", handlers.TablesSSE)
		r.Get("/schema/{name}", handlers.SchemaSSE)
		r.Get("/search", handlers.SearchSSE)
	})

	return nil
}
