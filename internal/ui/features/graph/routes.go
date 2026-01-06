package graph

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers the graph feature routes.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
) error {
	handlers := NewHandlers(eng, store, sessionStore)

	// Page route
	router.Get("/graph", handlers.GraphPage)

	// API routes
	router.Route("/api/graph", func(r chi.Router) {
		r.Get("/", handlers.FullGraphSSE)              // Full DAG
		r.Get("/model/{path}", handlers.ModelGraphSSE) // Model neighborhood (parents + children)
	})

	return nil
}
