package graph

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers the graph feature routes.
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
	router.Get("/graph", handlers.HandleGraphPage)

	// SSE route (live updates only)
	router.Get("/graph/updates", handlers.GraphPageUpdates)

	// API routes (kept for backward compatibility)
	router.Route("/api/graph", func(r chi.Router) {
		r.Get("/", handlers.FullGraphSSE)              // Full DAG
		r.Get("/model/{path}", handlers.ModelGraphSSE) // Model neighborhood (parents + children)
	})

	return nil
}
