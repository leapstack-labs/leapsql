// Package models provides the model detail feature for the UI.
package models

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers model routes on the router.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
) error {
	handlers := NewHandlers(eng, store, sessionStore)

	router.Route("/api/models", func(r chi.Router) {
		r.Get("/{path}", handlers.ModelSSE)             // Model details (patches content + context)
		r.Get("/{path}/sql", handlers.SQLViewSSE)       // Raw SQL only
		r.Get("/{path}/compiled", handlers.CompiledSSE) // Compiled SQL only
		r.Get("/{path}/preview", handlers.PreviewSSE)   // Data preview from database
	})

	return nil
}
