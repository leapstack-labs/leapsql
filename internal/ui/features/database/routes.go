// Package database provides database browser handlers for the UI.
package database

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers the database browser feature routes.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
) error {
	handlers := NewHandlers(eng, store, sessionStore)

	// API routes for database browser
	router.Route("/api/database", func(r chi.Router) {
		r.Get("/status", handlers.StatusSSE)                     // Connection status
		r.Get("/schemas", handlers.SchemasSSE)                   // List schemas
		r.Get("/schemas/{name}", handlers.TablesSSE)             // Tables in schema
		r.Get("/tables/{schema}/{table}", handlers.TableMetaSSE) // Table columns
	})

	return nil
}
