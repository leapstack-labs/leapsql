// Package home provides the home/landing page feature for the UI.
package home

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes configures routes for the home feature.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
	isDev bool,
) error {
	handlers := NewHandlers(eng, store, sessionStore, isDev)

	router.Get("/", handlers.HomePage)
	router.Get("/api/stats", handlers.StatsSSE)

	return nil
}
