// Package macros provides the macros catalog feature for the UI.
package macros

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers the macros catalog feature routes.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
	notify *notifier.Notifier,
	isDev bool,
) error {
	handlers := NewHandlers(eng, store, sessionStore, notify, isDev)

	// Page routes - each function is a resource
	router.Get("/macros", handlers.HandleMacrosPage)
	router.Get("/macros/{namespace}/{function}", handlers.HandleMacrosPage)

	// SSE routes - live updates for both list and detail views
	router.Get("/macros/updates", handlers.MacrosPageUpdates)
	router.Get("/macros/{namespace}/{function}/updates", handlers.MacrosPageUpdates)

	return nil
}
