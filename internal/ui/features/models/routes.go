// Package models provides the model detail feature for the UI.
package models

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers model routes on the router.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
	notify *notifier.Notifier,
	isDev bool,
) error {
	handlers := NewHandlers(eng, store, sessionStore, notify, isDev)

	// Page routes (full page render)
	router.Get("/models/{path}", handlers.ModelPage)

	// SSE routes (long-lived streams)
	router.Get("/models/{path}/sse", handlers.ModelPageSSE)

	return nil
}
