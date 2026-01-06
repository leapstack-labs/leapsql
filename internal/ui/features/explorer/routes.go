package explorer

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// SetupRoutes registers explorer routes on the router.
func SetupRoutes(
	router chi.Router,
	eng *engine.Engine,
	store core.Store,
	sessionStore sessions.Store,
	notify *notifier.Notifier,
) error {
	handlers := NewHandlers(eng, store, sessionStore, notify)

	router.Route("/api/explorer", func(r chi.Router) {
		r.Get("/", handlers.TreeSSE) // Full tree
	})

	return nil
}
