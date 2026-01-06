package home

import (
	"fmt"
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/home/pages"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the home feature.
type Handlers struct {
	engine       *engine.Engine
	store        core.Store
	sessionStore sessions.Store
	isDev        bool
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(eng *engine.Engine, store core.Store, sessionStore sessions.Store, isDev bool) *Handlers {
	return &Handlers{
		engine:       eng,
		store:        store,
		sessionStore: sessionStore,
		isDev:        isDev,
	}
}

// HomePage renders the home page.
func (h *Handlers) HomePage(w http.ResponseWriter, r *http.Request) {
	if err := pages.HomePage("Dashboard", h.isDev).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// StatsSSE sends stats via SSE.
func (h *Handlers) StatsSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	// Get stats from store
	models, err := h.store.ListModels()
	if err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	stats := pages.Stats{
		ModelCount: len(models),
	}

	if err := sse.PatchElementTempl(pages.StatsView(stats)); err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to render stats: %w", err))
	}
}
