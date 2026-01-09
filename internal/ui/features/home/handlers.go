package home

import (
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
	"github.com/leapstack-labs/leapsql/internal/ui/features/home/pages"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the home feature.
type Handlers struct {
	engine       *engine.Engine
	store        core.Store
	sessionStore sessions.Store
	notifier     *notifier.Notifier
	isDev        bool
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(eng *engine.Engine, store core.Store, sessionStore sessions.Store, notify *notifier.Notifier, isDev bool) *Handlers {
	return &Handlers{
		engine:       eng,
		store:        store,
		sessionStore: sessionStore,
		notifier:     notify,
		isDev:        isDev,
	}
}

// HomePage renders the home page with full content.
func (h *Handlers) HomePage(w http.ResponseWriter, r *http.Request) {
	sidebar, stats, err := h.buildDashboardData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := pages.HomePage("Dashboard", h.isDev, sidebar, stats).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// HomePageUpdates is the long-lived SSE endpoint for the dashboard page.
// It subscribes to updates and pushes changes when the store changes.
// Unlike the old pattern, it does NOT send initial state - that's rendered by HomePage.
func (h *Handlers) HomePageUpdates(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	// Subscribe to updates
	updates := h.notifier.Subscribe()
	defer h.notifier.Unsubscribe(updates)

	// Wait for updates (no initial send - content is already rendered)
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-updates:
			if err := h.sendDashboardView(sse); err != nil {
				_ = sse.ConsoleError(err)
				// Don't return - keep trying on next update
			}
		}
	}
}

// sendDashboardView builds and sends the full app view for the dashboard.
func (h *Handlers) sendDashboardView(sse *datastar.ServerSentEventGenerator) error {
	sidebar, stats, err := h.buildDashboardData()
	if err != nil {
		return err
	}
	return sse.PatchElementTempl(pages.HomeAppShell(sidebar, stats))
}

// buildDashboardData assembles all data needed for the dashboard view.
func (h *Handlers) buildDashboardData() (common.SidebarData, *pages.DashboardStats, error) {
	sidebar := common.SidebarData{
		CurrentPath: "/",
		FullWidth:   false,
	}

	// Build explorer tree
	models, err := h.store.ListModels()
	if err != nil {
		return sidebar, nil, err
	}
	sidebar.ExplorerTree = common.BuildExplorerTree(models)

	// Get stats for dashboard
	stats := &pages.DashboardStats{
		ModelCount: len(models),
	}

	return sidebar, stats, nil
}
