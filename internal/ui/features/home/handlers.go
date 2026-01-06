package home

import (
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/common/components"
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

// HomePage renders the home page.
func (h *Handlers) HomePage(w http.ResponseWriter, r *http.Request) {
	if err := pages.HomePage("Dashboard", h.isDev).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// HomePageSSE is the long-lived SSE endpoint for the dashboard page.
// It sends the initial view and then pushes updates when the store changes.
func (h *Handlers) HomePageSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	// Subscribe to updates
	updates := h.notifier.Subscribe()
	defer h.notifier.Unsubscribe(updates)

	// Send initial state
	if err := h.sendDashboardView(sse); err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	// Wait for updates
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
	appData, err := h.buildDashboardAppData()
	if err != nil {
		return err
	}
	return sse.PatchElementTempl(components.AppContainer(appData))
}

// buildDashboardAppData assembles all data needed for the dashboard view.
func (h *Handlers) buildDashboardAppData() (components.AppData, error) {
	data := components.AppData{
		CurrentPath: "/",
	}

	// Build explorer tree
	models, err := h.store.ListModels()
	if err != nil {
		return data, err
	}
	data.ExplorerTree = buildExplorerTree(models)

	// Get stats for dashboard
	data.Stats = &components.DashboardStats{
		ModelCount: len(models),
	}

	return data, nil
}

// buildExplorerTree groups models into a tree structure by folder.
func buildExplorerTree(models []*core.PersistedModel) []components.TreeNode {
	folders := make(map[string]*components.TreeNode)

	for _, m := range models {
		folder := extractFolder(m.Path)

		if _, ok := folders[folder]; !ok {
			folders[folder] = &components.TreeNode{
				Name:     folder,
				Path:     folder,
				Type:     "folder",
				Children: []components.TreeNode{},
			}
		}

		folders[folder].Children = append(folders[folder].Children, components.TreeNode{
			Name: m.Name,
			Path: m.Path,
			Type: "model",
		})
	}

	// Convert map to sorted slice
	result := make([]components.TreeNode, 0, len(folders))
	for _, node := range folders {
		// Sort children by name
		sort.Slice(node.Children, func(i, j int) bool {
			return node.Children[i].Name < node.Children[j].Name
		})
		result = append(result, *node)
	}

	// Sort folders by name
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// extractFolder extracts the folder name from a model path.
func extractFolder(modelPath string) string {
	parts := strings.Split(modelPath, ".")
	if len(parts) <= 1 {
		return "models"
	}
	return filepath.Join(parts[:len(parts)-1]...)
}
