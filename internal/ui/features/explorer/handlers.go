// Package explorer provides the model explorer feature for the UI.
package explorer

import (
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/explorer/components"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the explorer feature.
type Handlers struct {
	engine       *engine.Engine
	store        core.Store
	sessionStore sessions.Store
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(eng *engine.Engine, store core.Store, sessionStore sessions.Store) *Handlers {
	return &Handlers{
		engine:       eng,
		store:        store,
		sessionStore: sessionStore,
	}
}

// TreeSSE sends the explorer tree via SSE.
func (h *Handlers) TreeSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	// Get models from store
	models, err := h.store.ListModels()
	if err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	// Build tree structure grouped by folder
	tree := buildTree(models)

	if err := sse.PatchElementTempl(components.ExplorerTree(tree)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// buildTree groups models into a tree structure by folder.
func buildTree(models []*core.PersistedModel) []components.TreeNode {
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
// e.g., "staging.customers" -> "staging"
// e.g., "marts.finance.revenue" -> "marts/finance"
func extractFolder(modelPath string) string {
	parts := strings.Split(modelPath, ".")
	if len(parts) <= 1 {
		return "models"
	}
	// Join all but the last part (which is the model name)
	return filepath.Join(parts[:len(parts)-1]...)
}
