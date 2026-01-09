// Package graph provides DAG visualization handlers for the UI.
package graph

import (
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the graph feature.
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

// HandleGraphPage renders the graph visualization page with full content.
func (h *Handlers) HandleGraphPage(w http.ResponseWriter, r *http.Request) {
	sidebar, graphData, err := h.buildGraphData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := GraphPage("DAG", h.isDev, sidebar, graphData).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// GraphPageUpdates is the long-lived SSE endpoint for the graph page.
// It subscribes to updates and pushes changes when the store changes.
// Unlike the old pattern, it does NOT send initial state - that's rendered by GraphPage.
func (h *Handlers) GraphPageUpdates(w http.ResponseWriter, r *http.Request) {
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
			if err := h.sendGraphView(sse); err != nil {
				_ = sse.ConsoleError(err)
			}
		}
	}
}

// sendGraphView builds and sends the full app view for the graph page.
func (h *Handlers) sendGraphView(sse *datastar.ServerSentEventGenerator) error {
	sidebar, graphData, err := h.buildGraphData()
	if err != nil {
		return err
	}
	return sse.PatchElementTempl(GraphAppShell(sidebar, graphData))
}

// buildGraphData assembles all data needed for the graph view.
func (h *Handlers) buildGraphData() (common.SidebarData, *GraphViewData, error) {
	sidebar := common.SidebarData{
		CurrentPath: "/graph",
		FullWidth:   true,
	}

	// Get all models
	models, err := h.store.ListModels()
	if err != nil {
		return sidebar, nil, err
	}

	// Build explorer tree
	sidebar.ExplorerTree = common.BuildExplorerTree(models)

	// Build graph data
	graphData := h.buildFullGraphData(models)

	return sidebar, &graphData, nil
}

// buildFullGraphData creates graph view data from all models and their dependencies.
func (h *Handlers) buildFullGraphData(models []*core.PersistedModel) GraphViewData {
	// Create a map for quick lookup
	modelMap := make(map[string]*core.PersistedModel)
	for _, m := range models {
		modelMap[m.ID] = m
	}

	// Build nodes
	nodes := make([]GraphNode, 0, len(models))
	for _, m := range models {
		nodes = append(nodes, GraphNode{
			ID:    m.Path,
			Label: m.Name,
			Type:  nodeType(m),
		})
	}

	// Sort nodes for consistent display
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	// Build edges from dependencies
	edges := make([]GraphEdge, 0)
	for _, m := range models {
		deps, err := h.store.GetDependencies(m.ID)
		if err != nil {
			continue
		}
		for _, depID := range deps {
			depModel, ok := modelMap[depID]
			if !ok {
				continue
			}
			edges = append(edges, GraphEdge{
				Source: depModel.Path,
				Target: m.Path,
			})
		}
	}

	return GraphViewData{
		Nodes: nodes,
		Edges: edges,
	}
}

// FullGraphSSE sends the full DAG via SSE (legacy endpoint for backward compatibility).
func (h *Handlers) FullGraphSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	// Get all models
	models, err := h.store.ListModels()
	if err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	// Build graph data
	graphData := h.buildFullGraphData(models)

	if err := sse.PatchElementTempl(GraphView(graphData)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// ModelGraphSSE sends a model's neighborhood graph (parents + children) via SSE.
func (h *Handlers) ModelGraphSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	modelPath := chi.URLParam(r, "path")

	// Get model
	model, err := h.store.GetModelByPath(modelPath)
	if err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	// Build neighborhood graph
	graphData := h.buildModelNeighborhood(model)

	if err := sse.PatchElementTempl(GraphView(graphData)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// buildModelNeighborhood creates graph data for a model and its immediate neighbors.
func (h *Handlers) buildModelNeighborhood(model *core.PersistedModel) GraphViewData {
	nodeSet := make(map[string]GraphNode)
	edges := make([]GraphEdge, 0)

	// Add the center model
	nodeSet[model.Path] = GraphNode{
		ID:    model.Path,
		Label: model.Name,
		Type:  nodeType(model),
	}

	// Add parents (dependencies)
	deps, _ := h.store.GetDependencies(model.ID)
	for _, depID := range deps {
		depModel, err := h.store.GetModelByID(depID)
		if err != nil {
			continue
		}
		nodeSet[depModel.Path] = GraphNode{
			ID:    depModel.Path,
			Label: depModel.Name,
			Type:  nodeType(depModel),
		}
		edges = append(edges, GraphEdge{
			Source: depModel.Path,
			Target: model.Path,
		})
	}

	// Add children (dependents)
	dependents, _ := h.store.GetDependents(model.ID)
	for _, depID := range dependents {
		depModel, err := h.store.GetModelByID(depID)
		if err != nil {
			continue
		}
		nodeSet[depModel.Path] = GraphNode{
			ID:    depModel.Path,
			Label: depModel.Name,
			Type:  nodeType(depModel),
		}
		edges = append(edges, GraphEdge{
			Source: model.Path,
			Target: depModel.Path,
		})
	}

	// Convert node set to slice
	nodes := make([]GraphNode, 0, len(nodeSet))
	for _, node := range nodeSet {
		nodes = append(nodes, node)
	}

	// Sort nodes for consistent display
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	return GraphViewData{
		Nodes: nodes,
		Edges: edges,
	}
}

// nodeType determines the node type based on model properties.
func nodeType(m *core.PersistedModel) string {
	// Could be extended to detect sources, seeds, etc.
	if m.Materialized == "view" || m.Materialized == "" {
		return "view"
	}
	return m.Materialized
}
