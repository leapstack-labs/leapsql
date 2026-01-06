// Package graph provides DAG visualization handlers for the UI.
package graph

import (
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/graph/pages"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the graph feature.
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

// GraphPage renders the graph visualization page.
func (h *Handlers) GraphPage(w http.ResponseWriter, r *http.Request) {
	isDev := true // TODO: Get from context
	if err := pages.GraphPage("DAG", isDev).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// FullGraphSSE sends the full DAG via SSE.
func (h *Handlers) FullGraphSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	// Get all models
	models, err := h.store.ListModels()
	if err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	// Build graph data
	graphData := h.buildFullGraph(models)

	if err := sse.PatchElementTempl(pages.GraphView(graphData)); err != nil {
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

	if err := sse.PatchElementTempl(pages.GraphView(graphData)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// buildFullGraph creates graph data from all models and their dependencies.
func (h *Handlers) buildFullGraph(models []*core.PersistedModel) pages.GraphData {
	// Create a map for quick lookup
	modelMap := make(map[string]*core.PersistedModel)
	for _, m := range models {
		modelMap[m.ID] = m
	}

	// Build nodes
	nodes := make([]pages.GraphNode, 0, len(models))
	for _, m := range models {
		nodes = append(nodes, pages.GraphNode{
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
	edges := make([]pages.GraphEdge, 0)
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
			edges = append(edges, pages.GraphEdge{
				Source: depModel.Path,
				Target: m.Path,
			})
		}
	}

	return pages.GraphData{
		Nodes: nodes,
		Edges: edges,
	}
}

// buildModelNeighborhood creates graph data for a model and its immediate neighbors.
func (h *Handlers) buildModelNeighborhood(model *core.PersistedModel) pages.GraphData {
	nodeSet := make(map[string]pages.GraphNode)
	edges := make([]pages.GraphEdge, 0)

	// Add the center model
	nodeSet[model.Path] = pages.GraphNode{
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
		nodeSet[depModel.Path] = pages.GraphNode{
			ID:    depModel.Path,
			Label: depModel.Name,
			Type:  nodeType(depModel),
		}
		edges = append(edges, pages.GraphEdge{
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
		nodeSet[depModel.Path] = pages.GraphNode{
			ID:    depModel.Path,
			Label: depModel.Name,
			Type:  nodeType(depModel),
		}
		edges = append(edges, pages.GraphEdge{
			Source: model.Path,
			Target: depModel.Path,
		})
	}

	// Convert node set to slice
	nodes := make([]pages.GraphNode, 0, len(nodeSet))
	for _, node := range nodeSet {
		nodes = append(nodes, node)
	}

	// Sort nodes for consistent display
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	return pages.GraphData{
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
