package models

import (
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	commonComponents "github.com/leapstack-labs/leapsql/internal/ui/features/common/components"
	"github.com/leapstack-labs/leapsql/internal/ui/features/models/pages"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the models feature.
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

// ModelPage renders the model detail page shell.
func (h *Handlers) ModelPage(w http.ResponseWriter, r *http.Request) {
	modelPath := chi.URLParam(r, "path")

	// Get model name for title
	model, err := h.store.GetModelByPath(modelPath)
	title := modelPath
	if err == nil && model != nil {
		title = model.Name
	}

	if err := pages.ModelPage(title, modelPath, h.isDev).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ModelPageSSE is the long-lived SSE endpoint for a model page.
// It sends the initial view and then pushes updates when the store changes.
func (h *Handlers) ModelPageSSE(w http.ResponseWriter, r *http.Request) {
	modelPath := chi.URLParam(r, "path")
	sse := datastar.NewSSE(w, r)

	// Subscribe to updates
	updates := h.notifier.Subscribe()
	defer h.notifier.Unsubscribe(updates)

	// Send initial state
	if err := h.sendAppView(sse, modelPath); err != nil {
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
			if err := h.sendAppView(sse, modelPath); err != nil {
				_ = sse.ConsoleError(err)
				// Don't return - keep trying on next update
			}
		}
	}
}

// sendAppView builds and sends the full app view for a model page.
func (h *Handlers) sendAppView(sse *datastar.ServerSentEventGenerator, modelPath string) error {
	appData, err := h.buildAppData(modelPath)
	if err != nil {
		return err
	}
	return sse.PatchElementTempl(commonComponents.AppContainer(appData))
}

// buildAppData assembles all data needed for the app view.
func (h *Handlers) buildAppData(modelPath string) (commonComponents.AppData, error) {
	data := commonComponents.AppData{
		CurrentPath: "/models/" + modelPath,
	}

	// Build explorer tree
	models, err := h.store.ListModels()
	if err != nil {
		return data, err
	}
	data.ExplorerTree = buildExplorerTree(models)

	// Get model details if path specified
	if modelPath != "" {
		model, err := h.store.GetModelByPath(modelPath)
		if err != nil {
			return data, err
		}

		// Only build model view if model exists
		if model != nil {
			// Build model view data with all tab content
			modelView := h.buildModelViewData(model)
			data.Model = &modelView

			// Build context panel
			context := h.buildModelContext(model)
			data.Context = &context
		}
	}

	return data, nil
}

// buildModelViewData builds the model view with all tabs pre-rendered.
func (h *Handlers) buildModelViewData(model *core.PersistedModel) commonComponents.ModelViewData {
	data := commonComponents.ModelViewData{
		Path:         model.Path,
		Name:         model.Name,
		FilePath:     model.FilePath,
		Materialized: model.Materialized,
		Schema:       model.Schema,
		Description:  model.Description,
		Owner:        model.Owner,
		Tags:         model.Tags,
		SourceSQL:    model.SQL,
	}

	// Compile SQL
	compiled, err := h.engine.RenderModel(model.Path)
	if err != nil {
		data.CompileError = err.Error()
	} else {
		data.CompiledSQL = compiled
	}

	return data
}

// buildModelContext builds the context panel data.
func (h *Handlers) buildModelContext(model *core.PersistedModel) commonComponents.ModelContext {
	deps, _ := h.store.GetDependencies(model.ID)
	dependents, _ := h.store.GetDependents(model.ID)
	columns, _ := h.store.GetModelColumns(model.Path)

	return commonComponents.ModelContext{
		Path:       model.Path,
		Name:       model.Name,
		Type:       model.Materialized,
		Schema:     model.Schema,
		DependsOn:  h.resolveModelNames(deps),
		Dependents: h.resolveModelNames(dependents),
		Columns:    toColumnData(columns),
	}
}

// resolveModelNames converts model IDs to model paths/names.
func (h *Handlers) resolveModelNames(ids []string) []string {
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		model, err := h.store.GetModelByID(id)
		if err != nil {
			continue
		}
		names = append(names, model.Path)
	}
	return names
}

// buildExplorerTree groups models into a tree structure by folder.
func buildExplorerTree(models []*core.PersistedModel) []commonComponents.TreeNode {
	folders := make(map[string]*commonComponents.TreeNode)

	for _, m := range models {
		folder := extractFolder(m.Path)

		if _, ok := folders[folder]; !ok {
			folders[folder] = &commonComponents.TreeNode{
				Name:     folder,
				Path:     folder,
				Type:     "folder",
				Children: []commonComponents.TreeNode{},
			}
		}

		folders[folder].Children = append(folders[folder].Children, commonComponents.TreeNode{
			Name: m.Name,
			Path: m.Path,
			Type: "model",
		})
	}

	// Convert map to sorted slice
	result := make([]commonComponents.TreeNode, 0, len(folders))
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

// toColumnData converts core.ColumnInfo to component-friendly data.
func toColumnData(columns []core.ColumnInfo) []commonComponents.ColumnData {
	result := make([]commonComponents.ColumnData, len(columns))
	for i, col := range columns {
		sources := make([]string, len(col.Sources))
		for j, src := range col.Sources {
			sources[j] = fmt.Sprintf("%s.%s", src.Table, src.Column)
		}
		result[i] = commonComponents.ColumnData{
			Name:    col.Name,
			Type:    string(col.TransformType),
			Sources: sources,
		}
	}
	return result
}
