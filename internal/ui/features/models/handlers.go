// Package models provides model detail handlers for the UI.
package models

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
	"github.com/leapstack-labs/leapsql/internal/ui/features/models/pages"
	modelstypes "github.com/leapstack-labs/leapsql/internal/ui/features/models/types"
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

// ModelPage renders the model detail page with full content.
func (h *Handlers) ModelPage(w http.ResponseWriter, r *http.Request) {
	modelPath := chi.URLParam(r, "path")

	sidebar, modelData, contextData, err := h.buildModelData(modelPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get model name for title
	title := modelPath
	if modelData != nil {
		title = modelData.Name
	}

	updatePath := "/models/" + modelPath + "/updates"
	if err := pages.ModelPage(title, h.isDev, sidebar, modelData, contextData, updatePath).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ModelPageUpdates is the long-lived SSE endpoint for a model page.
// It subscribes to updates and pushes changes when the store changes.
// Unlike the old pattern, it does NOT send initial state - that's rendered by ModelPage.
func (h *Handlers) ModelPageUpdates(w http.ResponseWriter, r *http.Request) {
	modelPath := chi.URLParam(r, "path")
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
			if err := h.sendModelView(sse, modelPath); err != nil {
				_ = sse.ConsoleError(err)
				// Don't return - keep trying on next update
			}
		}
	}
}

// sendModelView builds and sends the full app view for a model page.
func (h *Handlers) sendModelView(sse *datastar.ServerSentEventGenerator, modelPath string) error {
	sidebar, modelData, contextData, err := h.buildModelData(modelPath)
	if err != nil {
		return err
	}
	return sse.PatchElementTempl(pages.ModelAppShell(sidebar, modelData, contextData))
}

// buildModelData assembles all data needed for the model view.
func (h *Handlers) buildModelData(modelPath string) (common.SidebarData, *modelstypes.ModelViewData, *modelstypes.ModelContext, error) {
	sidebar := common.SidebarData{
		CurrentPath: "/models/" + modelPath,
		FullWidth:   false,
	}

	// Build explorer tree
	models, err := h.store.ListModels()
	if err != nil {
		return sidebar, nil, nil, err
	}
	sidebar.ExplorerTree = common.BuildExplorerTree(models)

	var modelData *modelstypes.ModelViewData
	var contextData *modelstypes.ModelContext

	// Get model details if path specified
	if modelPath != "" {
		model, err := h.store.GetModelByPath(modelPath)
		if err != nil {
			return sidebar, nil, nil, err
		}

		// Only build model view if model exists
		if model != nil {
			// Build model view data with all tab content
			mv := h.buildModelViewData(model)
			modelData = &mv

			// Build context panel
			ctx := h.buildModelContext(model)
			contextData = &ctx
		}
	}

	return sidebar, modelData, contextData, nil
}

// buildModelViewData builds the model view with all tabs pre-rendered.
func (h *Handlers) buildModelViewData(model *core.PersistedModel) modelstypes.ModelViewData {
	data := modelstypes.ModelViewData{
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
func (h *Handlers) buildModelContext(model *core.PersistedModel) modelstypes.ModelContext {
	deps, _ := h.store.GetDependencies(model.ID)
	dependents, _ := h.store.GetDependents(model.ID)
	columns, _ := h.store.GetModelColumns(model.Path)

	return modelstypes.ModelContext{
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

// toColumnData converts core.ColumnInfo to component-friendly data.
func toColumnData(columns []core.ColumnInfo) []modelstypes.ColumnData {
	result := make([]modelstypes.ColumnData, len(columns))
	for i, col := range columns {
		sources := make([]string, len(col.Sources))
		for j, src := range col.Sources {
			sources[j] = fmt.Sprintf("%s.%s", src.Table, src.Column)
		}
		result[i] = modelstypes.ColumnData{
			Name:    col.Name,
			Type:    string(col.TransformType),
			Sources: sources,
		}
	}
	return result
}
