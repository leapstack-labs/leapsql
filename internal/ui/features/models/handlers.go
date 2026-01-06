package models

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/models/components"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the models feature.
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

// ModelSSE sends model details via SSE, patching both content and context panel.
func (h *Handlers) ModelSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	modelPath := chi.URLParam(r, "path")

	// Get model from store
	model, err := h.store.GetModelByPath(modelPath)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("model not found: %s", modelPath))
		return
	}

	// Get dependencies
	deps, _ := h.store.GetDependencies(model.ID)
	dependents, _ := h.store.GetDependents(model.ID)

	// Get columns
	columns, _ := h.store.GetModelColumns(modelPath)

	// Resolve dependency names
	depNames := h.resolveModelNames(deps)
	dependentNames := h.resolveModelNames(dependents)

	// Build model data for templates
	modelData := components.ModelData{
		Path:         model.Path,
		Name:         model.Name,
		FilePath:     model.FilePath,
		Materialized: model.Materialized,
		Schema:       model.Schema,
		Description:  model.Description,
		Owner:        model.Owner,
		SQL:          model.SQL,
		Tags:         model.Tags,
	}

	contextData := components.ModelContext{
		Path:       model.Path,
		Name:       model.Name,
		Type:       model.Materialized,
		Schema:     model.Schema,
		DependsOn:  depNames,
		Dependents: dependentNames,
		Columns:    toColumnData(columns),
	}

	// Patch main content area with model detail
	if err := sse.PatchElementTempl(components.ModelDetail(modelData)); err != nil {
		_ = sse.ConsoleError(err)
		return
	}

	// Patch context panel with model context
	if err := sse.PatchElementTempl(components.ModelContextPanel(contextData)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// SQLViewSSE sends raw SQL via SSE.
func (h *Handlers) SQLViewSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	modelPath := chi.URLParam(r, "path")

	model, err := h.store.GetModelByPath(modelPath)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("model not found: %s", modelPath))
		return
	}

	if err := sse.PatchElementTempl(components.SQLView(model.SQL, false)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// CompiledSSE sends compiled SQL via SSE.
func (h *Handlers) CompiledSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	modelPath := chi.URLParam(r, "path")

	model, err := h.store.GetModelByPath(modelPath)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("model not found: %s", modelPath))
		return
	}

	// Compile the model SQL using RenderModel
	compiled, err := h.engine.RenderModel(model.Path)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("compile error: %w", err))
		return
	}

	if err := sse.PatchElementTempl(components.SQLView(compiled, true)); err != nil {
		_ = sse.ConsoleError(err)
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
func toColumnData(columns []core.ColumnInfo) []components.ColumnData {
	result := make([]components.ColumnData, len(columns))
	for i, col := range columns {
		sources := make([]string, len(col.Sources))
		for j, src := range col.Sources {
			sources[j] = fmt.Sprintf("%s.%s", src.Table, src.Column)
		}
		result[i] = components.ColumnData{
			Name:    col.Name,
			Type:    string(col.TransformType),
			Sources: sources,
		}
	}
	return result
}

// PreviewSSE sends data preview via SSE.
func (h *Handlers) PreviewSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	modelPath := chi.URLParam(r, "path")

	// Get model from store
	model, err := h.store.GetModelByPath(modelPath)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("model not found: %s", modelPath))
		return
	}

	// Ensure database is connected
	if err := h.engine.EnsureConnected(r.Context()); err != nil {
		_ = sse.ConsoleError(fmt.Errorf("database not connected: %w", err))
		return
	}

	adapter := h.engine.GetAdapter()
	if adapter == nil {
		_ = sse.ConsoleError(fmt.Errorf("no database adapter available"))
		return
	}

	// Build preview query - use schema if available
	tableName := model.Name
	if model.Schema != "" {
		tableName = model.Schema + "." + model.Name
	}

	const previewLimit = 50
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableName, previewLimit)

	// Execute query
	rows, err := adapter.Query(r.Context(), query)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("query error: %w", err))
		return
	}
	defer func() { _ = rows.Close() }()

	// Get column names
	columnNames, err := rows.Columns()
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to get columns: %w", err))
		return
	}

	// Read all rows
	var data [][]string
	for rows.Next() {
		// Create a slice of interface{} to hold each column value
		values := make([]any, len(columnNames))
		valuePtrs := make([]any, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			_ = sse.ConsoleError(fmt.Errorf("scan error: %w", err))
			return
		}

		// Convert values to strings
		row := make([]string, len(columnNames))
		for i, v := range values {
			row[i] = formatValue(v)
		}
		data = append(data, row)
	}

	if err := rows.Err(); err != nil {
		_ = sse.ConsoleError(fmt.Errorf("rows error: %w", err))
		return
	}

	// Build preview data
	preview := components.PreviewData{
		Columns:  columnNames,
		Rows:     data,
		RowCount: len(data),
		Limited:  len(data) >= previewLimit,
	}

	if err := sse.PatchElementTempl(components.DataPreview(preview)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// formatValue converts any value to a string for display.
func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}
