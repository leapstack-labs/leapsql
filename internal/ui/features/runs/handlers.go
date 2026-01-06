// Package runs provides run history handlers for the UI.
package runs

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/runs/components"
	"github.com/leapstack-labs/leapsql/internal/ui/features/runs/pages"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the runs history feature.
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

// RunsPage renders the runs history page.
func (h *Handlers) RunsPage(w http.ResponseWriter, r *http.Request) {
	isDev := true // TODO: Get from context
	if err := pages.RunsPage("Run History", isDev).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// RunsListSSE sends the list of recent runs via SSE.
func (h *Handlers) RunsListSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)

	// Parse limit from query param, default to 50
	limit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	runs, err := h.store.ListRuns(limit)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to list runs: %w", err))
		return
	}

	// Convert to display data
	runItems := make([]components.RunItem, len(runs))
	for i, run := range runs {
		runItems[i] = components.RunItem{
			ID:          run.ID,
			Environment: run.Environment,
			Status:      string(run.Status),
			StartedAt:   run.StartedAt,
			CompletedAt: run.CompletedAt,
			Error:       run.Error,
		}
	}

	if err := sse.PatchElementTempl(components.RunsList(runItems)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// RunDetailSSE sends a single run's details via SSE.
func (h *Handlers) RunDetailSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	runID := chi.URLParam(r, "id")

	run, err := h.store.GetRun(runID)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to get run: %w", err))
		return
	}

	runDetail := components.RunDetail{
		ID:          run.ID,
		Environment: run.Environment,
		Status:      string(run.Status),
		StartedAt:   run.StartedAt,
		CompletedAt: run.CompletedAt,
		Error:       run.Error,
	}

	// Calculate duration
	if run.CompletedAt != nil {
		runDetail.Duration = run.CompletedAt.Sub(run.StartedAt)
	} else {
		runDetail.Duration = time.Since(run.StartedAt)
	}

	if err := sse.PatchElementTempl(components.RunDetailView(runDetail)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// RunModelsSSE sends the model runs for a specific run via SSE.
func (h *Handlers) RunModelsSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	runID := chi.URLParam(r, "id")

	modelRuns, err := h.store.GetModelRunsForRun(runID)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to get model runs: %w", err))
		return
	}

	// Convert to display data with model names
	modelItems := make([]components.ModelRunItem, len(modelRuns))
	for i, mr := range modelRuns {
		modelName := mr.ModelID // Default to ID
		// Try to get model name
		if model, err := h.store.GetModelByID(mr.ModelID); err == nil && model != nil {
			modelName = model.Name
		}

		modelItems[i] = components.ModelRunItem{
			ID:           mr.ID,
			ModelID:      mr.ModelID,
			ModelName:    modelName,
			Status:       string(mr.Status),
			RowsAffected: mr.RowsAffected,
			ExecutionMS:  mr.ExecutionMS,
			StartedAt:    mr.StartedAt,
			CompletedAt:  mr.CompletedAt,
			Error:        mr.Error,
		}
	}

	if err := sse.PatchElementTempl(components.ModelRunsList(runID, modelItems)); err != nil {
		_ = sse.ConsoleError(err)
	}
}
