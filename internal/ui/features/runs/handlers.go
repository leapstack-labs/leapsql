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
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
	commonComponents "github.com/leapstack-labs/leapsql/internal/ui/features/common/components"
	"github.com/leapstack-labs/leapsql/internal/ui/features/runs/components"
	"github.com/leapstack-labs/leapsql/internal/ui/features/runs/pages"
	"github.com/leapstack-labs/leapsql/internal/ui/notifier"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/starfederation/datastar-go/datastar"
)

// Handlers provides HTTP handlers for the runs history feature.
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

// RunsPage renders the runs history page with full content.
func (h *Handlers) RunsPage(w http.ResponseWriter, r *http.Request) {
	appData, err := h.buildRunsAppData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := pages.RunsPage("Run History", h.isDev, appData).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// RunsPageUpdates is the long-lived SSE endpoint for the runs page.
// It subscribes to updates and pushes changes when the store changes.
// Unlike the old pattern, it does NOT send initial state - that's rendered by RunsPage.
func (h *Handlers) RunsPageUpdates(w http.ResponseWriter, r *http.Request) {
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
			if err := h.sendRunsView(sse); err != nil {
				_ = sse.ConsoleError(err)
			}
		}
	}
}

// sendRunsView builds and sends the full app view for the runs page.
func (h *Handlers) sendRunsView(sse *datastar.ServerSentEventGenerator) error {
	appData, err := h.buildRunsAppData()
	if err != nil {
		return err
	}
	return sse.PatchElementTempl(commonComponents.AppContainer(appData))
}

// buildRunsAppData assembles all data needed for the runs view.
func (h *Handlers) buildRunsAppData() (commonComponents.AppData, error) {
	data := commonComponents.AppData{
		CurrentPath: "/runs",
	}

	// Build explorer tree
	models, err := h.store.ListModels()
	if err != nil {
		return data, err
	}
	data.ExplorerTree = common.BuildExplorerTree(models)

	// Get runs
	runs, err := h.store.ListRuns(50)
	if err != nil {
		return data, err
	}

	// Convert to view data
	runsData := commonComponents.RunsViewData{
		Runs: h.convertToRunItems(runs),
	}
	data.Runs = &runsData

	return data, nil
}

// convertToRunItems converts core.Run to component-friendly RunItem.
func (h *Handlers) convertToRunItems(runs []*core.Run) []commonComponents.RunItem {
	items := make([]commonComponents.RunItem, len(runs))
	for i, run := range runs {
		items[i] = commonComponents.RunItem{
			ID:          run.ID,
			Environment: run.Environment,
			Status:      string(run.Status),
			StartedAt:   formatTimeAgo(run.StartedAt),
			Duration:    formatRunDuration(run.StartedAt, run.CompletedAt),
			Error:       run.Error,
		}
	}
	return items
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

// formatTimeAgo formats a time as a human-readable relative time string.
func formatTimeAgo(t time.Time) string {
	now := time.Now()
	diff := now.Sub(t)

	if diff < time.Minute {
		return "just now"
	}
	if diff < time.Hour {
		mins := int(diff.Minutes())
		if mins == 1 {
			return "1 minute ago"
		}
		return fmt.Sprintf("%d minutes ago", mins)
	}
	if diff < 24*time.Hour {
		hours := int(diff.Hours())
		if hours == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", hours)
	}
	return t.Format("Jan 2, 15:04")
}

// formatRunDuration formats the duration between start and end times.
func formatRunDuration(start time.Time, end *time.Time) string {
	var d time.Duration
	if end != nil {
		d = end.Sub(start)
	} else {
		d = time.Since(start)
	}

	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.Round(time.Second).String()
}
