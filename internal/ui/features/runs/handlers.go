// Package runs provides run history handlers for the UI.
package runs

import (
	"fmt"
	"net/http"
	"sort"
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

// buildRunsAppData assembles all data needed for the runs view (no selection).
func (h *Handlers) buildRunsAppData() (commonComponents.AppData, error) {
	return h.buildRunsAppDataWithSelection("")
}

// buildRunsAppDataWithSelection assembles all data needed for the runs view with optional selected run.
func (h *Handlers) buildRunsAppDataWithSelection(selectedRunID string) (commonComponents.AppData, error) {
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

	// Convert to view data with stats
	runsData := commonComponents.RunsViewData{
		Runs:          h.convertToRunListItems(runs),
		SelectedRunID: selectedRunID,
	}

	// If a run is selected, build its detail
	if selectedRunID != "" {
		runDetail, err := h.buildRunDetailWithTiers(selectedRunID)
		if err == nil {
			runsData.SelectedRun = runDetail
		}
		// If error, just don't show the detail (run might have been deleted)
	}

	data.Runs = &runsData

	return data, nil
}

// convertToRunListItems converts core.Run to component-friendly RunListItem with stats.
func (h *Handlers) convertToRunListItems(runs []*core.Run) []commonComponents.RunListItem {
	items := make([]commonComponents.RunListItem, len(runs))
	for i, run := range runs {
		// Get stats for this run
		stats := h.getRunStats(run.ID)

		items[i] = commonComponents.RunListItem{
			ID:          run.ID,
			Environment: run.Environment,
			Status:      string(run.Status),
			StartedAt:   formatTimeAgo(run.StartedAt),
			Duration:    formatRunDuration(run.StartedAt, run.CompletedAt),
			Stats:       stats,
			Error:       run.Error,
		}
	}
	return items
}

// getRunStats calculates aggregate stats for a run.
func (h *Handlers) getRunStats(runID string) commonComponents.RunStats {
	stats := commonComponents.RunStats{}

	modelRuns, err := h.store.GetModelRunsForRun(runID)
	if err != nil {
		return stats
	}

	stats.TotalModels = len(modelRuns)
	for _, mr := range modelRuns {
		switch mr.Status {
		case core.ModelRunStatusSuccess:
			stats.Succeeded++
		case core.ModelRunStatusFailed:
			stats.Failed++
		case core.ModelRunStatusSkipped:
			stats.Skipped++
		case core.ModelRunStatusPending:
			stats.Pending++
		case core.ModelRunStatusRunning:
			stats.Running++
		}
		stats.TotalDuration += mr.ExecutionMS
	}

	return stats
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

// RunDetailSSE sends the full app view with the selected run's details via SSE (fat morph).
func (h *Handlers) RunDetailSSE(w http.ResponseWriter, r *http.Request) {
	sse := datastar.NewSSE(w, r)
	runID := chi.URLParam(r, "id")

	appData, err := h.buildRunsAppDataWithSelection(runID)
	if err != nil {
		_ = sse.ConsoleError(fmt.Errorf("failed to build app data: %w", err))
		return
	}

	// Fat morph - send entire AppContainer
	if err := sse.PatchElementTempl(commonComponents.AppContainer(appData)); err != nil {
		_ = sse.ConsoleError(err)
	}
}

// buildRunDetailWithTiers builds a full run detail with tiered model runs.
func (h *Handlers) buildRunDetailWithTiers(runID string) (*commonComponents.RunDetailWithTiers, error) {
	run, err := h.store.GetRun(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get run: %w", err)
	}

	// Get model runs with model info
	modelRuns, err := h.store.GetModelRunsWithModelInfo(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get model runs: %w", err)
	}

	// Convert to tiered model runs
	tieredModels := h.convertToTieredModelRuns(modelRuns)

	// Calculate tiers using dependencies
	tiers := h.groupByTier(tieredModels)

	// Calculate stats
	stats := h.calculateStats(tieredModels)

	// Format times
	startedAt := run.StartedAt.Format("Jan 2, 15:04:05")
	completedAt := ""
	if run.CompletedAt != nil {
		completedAt = run.CompletedAt.Format("Jan 2, 15:04:05")
	}
	duration := formatRunDuration(run.StartedAt, run.CompletedAt)

	return &commonComponents.RunDetailWithTiers{
		ID:          run.ID,
		Environment: run.Environment,
		Status:      string(run.Status),
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		Duration:    duration,
		Error:       run.Error,
		Stats:       stats,
		Tiers:       tiers,
	}, nil
}

// convertToTieredModelRuns converts model runs with info to tiered model runs.
func (h *Handlers) convertToTieredModelRuns(modelRuns []*core.ModelRunWithInfo) []commonComponents.TieredModelRun {
	result := make([]commonComponents.TieredModelRun, len(modelRuns))
	for i, mr := range modelRuns {
		result[i] = commonComponents.TieredModelRun{
			ID:           mr.ID,
			ModelID:      mr.ModelID,
			ModelPath:    mr.ModelPath,
			ModelName:    mr.ModelName,
			Status:       string(mr.Status),
			RowsAffected: mr.RowsAffected,
			ExecutionMS:  mr.ExecutionMS,
			RenderMS:     mr.RenderMS,
			Error:        mr.Error,
			Tier:         0, // Will be calculated in groupByTier
		}
	}
	return result
}

// groupByTier groups models by their execution tier based on dependencies.
func (h *Handlers) groupByTier(models []commonComponents.TieredModelRun) []commonComponents.TierGroup {
	if len(models) == 0 {
		return nil
	}

	// Get all dependencies
	deps, err := h.store.BatchGetAllDependencies()
	if err != nil {
		// Fall back to single tier
		return h.fallbackSingleTier(models)
	}

	// Build reverse index: model path -> index in models slice
	pathToIndex := make(map[string]int)
	for i, m := range models {
		pathToIndex[m.ModelPath] = i
	}

	// Calculate tier for each model using BFS
	modelTiers := make(map[string]int)
	for _, m := range models {
		modelTiers[m.ModelPath] = h.calculateTier(m.ModelPath, deps, make(map[string]bool))
	}

	// Update tier in models and group by tier
	tierGroups := make(map[int][]commonComponents.TieredModelRun)
	maxTier := 0

	for i := range models {
		tier := modelTiers[models[i].ModelPath]
		models[i].Tier = tier
		tierGroups[tier] = append(tierGroups[tier], models[i])
		if tier > maxTier {
			maxTier = tier
		}
	}

	// Create tier groups in order
	result := make([]commonComponents.TierGroup, 0, maxTier+1)
	for tier := 0; tier <= maxTier; tier++ {
		modelsInTier := tierGroups[tier]
		if len(modelsInTier) == 0 {
			continue
		}

		// Sort models within tier by name
		sort.Slice(modelsInTier, func(i, j int) bool {
			return modelsInTier[i].ModelName < modelsInTier[j].ModelName
		})

		// Calculate tier stats
		tierStats := h.calculateTierStats(modelsInTier)

		result = append(result, commonComponents.TierGroup{
			Tier:      tier,
			Label:     h.generateTierLabel(tier, modelsInTier),
			Models:    modelsInTier,
			Stats:     tierStats,
			Collapsed: false,
		})
	}

	return result
}

// calculateTier calculates the tier (depth) of a model based on dependencies.
func (h *Handlers) calculateTier(modelPath string, deps map[string][]string, visited map[string]bool) int {
	if visited[modelPath] {
		return 0 // Avoid infinite loops
	}
	visited[modelPath] = true

	parents := deps[modelPath]
	if len(parents) == 0 {
		return 0
	}

	maxParentTier := 0
	for _, parent := range parents {
		parentTier := h.calculateTier(parent, deps, visited)
		if parentTier >= maxParentTier {
			maxParentTier = parentTier + 1
		}
	}

	return maxParentTier
}

// fallbackSingleTier creates a single tier with all models.
func (h *Handlers) fallbackSingleTier(models []commonComponents.TieredModelRun) []commonComponents.TierGroup {
	tierStats := h.calculateTierStats(models)
	return []commonComponents.TierGroup{
		{
			Tier:   0,
			Label:  "All Models",
			Models: models,
			Stats:  tierStats,
		},
	}
}

// calculateStats calculates overall stats from model runs.
func (h *Handlers) calculateStats(models []commonComponents.TieredModelRun) commonComponents.RunStats {
	stats := commonComponents.RunStats{
		TotalModels: len(models),
	}
	for _, m := range models {
		switch m.Status {
		case "success":
			stats.Succeeded++
		case "failed":
			stats.Failed++
		case "skipped":
			stats.Skipped++
		case "pending":
			stats.Pending++
		case "running":
			stats.Running++
		}
		stats.TotalDuration += m.ExecutionMS
	}
	return stats
}

// calculateTierStats calculates stats for a specific tier.
func (h *Handlers) calculateTierStats(models []commonComponents.TieredModelRun) commonComponents.TierStats {
	stats := commonComponents.TierStats{
		TotalModels: len(models),
	}
	for _, m := range models {
		switch m.Status {
		case "success":
			stats.Succeeded++
		case "failed":
			stats.Failed++
		case "skipped":
			stats.Skipped++
		case "pending":
			stats.Pending++
		case "running":
			stats.Running++
		}
		stats.TotalDuration += m.ExecutionMS
	}
	return stats
}

// generateTierLabel generates a label for a tier.
func (h *Handlers) generateTierLabel(tier int, models []commonComponents.TieredModelRun) string {
	if len(models) == 0 {
		return fmt.Sprintf("Tier %d", tier)
	}

	// Try to detect common folder prefix
	folders := make(map[string]int)
	for _, m := range models {
		// Extract folder from path (e.g., "staging.customers" -> "staging")
		path := m.ModelPath
		for i := 0; i < len(path); i++ {
			if path[i] == '.' {
				folders[path[:i]]++
				break
			}
		}
	}

	// If all models share the same folder, use it as label
	for folder, count := range folders {
		if count == len(models) {
			return fmt.Sprintf("Tier %d: %s", tier, folder)
		}
	}

	return fmt.Sprintf("Tier %d", tier)
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
