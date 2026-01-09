// Package runs provides run history handlers for the UI.
package runs

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
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
// Handles both /runs (no selection) and /runs/{id} (with selection).
func (h *Handlers) RunsPage(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id") // "" if not present

	sidebar, runsData, err := h.buildRunsDataWithSelection(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// SSE path depends on whether viewing a specific run
	sseUpdatePath := "/runs/updates"
	if runID != "" {
		sseUpdatePath = fmt.Sprintf("/runs/%s/updates", runID)
	}

	if err := pages.RunsPage("Run History", h.isDev, sidebar, runsData, sseUpdatePath).Render(r.Context(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// RunsPageUpdates is the long-lived SSE endpoint for the runs page.
// Handles both /runs/updates (no selection) and /runs/{id}/updates (with selection).
// Pushes full AppShell on every update so both list and detail stay in sync.
func (h *Handlers) RunsPageUpdates(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id") // "" if not present
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
			sidebar, runsData, err := h.buildRunsDataWithSelection(runID)
			if err != nil {
				_ = sse.ConsoleError(err)
				continue
			}
			if err := sse.PatchElementTempl(pages.RunsAppShell(sidebar, runsData)); err != nil {
				_ = sse.ConsoleError(err)
			}
		}
	}
}

// buildRunsDataWithSelection assembles all data needed for the runs view with optional selected run.
func (h *Handlers) buildRunsDataWithSelection(selectedRunID string) (common.SidebarData, *pages.RunsViewData, error) {
	sidebar := common.SidebarData{
		CurrentPath: "/runs",
		FullWidth:   true,
	}

	// Build explorer tree
	models, err := h.store.ListModels()
	if err != nil {
		return sidebar, nil, err
	}
	sidebar.ExplorerTree = common.BuildExplorerTree(models)

	// Get runs
	runs, err := h.store.ListRuns(50)
	if err != nil {
		return sidebar, nil, err
	}

	// Convert to view data with stats
	runsData := &pages.RunsViewData{
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

	return sidebar, runsData, nil
}

// convertToRunListItems converts core.Run to component-friendly RunListItem with stats.
func (h *Handlers) convertToRunListItems(runs []*core.Run) []pages.RunListItem {
	items := make([]pages.RunListItem, len(runs))
	for i, run := range runs {
		// Get stats for this run
		stats := h.getRunStats(run.ID)

		items[i] = pages.RunListItem{
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
func (h *Handlers) getRunStats(runID string) pages.RunStats {
	stats := pages.RunStats{}

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

// buildRunDetailWithTiers builds a full run detail with tiered model runs.
func (h *Handlers) buildRunDetailWithTiers(runID string) (*pages.RunDetailWithTiers, error) {
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

	return &pages.RunDetailWithTiers{
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
func (h *Handlers) convertToTieredModelRuns(modelRuns []*core.ModelRunWithInfo) []pages.TieredModelRun {
	result := make([]pages.TieredModelRun, len(modelRuns))
	for i, mr := range modelRuns {
		result[i] = pages.TieredModelRun{
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
func (h *Handlers) groupByTier(models []pages.TieredModelRun) []pages.TierGroup {
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
	tierGroups := make(map[int][]pages.TieredModelRun)
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
	result := make([]pages.TierGroup, 0, maxTier+1)
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

		result = append(result, pages.TierGroup{
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
func (h *Handlers) fallbackSingleTier(models []pages.TieredModelRun) []pages.TierGroup {
	tierStats := h.calculateTierStats(models)
	return []pages.TierGroup{
		{
			Tier:   0,
			Label:  "All Models",
			Models: models,
			Stats:  tierStats,
		},
	}
}

// calculateStats calculates overall stats from model runs.
func (h *Handlers) calculateStats(models []pages.TieredModelRun) pages.RunStats {
	stats := pages.RunStats{
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
func (h *Handlers) calculateTierStats(models []pages.TieredModelRun) pages.TierStats {
	stats := pages.TierStats{
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
func (h *Handlers) generateTierLabel(tier int, models []pages.TieredModelRun) string {
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
