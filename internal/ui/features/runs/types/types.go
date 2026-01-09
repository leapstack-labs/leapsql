// Package types provides shared types for the runs feature.
package types //nolint:revive // intentional: imported with alias runstypes

// RunsViewData holds data for the runs history page.
type RunsViewData struct {
	Runs          []RunListItem
	SelectedRunID string
	SelectedRun   *RunDetailWithTiers
}

// RunListItem is a compact run representation for the list view.
type RunListItem struct {
	ID          string
	Environment string
	Status      string
	StartedAt   string // formatted time string
	Duration    string // formatted duration
	Stats       RunStats
	Error       string
}

// RunStats holds aggregate stats for a run.
type RunStats struct {
	TotalModels   int
	Succeeded     int
	Failed        int
	Skipped       int
	Pending       int
	Running       int
	TotalDuration int64 // milliseconds
}

// TieredModelRun represents a model run with tier information.
type TieredModelRun struct {
	ID           string
	ModelID      string
	ModelPath    string
	ModelName    string
	Status       string
	RowsAffected int64
	ExecutionMS  int64
	RenderMS     int64
	Error        string
	Tier         int // Execution tier (0 = no deps, 1 = depends on tier 0, etc.)
}

// TierGroup represents a group of models at the same execution tier.
type TierGroup struct {
	Tier      int
	Label     string // e.g., "Tier 0: Sources"
	Models    []TieredModelRun
	Stats     TierStats
	Collapsed bool // For tiers not reached due to failure
}

// TierStats holds aggregate stats for a tier.
type TierStats struct {
	TotalModels   int
	Succeeded     int
	Failed        int
	Skipped       int
	Pending       int
	Running       int
	TotalDuration int64 // milliseconds
}

// RunDetailWithTiers holds full run info with tiered model runs.
type RunDetailWithTiers struct {
	ID          string
	Environment string
	Status      string
	StartedAt   string
	CompletedAt string
	Duration    string
	Error       string
	Stats       RunStats
	Tiers       []TierGroup
}
