// Package types provides shared types for the home feature.
package types //nolint:revive // intentional: imported with alias hometypes

// DashboardStats holds stats for the dashboard view.
type DashboardStats struct {
	ModelCount  int
	SourceCount int
	RunCount    int
}
