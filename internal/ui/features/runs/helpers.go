// Package runs provides run history handlers for the UI.
package runs

import (
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
)

// CSS class helpers

func runStatusBadgeClass(status string) string {
	switch status {
	case "completed":
		return "run-status--completed"
	case "running":
		return "run-status--running"
	case "failed":
		return "run-status--failed"
	case "cancelled":
		return "run-status--cancelled"
	default:
		return ""
	}
}

func truncateID(id string) string {
	if len(id) > 7 {
		return id[:7]
	}
	return id
}

func runListCardStatusClass(status string) string {
	switch status {
	case "completed":
		return "run-list-card--completed"
	case "running":
		return "run-list-card--running"
	case "failed":
		return "run-list-card--failed"
	case "cancelled":
		return "run-list-card--cancelled"
	default:
		return ""
	}
}

func statusIconClass(status string) string {
	switch status {
	case "success", "completed":
		return "status-icon--success"
	case "running":
		return "status-icon--running"
	case "failed":
		return "status-icon--failed"
	case "skipped":
		return "status-icon--skipped"
	case "pending":
		return "status-icon--pending"
	default:
		return ""
	}
}

func statusIconSizeClass(size string) string {
	switch size {
	case "lg":
		return "status-icon--lg"
	case "md":
		return "status-icon--md"
	case "sm":
		return "status-icon--sm"
	default:
		return "status-icon--md"
	}
}

func modelRunCardClass(status string) string {
	switch status {
	case "failed":
		return "model-run-card--failed"
	case "skipped":
		return "model-run-card--skipped"
	case "running":
		return "model-run-card--running"
	default:
		return ""
	}
}

func formatDurationMS(ms int64) string {
	if ms < 1000 {
		return common.Itoa(int(ms)) + "ms"
	}
	secs := float64(ms) / 1000
	if secs < 60 {
		whole := int(secs)
		frac := int((secs - float64(whole)) * 10)
		return common.Itoa(whole) + "." + common.Itoa(frac) + "s"
	}
	mins := int(secs / 60)
	remainSecs := int(secs) % 60
	return common.Itoa(mins) + "m" + common.Itoa(remainSecs) + "s"
}

// MaxDurationFromSlice finds the maximum duration from a slice of execution times.
// This is called from the templ file with extracted ExecutionMS values.
func MaxDurationFromSlice(durations []int64) int64 {
	var maxDuration int64
	for _, d := range durations {
		if d > maxDuration {
			maxDuration = d
		}
	}
	return maxDuration
}
