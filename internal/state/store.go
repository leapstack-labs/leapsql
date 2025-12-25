// Package state provides state management for DBGo using SQLite.
// It tracks runs, models, execution history, and dependencies.
//
// Note: Core types are now defined in pkg/core. This package re-exports
// them via type aliases for backward compatibility. New code should
// import pkg/core directly.
package state

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// Type aliases for backward compatibility - these types are now defined in pkg/core.
// Use core.* types directly in new code.
type (
	// Store is an alias for core.Store.
	Store = core.Store

	// RunStatus is an alias for core.RunStatus.
	RunStatus = core.RunStatus

	// Run is an alias for core.Run.
	Run = core.Run

	// ModelRunStatus is an alias for core.ModelRunStatus.
	ModelRunStatus = core.ModelRunStatus

	// ModelRun is an alias for core.ModelRun.
	ModelRun = core.ModelRun

	// Model is an alias for core.PersistedModel.
	// Note: PersistedModel embeds *core.Model for composition.
	Model = core.PersistedModel

	// Dependency is an alias for core.Dependency.
	Dependency = core.Dependency

	// Environment is an alias for core.Environment.
	Environment = core.Environment

	// SourceRef is an alias for core.SourceRef.
	SourceRef = core.SourceRef

	// ColumnInfo is an alias for core.ColumnInfo.
	ColumnInfo = core.ColumnInfo

	// TraceResult is an alias for core.TraceResult.
	TraceResult = core.TraceResult

	// MacroNamespace is an alias for core.MacroNamespace.
	MacroNamespace = core.MacroNamespace

	// MacroFunction is an alias for core.MacroFunction.
	MacroFunction = core.MacroFunction

	// TestConfig is an alias for core.TestConfig.
	TestConfig = core.TestConfig

	// AcceptedValuesConfig is an alias for core.AcceptedValuesConfig.
	AcceptedValuesConfig = core.AcceptedValuesConfig
)

// Re-export status constants from core for backward compatibility.
const (
	RunStatusRunning   = core.RunStatusRunning
	RunStatusCompleted = core.RunStatusCompleted
	RunStatusFailed    = core.RunStatusFailed
	RunStatusCancelled = core.RunStatusCancelled

	ModelRunStatusPending = core.ModelRunStatusPending
	ModelRunStatusRunning = core.ModelRunStatusRunning
	ModelRunStatusSuccess = core.ModelRunStatusSuccess
	ModelRunStatusFailed  = core.ModelRunStatusFailed
	ModelRunStatusSkipped = core.ModelRunStatusSkipped
)
