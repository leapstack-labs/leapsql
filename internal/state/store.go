// Package state provides state management for DBGo using SQLite.
// It tracks runs, models, execution history, and dependencies.
package state

import (
	"time"
)

// RunStatus represents the status of a pipeline run.
type RunStatus string

const (
	RunStatusRunning   RunStatus = "running"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCancelled RunStatus = "cancelled"
)

// ModelRunStatus represents the status of an individual model execution.
type ModelRunStatus string

const (
	ModelRunStatusPending ModelRunStatus = "pending"
	ModelRunStatusRunning ModelRunStatus = "running"
	ModelRunStatusSuccess ModelRunStatus = "success"
	ModelRunStatusFailed  ModelRunStatus = "failed"
	ModelRunStatusSkipped ModelRunStatus = "skipped"
)

// Run represents a pipeline execution session.
type Run struct {
	ID          string     `json:"id"`
	Environment string     `json:"environment"`
	Status      RunStatus  `json:"status"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

// Model represents a registered model in the state store.
type Model struct {
	ID           string    `json:"id"`
	Path         string    `json:"path"`         // e.g., "models.staging.stg_users"
	Name         string    `json:"name"`         // e.g., "stg_users"
	Materialized string    `json:"materialized"` // "table", "view", "incremental"
	UniqueKey    string    `json:"unique_key,omitempty"`
	ContentHash  string    `json:"content_hash"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ModelRun represents a single execution of a model within a run.
type ModelRun struct {
	ID           string         `json:"id"`
	RunID        string         `json:"run_id"`
	ModelID      string         `json:"model_id"`
	Status       ModelRunStatus `json:"status"`
	RowsAffected int64          `json:"rows_affected"`
	StartedAt    time.Time      `json:"started_at"`
	CompletedAt  *time.Time     `json:"completed_at,omitempty"`
	Error        string         `json:"error,omitempty"`
	ExecutionMS  int64          `json:"execution_ms"`
}

// Dependency represents an edge in the model dependency graph.
type Dependency struct {
	ModelID  string `json:"model_id"`
	ParentID string `json:"parent_id"`
}

// Environment represents a virtual environment pointer.
type Environment struct {
	Name      string    `json:"name"`
	CommitRef string    `json:"commit_ref,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// StateStore defines the interface for state management operations.
type StateStore interface {
	// Open opens a connection to the state store at the given path.
	Open(path string) error

	// Close closes the state store connection.
	Close() error

	// InitSchema initializes the database schema.
	InitSchema() error

	// Run operations
	CreateRun(env string) (*Run, error)
	GetRun(id string) (*Run, error)
	CompleteRun(id string, status RunStatus, errMsg string) error
	GetLatestRun(env string) (*Run, error)

	// Model operations
	RegisterModel(model *Model) error
	GetModelByID(id string) (*Model, error)
	GetModelByPath(path string) (*Model, error)
	UpdateModelHash(id string, contentHash string) error
	ListModels() ([]*Model, error)

	// Model run operations
	RecordModelRun(modelRun *ModelRun) error
	UpdateModelRun(id string, status ModelRunStatus, rowsAffected int64, errMsg string) error
	GetModelRunsForRun(runID string) ([]*ModelRun, error)
	GetLatestModelRun(modelID string) (*ModelRun, error)

	// Dependency operations
	SetDependencies(modelID string, parentIDs []string) error
	GetDependencies(modelID string) ([]string, error)
	GetDependents(modelID string) ([]string, error)

	// Environment operations
	CreateEnvironment(name string) (*Environment, error)
	GetEnvironment(name string) (*Environment, error)
	UpdateEnvironmentRef(name string, commitRef string) error
}
