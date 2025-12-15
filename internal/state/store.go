// Package state provides state management for DBGo using SQLite.
// It tracks runs, models, execution history, and dependencies.
package state

import (
	"time"
)

// RunStatus represents the status of a pipeline run.
type RunStatus string

// RunStatus constants for pipeline execution states.
const (
	RunStatusRunning   RunStatus = "running"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCancelled RunStatus = "cancelled"
)

// ModelRunStatus represents the status of an individual model execution.
type ModelRunStatus string

// ModelRunStatus constants for individual model execution states.
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
	ID           string         `json:"id"`
	Path         string         `json:"path"`         // e.g., "models.staging.stg_users"
	Name         string         `json:"name"`         // e.g., "stg_users"
	Materialized string         `json:"materialized"` // "table", "view", "incremental"
	UniqueKey    string         `json:"unique_key,omitempty"`
	ContentHash  string         `json:"content_hash"`
	FilePath     string         `json:"file_path,omitempty"` // Absolute path to .sql file
	Owner        string         `json:"owner,omitempty"`
	Schema       string         `json:"schema,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Tests        []TestConfig   `json:"tests,omitempty"`
	Meta         map[string]any `json:"meta,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

// TestConfig represents a test configuration for a model.
type TestConfig struct {
	Unique         []string              `json:"unique,omitempty"`
	NotNull        []string              `json:"not_null,omitempty"`
	AcceptedValues *AcceptedValuesConfig `json:"accepted_values,omitempty"`
}

// AcceptedValuesConfig represents accepted values test configuration.
type AcceptedValuesConfig struct {
	Column string   `json:"column"`
	Values []string `json:"values"`
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

// SourceRef represents a source column reference in lineage.
type SourceRef struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// ColumnInfo represents column lineage information for a model.
type ColumnInfo struct {
	Name          string      `json:"name"`
	Index         int         `json:"index"`
	TransformType string      `json:"transform_type"` // "" (direct) or "EXPR"
	Function      string      `json:"function"`       // "sum", "count", etc.
	Sources       []SourceRef `json:"sources"`        // where this column comes from
}

// TraceResult represents a single node in a column lineage trace.
type TraceResult struct {
	ModelPath  string `json:"model_path"`
	ColumnName string `json:"column_name"`
	Depth      int    `json:"depth"`
	IsExternal bool   `json:"is_external"` // true if source_table is not a known model
}

// MacroNamespace represents a macro namespace from a .star file.
type MacroNamespace struct {
	Name      string `json:"name"`       // e.g., "utils", "datetime"
	FilePath  string `json:"file_path"`  // Absolute path to .star file
	Package   string `json:"package"`    // "" for local, package name for vendor
	UpdatedAt string `json:"updated_at"` // ISO timestamp
}

// MacroFunction represents a function exported from a macro namespace.
type MacroFunction struct {
	Namespace string   `json:"namespace"` // Parent namespace name
	Name      string   `json:"name"`      // Function name
	Args      []string `json:"args"`      // Argument names with defaults
	Docstring string   `json:"docstring"` // Function documentation
	Line      int      `json:"line"`      // Line number for go-to-definition
}

// Store defines the interface for state management operations.
type Store interface {
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
	GetModelByFilePath(filePath string) (*Model, error) // NEW: lookup by file system path
	UpdateModelHash(id string, contentHash string) error
	ListModels() ([]*Model, error)
	DeleteModelByFilePath(filePath string) error // NEW: cleanup deleted files

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

	// Column lineage operations
	SaveModelColumns(modelPath string, columns []ColumnInfo) error
	GetModelColumns(modelPath string) ([]ColumnInfo, error)
	DeleteModelColumns(modelPath string) error
	TraceColumnBackward(modelPath, columnName string) ([]TraceResult, error)
	TraceColumnForward(modelPath, columnName string) ([]TraceResult, error)

	// Macro operations
	SaveMacroNamespace(ns *MacroNamespace, functions []*MacroFunction) error
	GetMacroNamespaces() ([]*MacroNamespace, error)
	GetMacroNamespace(name string) (*MacroNamespace, error)
	GetMacroFunctions(namespace string) ([]*MacroFunction, error)
	GetMacroFunction(namespace, name string) (*MacroFunction, error)
	MacroFunctionExists(namespace, name string) (bool, error)
	SearchMacroNamespaces(prefix string) ([]*MacroNamespace, error)
	SearchMacroFunctions(namespace, prefix string) ([]*MacroFunction, error)
	DeleteMacroNamespace(name string) error
	DeleteMacroNamespaceByFilePath(filePath string) error // NEW: cleanup by file path

	// File hash tracking for incremental discovery
	GetContentHash(filePath string) (string, error)
	SetContentHash(filePath, hash, fileType string) error
	DeleteContentHash(filePath string) error

	// List tracked file paths (for detecting deletions)
	ListModelFilePaths() ([]string, error)
	ListMacroFilePaths() ([]string, error)
}
