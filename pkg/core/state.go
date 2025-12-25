package core

import "time"

// Store defines the interface for state management operations.
type Store interface {
	Open(path string) error
	Close() error
	InitSchema() error

	// Run operations
	CreateRun(env string) (*Run, error)
	GetRun(id string) (*Run, error)
	CompleteRun(id string, status RunStatus, errMsg string) error
	GetLatestRun(env string) (*Run, error)

	// Model operations (uses PersistedModel for storage)
	RegisterModel(model *PersistedModel) error
	GetModelByID(id string) (*PersistedModel, error)
	GetModelByPath(path string) (*PersistedModel, error)
	GetModelByFilePath(filePath string) (*PersistedModel, error)
	UpdateModelHash(id string, contentHash string) error
	ListModels() ([]*PersistedModel, error)
	DeleteModelByFilePath(filePath string) error

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
	DeleteMacroNamespaceByFilePath(filePath string) error

	// File hash tracking
	GetContentHash(filePath string) (string, error)
	SetContentHash(filePath, hash, fileType string) error
	DeleteContentHash(filePath string) error

	// List tracked file paths
	ListModelFilePaths() ([]string, error)
	ListMacroFilePaths() ([]string, error)

	// Column snapshot operations
	SaveColumnSnapshot(runID, modelPath, sourceTable string, columns []string) error
	GetColumnSnapshot(modelPath, sourceTable string) (columns []string, runID string, err error)
	DeleteOldSnapshots(keepRuns int) error

	// Batch operations
	BatchGetAllColumns() (map[string][]ColumnInfo, error)
	BatchGetAllDependencies() (map[string][]string, error)
	BatchGetAllDependents() (map[string][]string, error)
}

// RunStatus represents the status of a pipeline run.
type RunStatus string

// Run status constants.
const (
	RunStatusRunning   RunStatus = "running"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCancelled RunStatus = "cancelled"
)

// Run represents a pipeline execution session.
type Run struct {
	ID          string
	Environment string
	Status      RunStatus
	StartedAt   time.Time
	CompletedAt *time.Time
	Error       string
}

// ModelRunStatus represents the status of an individual model execution.
type ModelRunStatus string

// Model run status constants.
const (
	ModelRunStatusPending ModelRunStatus = "pending"
	ModelRunStatusRunning ModelRunStatus = "running"
	ModelRunStatusSuccess ModelRunStatus = "success"
	ModelRunStatusFailed  ModelRunStatus = "failed"
	ModelRunStatusSkipped ModelRunStatus = "skipped"
)

// PersistedModel represents a model stored in the state database.
// It wraps core.Model with persistence-specific fields.
type PersistedModel struct {
	*Model             // Embed core identity
	ID          string // Database primary key
	ContentHash string // For change detection
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ModelRun represents a single execution of a model within a run.
type ModelRun struct {
	ID           string
	RunID        string
	ModelID      string
	Status       ModelRunStatus
	RowsAffected int64
	StartedAt    time.Time
	CompletedAt  *time.Time
	Error        string
	ExecutionMS  int64
}

// Dependency represents an edge in the model dependency graph.
type Dependency struct {
	ModelID  string
	ParentID string
}

// Environment represents a virtual environment pointer.
type Environment struct {
	Name      string
	CommitRef string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// TraceResult represents a single node in a column lineage trace.
type TraceResult struct {
	ModelPath  string
	ColumnName string
	Depth      int
	IsExternal bool
}

// MacroNamespace represents a macro namespace from a .star file.
type MacroNamespace struct {
	Name      string
	FilePath  string
	Package   string
	UpdatedAt string
}

// MacroFunction represents a function exported from a macro namespace.
type MacroFunction struct {
	Namespace string
	Name      string
	Args      []string
	Docstring string
	Line      int
}
