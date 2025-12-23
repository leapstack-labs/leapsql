package lint

// Provider is the base interface for all lint providers.
type Provider interface {
	Name() string
}

// SQLProvider analyzes individual SQL statements.
// Implemented by the SQL analyzer for statement-level linting.
type SQLProvider interface {
	Provider
	AnalyzeStatement(stmt any, dialect DialectInfo) []Diagnostic
}

// ProjectProvider analyzes project-level concerns.
// Implemented by the project health analyzer for DAG/architecture linting.
type ProjectProvider interface {
	Provider
	// AnalyzeProject runs project-level rules and returns diagnostics.
	// The context parameter provides all project data needed for analysis.
	AnalyzeProject(ctx ProjectContext) []Diagnostic
}

// ProjectContext provides access to project data for project-level rules.
// This is an interface to avoid import cycles between lint and project packages.
type ProjectContext interface {
	// GetModels returns all models indexed by path.
	GetModels() map[string]ModelInfo

	// GetParents returns upstream model paths for a given model.
	GetParents(modelPath string) []string

	// GetChildren returns downstream model paths for a given model.
	GetChildren(modelPath string) []string

	// GetConfig returns the project health configuration.
	GetConfig() ProjectHealthConfig
}

// ModelInfo represents a model for project-level analysis.
// This mirrors the data needed from parser.ModelConfig without importing it.
type ModelInfo struct {
	Path         string         // Model path (e.g., "staging.customers")
	Name         string         // Model name (e.g., "stg_customers")
	FilePath     string         // Absolute path to .sql file
	Type         ModelType      // Inferred or explicit model type
	Sources      []string       // Table references (deps)
	Columns      []ColumnInfo   // Column-level lineage
	Materialized string         // table, view, incremental
	Tags         []string       // Metadata tags
	Meta         map[string]any // Custom metadata
}

// ModelType represents the semantic type of a model.
type ModelType string

// Model type constants.
const (
	ModelTypeStaging      ModelType = "staging"
	ModelTypeIntermediate ModelType = "intermediate"
	ModelTypeMarts        ModelType = "marts"
	ModelTypeOther        ModelType = "other"
)

// ColumnInfo represents column lineage information for project-level analysis.
type ColumnInfo struct {
	Name          string      // Column name
	TransformType string      // "" (direct) or "EXPR"
	Function      string      // Aggregate/window function name
	Sources       []SourceRef // Where this column comes from
}

// SourceRef represents a source column reference.
type SourceRef struct {
	Table  string
	Column string
}

// ProjectHealthConfig holds configurable thresholds for project health rules.
type ProjectHealthConfig struct {
	ModelFanoutThreshold        int // PM04: default 3
	TooManyJoinsThreshold       int // PM05: default 7
	PassthroughColumnThreshold  int // PL01: default 20
	StarlarkComplexityThreshold int // PT01: default 10
}

// DefaultProjectHealthConfig returns the default configuration.
func DefaultProjectHealthConfig() ProjectHealthConfig {
	return ProjectHealthConfig{
		ModelFanoutThreshold:        3,
		TooManyJoinsThreshold:       7,
		PassthroughColumnThreshold:  20,
		StarlarkComplexityThreshold: 10,
	}
}
