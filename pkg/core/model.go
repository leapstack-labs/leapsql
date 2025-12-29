package core

// ModelType represents the semantic type of a model.
type ModelType string

// Model type constants.
const (
	ModelTypeStaging      ModelType = "staging"
	ModelTypeIntermediate ModelType = "intermediate"
	ModelTypeMarts        ModelType = "marts"
	ModelTypeOther        ModelType = "other"
)

// TransformType describes how source columns are transformed.
type TransformType string

const (
	// TransformDirect means the column is a direct copy (no transformation).
	TransformDirect TransformType = ""
	// TransformExpression means the column is derived from an expression.
	TransformExpression TransformType = "EXPR"
)

// Model represents a SQL model (transformation unit).
// This contains the core identity fields only.
// Persistence-specific fields (ID, ContentHash, timestamps) belong in state.PersistedModel.
type Model struct {
	// Path is the model path (e.g., "staging.customers")
	Path string
	// Name is the model name (filename without extension)
	Name string
	// FilePath is the absolute path to the SQL file
	FilePath string
	// Materialized defines how the model is stored: table, view, incremental
	Materialized string
	// UniqueKey for incremental models
	UniqueKey string
	// Owner is the team/person responsible for this model
	Owner string
	// Schema is the database schema for this model
	Schema string
	// Description is a human-readable description of the model
	Description string
	// Tags are metadata labels for filtering/organizing models
	Tags []string
	// Meta contains custom extension fields
	Meta map[string]any
	// Tests contains test configurations
	Tests []TestConfig
	// Imports are explicit model dependencies from @import pragmas (legacy)
	Imports []string
	// Sources are all table names referenced in the SQL
	Sources []string
	// Columns contains column-level lineage information
	Columns []ColumnInfo
	// UsesSelectStar is true if model uses SELECT * or t.*
	UsesSelectStar bool
	// SQL is the raw SQL content (excluding frontmatter)
	SQL string
	// RawContent is the full file content including frontmatter
	RawContent string
	// Conditionals are #if directives for environment-specific SQL
	Conditionals []Conditional
	// HasFrontmatter indicates if YAML frontmatter was found
	HasFrontmatter bool
}

// TestConfig represents test configuration for a model.
type TestConfig struct {
	Unique         []string
	NotNull        []string
	AcceptedValues *AcceptedValuesConfig
}

// AcceptedValuesConfig represents accepted values test configuration.
type AcceptedValuesConfig struct {
	Column string
	Values []string
}

// SourceRef represents a source column reference in lineage.
type SourceRef struct {
	Table  string
	Column string
}

// ColumnInfo represents column lineage information.
type ColumnInfo struct {
	Name          string
	Index         int
	TransformType TransformType // "" (direct) or "EXPR"
	Function      string        // "sum", "count", etc.
	Sources       []SourceRef   // where this column comes from
}

// Conditional represents an #if directive block.
type Conditional struct {
	Condition string
	Content   string
}
