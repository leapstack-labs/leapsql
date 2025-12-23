package project

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
)

// Context provides all data needed for project-level analysis.
// It implements lint.ProjectContext to bridge the gap between
// the engine's model data and the lint package's requirements.
type Context struct {
	models   map[string]*ModelInfo // path -> model
	parents  map[string][]string   // model -> upstream models
	children map[string][]string   // model -> downstream models
	config   lint.ProjectHealthConfig
}

// ModelInfo holds all metadata about a model for project-level analysis.
// This is a richer representation than lint.ModelInfo, with computed fields.
type ModelInfo struct {
	Path         string            // e.g., "staging.customers"
	Name         string            // e.g., "stg_customers"
	FilePath     string            // Absolute path to .sql file
	Type         lint.ModelType    // Inferred or explicit model type
	Sources      []string          // Table references (deps)
	Columns      []lint.ColumnInfo // Column-level lineage
	Materialized string            // table, view, incremental
	Tags         []string
	Meta         map[string]any
}

// NewContext creates a new project context for analysis.
func NewContext(models map[string]*ModelInfo, parents, children map[string][]string, config lint.ProjectHealthConfig) *Context {
	return &Context{
		models:   models,
		parents:  parents,
		children: children,
		config:   config,
	}
}

// GetModels implements lint.ProjectContext.
func (c *Context) GetModels() map[string]lint.ModelInfo {
	result := make(map[string]lint.ModelInfo, len(c.models))
	for path, m := range c.models {
		result[path] = lint.ModelInfo{
			Path:         m.Path,
			Name:         m.Name,
			FilePath:     m.FilePath,
			Type:         m.Type,
			Sources:      m.Sources,
			Columns:      m.Columns,
			Materialized: m.Materialized,
			Tags:         m.Tags,
			Meta:         m.Meta,
		}
	}
	return result
}

// GetParents implements lint.ProjectContext.
func (c *Context) GetParents(modelPath string) []string {
	return c.parents[modelPath]
}

// GetChildren implements lint.ProjectContext.
func (c *Context) GetChildren(modelPath string) []string {
	return c.children[modelPath]
}

// GetConfig implements lint.ProjectContext.
func (c *Context) GetConfig() lint.ProjectHealthConfig {
	return c.config
}

// GetModel returns a specific model by path.
func (c *Context) GetModel(path string) (*ModelInfo, bool) {
	m, ok := c.models[path]
	return m, ok
}

// Models returns the internal model map for direct access.
func (c *Context) Models() map[string]*ModelInfo {
	return c.models
}

// IsModel checks if a given table name is a known model.
func (c *Context) IsModel(name string) bool {
	_, ok := c.models[name]
	return ok
}
