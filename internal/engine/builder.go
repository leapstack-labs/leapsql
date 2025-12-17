package engine

// builder.go - SQL template rendering and building

import (
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/parser"
	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/leapstack-labs/leapsql/internal/template"
)

// RenderModel renders the SQL for a model with all templates expanded.
// This is the public API for SQL rendering.
func (e *Engine) RenderModel(modelPath string) (string, error) {
	m, ok := e.models[modelPath]
	if !ok {
		return "", fmt.Errorf("model not found: %s", modelPath)
	}

	model, err := e.store.GetModelByPath(modelPath)
	if err != nil {
		// Model not in state store, create minimal version
		model = &state.Model{Path: modelPath, Name: m.Name}
	}

	return e.buildSQL(m, model), nil
}

// buildSQL prepares the SQL for execution using template rendering.
func (e *Engine) buildSQL(m *parser.ModelConfig, model *state.Model) string {
	// Create execution context for this model
	ctx := e.createExecutionContext(m)

	// Render the template
	rendered, err := template.RenderString(m.SQL, m.FilePath, ctx)
	if err != nil {
		// Fallback to legacy string replacement if template fails
		// This provides backward compatibility
		return e.buildSQLLegacy(m, model)
	}

	return rendered
}

// buildSQLLegacy provides backward compatibility with simple string replacement.
func (e *Engine) buildSQLLegacy(m *parser.ModelConfig, _ *state.Model) string {
	sql := m.SQL

	// Replace {{ this }} with the model's table name
	tableName := pathToTableName(m.Path)
	sql = strings.ReplaceAll(sql, "{{ this }}", tableName)

	return sql
}

// createExecutionContext builds a Starlark execution context for template rendering.
func (e *Engine) createExecutionContext(m *parser.ModelConfig) *starctx.ExecutionContext {
	// Build config dict from model config
	config := starctx.BuildConfigDict(
		m.Name,
		m.Materialized,
		m.UniqueKey,
		m.Owner,
		m.Schema,
		m.Tags,
		m.Meta,
	)

	// Build this info
	thisInfo := &starctx.ThisInfo{
		Name:   m.Name,
		Schema: e.getModelSchema(m),
	}

	// Create context with macros
	ctx := starctx.NewContext(
		config,
		e.environment,
		e.target,
		thisInfo,
		starctx.WithMacroRegistry(e.macroRegistry),
	)

	return ctx
}

// getModelSchema extracts the schema from a model path.
func (e *Engine) getModelSchema(m *parser.ModelConfig) string {
	// If schema is explicitly set, use it
	if m.Schema != "" {
		return m.Schema
	}
	// Otherwise derive from path (e.g., "staging.customers" -> "staging")
	parts := strings.Split(m.Path, ".")
	if len(parts) > 1 {
		return parts[0]
	}
	return e.target.Schema // Default to target schema
}
