package engine

// builder.go - SQL template rendering and building

import (
	"fmt"
	"strings"
	"time"

	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
	"github.com/leapstack-labs/leapsql/internal/template"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// RenderResult contains rendered SQL and timing information.
type RenderResult struct {
	SQL      string
	RenderMS int64
	Error    error
}

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
		model = &core.PersistedModel{
			Model: &core.Model{Path: modelPath, Name: m.Name},
		}
	}

	return e.buildSQL(m, model)
}

// RenderModelTimed renders a model and returns timing information.
func (e *Engine) RenderModelTimed(modelPath string) RenderResult {
	start := time.Now()

	sql, err := e.RenderModel(modelPath)

	return RenderResult{
		SQL:      sql,
		RenderMS: time.Since(start).Milliseconds(),
		Error:    err,
	}
}

// buildSQL prepares the SQL for execution using template rendering.
// Returns an error if template rendering fails - no silent fallback.
func (e *Engine) buildSQL(m *core.Model, model *core.PersistedModel) (string, error) {
	// Create execution context for this model
	ctx := e.createExecutionContext(m)

	// Render the template
	rendered, err := template.RenderString(m.SQL, m.FilePath, ctx)
	if err != nil {
		e.logger.Error("template render failed",
			"model", m.Path,
			"file", m.FilePath,
			"error", err)
		return "", fmt.Errorf("render %s: %w", m.Path, err)
	}

	return rendered, nil
}

// createExecutionContext builds a Starlark execution context for template rendering.
func (e *Engine) createExecutionContext(m *core.Model) *starctx.ExecutionContext {
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
func (e *Engine) getModelSchema(m *core.Model) string {
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
