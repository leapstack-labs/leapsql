package starlark

import (
	"fmt"
	"sync"

	"github.com/leapstack-labs/leapsql/internal/macro"
	"go.starlark.net/starlark"
)

// ExecutionContext provides all globals and state for Starlark template execution.
// Note: No ref() function - dependencies are extracted by lineage parser from SQL AST.
type ExecutionContext struct {
	// Config dict containing parsed YAML frontmatter
	// Accessible as: config["materialized"], config["owner"], etc.
	Config starlark.Value

	// Env is the current environment string
	// Values: "prod", "dev", "staging", etc.
	Env string

	// Target contains adapter/database specifics
	// Accessible as: target.type, target.schema, target.database
	Target *TargetInfo

	// This contains current model info
	// Accessible as: this.name, this.schema
	This *ThisInfo

	// Macros contains loaded macro namespaces
	// Each key is a namespace (e.g., "datetime") with a struct of functions
	Macros starlark.StringDict

	// globals is the combined set of all globals for execution
	globals starlark.StringDict

	// mu protects globals during initialization
	mu sync.RWMutex
}

// NewExecutionContext creates a new execution context with the given parameters.
func NewExecutionContext(config starlark.Value, env string, target *TargetInfo, this *ThisInfo) *ExecutionContext {
	ctx := &ExecutionContext{
		Config: config,
		Env:    env,
		Target: target,
		This:   this,
		Macros: make(starlark.StringDict),
	}
	ctx.buildGlobals()
	return ctx
}

// buildGlobals constructs the combined globals dict.
func (ctx *ExecutionContext) buildGlobals() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	ctx.globals = Predeclared(ctx.Config, ctx.Env, ctx.Target, ctx.This)

	// Add macros
	for name, macro := range ctx.Macros {
		ctx.globals[name] = macro
	}
}

// Globals returns the combined globals dictionary for Starlark execution.
func (ctx *ExecutionContext) Globals() starlark.StringDict {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.globals
}

// AddMacros adds macro namespaces to the context.
// Returns error if a macro name conflicts with a builtin.
func (ctx *ExecutionContext) AddMacros(macros starlark.StringDict) error {
	builtins := map[string]bool{
		"config": true,
		"env":    true,
		"target": true,
		"this":   true,
	}

	for name := range macros {
		if builtins[name] {
			return fmt.Errorf("macro namespace %q conflicts with builtin", name)
		}
	}

	ctx.mu.Lock()
	for name, macro := range macros {
		ctx.Macros[name] = macro
	}
	ctx.mu.Unlock()

	ctx.buildGlobals()
	return nil
}

// EvalExpr evaluates a single Starlark expression and returns the result.
// This is used for {{ expr }} template expressions.
func (ctx *ExecutionContext) EvalExpr(expr string, filename string, line int) (starlark.Value, error) {
	return ctx.EvalExprWithLocals(expr, filename, line, nil)
}

// EvalExprWithLocals evaluates a Starlark expression with additional local variables.
// This is used for expressions inside loops where loop variables need to be in scope.
func (ctx *ExecutionContext) EvalExprWithLocals(expr string, filename string, line int, locals starlark.StringDict) (starlark.Value, error) {
	thread := ctx.newThread(filename)

	// Combine globals with locals (locals take precedence)
	globals := ctx.Globals()
	if len(locals) > 0 {
		combined := make(starlark.StringDict, len(globals)+len(locals))
		for k, v := range globals {
			combined[k] = v
		}
		for k, v := range locals {
			combined[k] = v
		}
		globals = combined
	}

	// Use starlark.Eval for expression evaluation
	result, err := starlark.Eval(thread, filename, expr, globals) //nolint:staticcheck // SA1019: will migrate to EvalOptions later
	if err != nil {
		return nil, &EvalError{
			File:    filename,
			Line:    line,
			Expr:    expr,
			Message: err.Error(),
		}
	}

	return result, nil
}

// EvalExprString evaluates a Starlark expression and returns the string result.
// This is the typical use case for template expressions.
func (ctx *ExecutionContext) EvalExprString(expr string, filename string, line int) (string, error) {
	return ctx.EvalExprStringWithLocals(expr, filename, line, nil)
}

// EvalExprStringWithLocals evaluates a Starlark expression with local variables and returns the string result.
func (ctx *ExecutionContext) EvalExprStringWithLocals(expr string, filename string, line int, locals starlark.StringDict) (string, error) {
	result, err := ctx.EvalExprWithLocals(expr, filename, line, locals)
	if err != nil {
		return "", err
	}

	// Convert result to string
	switch v := result.(type) {
	case starlark.String:
		return string(v), nil
	case starlark.NoneType:
		return "", nil
	default:
		// Use Starlark's string representation for other types
		return result.String(), nil
	}
}

// newThread creates a new Starlark thread for execution.
func (ctx *ExecutionContext) newThread(name string) *starlark.Thread {
	return &starlark.Thread{
		Name: name,
		Print: func(_ *starlark.Thread, _ string) {
			// Template execution should not print - this is a no-op
			// In the future, we could capture prints for debugging
		},
	}
}

// EvalError represents an error during Starlark expression evaluation.
type EvalError struct {
	File    string
	Line    int
	Expr    string
	Message string
}

func (e *EvalError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("%s:%d: error evaluating %q: %s", e.File, e.Line, e.Expr, e.Message)
	}
	return fmt.Sprintf("%s: error evaluating %q: %s", e.File, e.Expr, e.Message)
}

// ContextOption is a functional option for configuring ExecutionContext.
type ContextOption func(*ExecutionContext)

// WithMacros sets the macros for the context.
func WithMacros(macros starlark.StringDict) ContextOption {
	return func(ctx *ExecutionContext) {
		ctx.Macros = macros
	}
}

// WithMacroRegistry sets macros from a macro.Registry.
// This is the preferred way to inject macros loaded from .star files.
func WithMacroRegistry(registry *macro.Registry) ContextOption {
	return func(ctx *ExecutionContext) {
		if registry != nil {
			ctx.Macros = registry.ToStarlarkDict()
		}
	}
}

// NewContext creates a new execution context with functional options.
// This is an alternative constructor that uses the options pattern.
func NewContext(config starlark.Value, env string, target *TargetInfo, this *ThisInfo, opts ...ContextOption) *ExecutionContext {
	ctx := &ExecutionContext{
		Config: config,
		Env:    env,
		Target: target,
		This:   this,
		Macros: make(starlark.StringDict),
	}

	for _, opt := range opts {
		opt(ctx)
	}

	ctx.buildGlobals()
	return ctx
}
