package starlark

import (
	"testing"

	"github.com/leapstack-labs/leapsql/internal/macro"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

func TestNewExecutionContext(t *testing.T) {
	config := starlark.NewDict(1)
	config.SetKey(starlark.String("name"), starlark.String("test_model"))

	target := &TargetInfo{
		Type:     "duckdb",
		Schema:   "main",
		Database: "test.db",
	}

	this := &ThisInfo{
		Name:   "test_model",
		Schema: "analytics",
	}

	ctx := NewExecutionContext(config, "dev", target, this)

	require.NotNil(t, ctx, "NewExecutionContext returned nil")

	globals := ctx.Globals()

	// Check all expected globals are present
	expectedKeys := []string{"config", "env", "target", "this"}
	for _, key := range expectedKeys {
		_, ok := globals[key]
		assert.True(t, ok, "global %q not found", key)
	}
}

func TestExecutionContext_EvalExpr(t *testing.T) {
	config := starlark.NewDict(2)
	config.SetKey(starlark.String("name"), starlark.String("my_model"))
	config.SetKey(starlark.String("materialized"), starlark.String("table"))

	ctx := NewExecutionContext(config, "prod", nil, nil)

	tests := []struct {
		name    string
		expr    string
		want    string
		wantErr bool
	}{
		{
			name: "simple string",
			expr: `"hello"`,
			want: "hello",
		},
		{
			name: "env variable",
			expr: `env`,
			want: "prod",
		},
		{
			name: "config access",
			expr: `config["name"]`,
			want: "my_model",
		},
		{
			name: "string concatenation",
			expr: `"prefix_" + config["name"]`,
			want: "prefix_my_model",
		},
		{
			name: "conditional expression",
			expr: `"production" if env == "prod" else "development"`,
			want: "production",
		},
		{
			name: "arithmetic",
			expr: `str(1 + 2)`,
			want: "3",
		},
		{
			name:    "undefined variable",
			expr:    `undefined_var`,
			wantErr: true,
		},
		{
			name:    "syntax error",
			expr:    `if`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ctx.EvalExprString(tt.expr, "test.sql", 1)

			if tt.wantErr {
				assert.Error(t, err, "expected error")
				return
			}

			require.NoError(t, err, "unexpected error")
			assert.Equal(t, tt.want, result, "EvalExprString()")
		})
	}
}

func TestExecutionContext_EvalExpr_WithTarget(t *testing.T) {
	config := starlark.NewDict(0)
	target := &TargetInfo{
		Type:     "duckdb",
		Schema:   "analytics",
		Database: "mydb",
	}

	ctx := NewExecutionContext(config, "dev", target, nil)

	// Test target.schema access
	result, err := ctx.EvalExprString(`target.schema`, "test.sql", 1)
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, "analytics", result, "target.schema")

	// Test target.type access
	result, err = ctx.EvalExprString(`target.type`, "test.sql", 1)
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, "duckdb", result, "target.type")
}

func TestExecutionContext_EvalExpr_WithThis(t *testing.T) {
	config := starlark.NewDict(0)
	this := &ThisInfo{
		Name:   "orders",
		Schema: "staging",
	}

	ctx := NewExecutionContext(config, "dev", nil, this)

	// Test this.name access
	result, err := ctx.EvalExprString(`this.name`, "test.sql", 1)
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, "orders", result, "this.name")

	// Test this.schema access
	result, err = ctx.EvalExprString(`this.schema`, "test.sql", 1)
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, "staging", result, "this.schema")
}

func TestExecutionContext_AddMacros(t *testing.T) {
	config := starlark.NewDict(0)
	ctx := NewExecutionContext(config, "dev", nil, nil)

	// Create a simple macro namespace
	macros := starlark.StringDict{
		"utils": starlark.String("mock_utils_module"),
	}

	err := ctx.AddMacros(macros)
	require.NoError(t, err, "AddMacros() error")

	globals := ctx.Globals()
	_, ok := globals["utils"]
	assert.True(t, ok, "utils macro not found in globals")
}

func TestExecutionContext_AddMacros_ConflictWithBuiltin(t *testing.T) {
	config := starlark.NewDict(0)
	ctx := NewExecutionContext(config, "dev", nil, nil)

	tests := []struct {
		name      string
		macroName string
	}{
		{"config conflict", "config"},
		{"env conflict", "env"},
		{"target conflict", "target"},
		{"this conflict", "this"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			macros := starlark.StringDict{
				tt.macroName: starlark.String("conflict"),
			}

			err := ctx.AddMacros(macros)
			assert.Error(t, err, "expected error for conflicting macro name %q", tt.macroName)
		})
	}
}

func TestEvalError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  EvalError
		want string
	}{
		{
			name: "with line",
			err: EvalError{
				File:    "model.sql",
				Line:    10,
				Expr:    "undefined",
				Message: "undefined variable",
			},
			want: `model.sql:10: error evaluating "undefined": undefined variable`,
		},
		{
			name: "without line",
			err: EvalError{
				File:    "model.sql",
				Expr:    "bad",
				Message: "syntax error",
			},
			want: `model.sql: error evaluating "bad": syntax error`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err.Error(), "Error()")
		})
	}
}

func TestNewContext_WithOptions(t *testing.T) {
	config := starlark.NewDict(0)
	macros := starlark.StringDict{
		"datetime": starlark.String("datetime_module"),
	}

	ctx := NewContext(config, "prod", nil, nil, WithMacros(macros))

	globals := ctx.Globals()
	_, ok := globals["datetime"]
	assert.True(t, ok, "datetime macro not found in globals")
}

func TestNewContext_WithMacroRegistry(t *testing.T) {
	config := starlark.NewDict(0)

	// Create a registry with a module
	registry := macro.NewRegistry()
	module := &macro.LoadedModule{
		Namespace: "utils",
		Path:      "/test/utils.star",
		Exports: starlark.StringDict{
			"greet": starlark.String("greet_func"),
		},
	}
	err := registry.Register(module)
	require.NoError(t, err, "failed to register module")

	ctx := NewContext(config, "prod", nil, nil, WithMacroRegistry(registry))

	globals := ctx.Globals()
	utilsVal, ok := globals["utils"]
	require.True(t, ok, "utils macro not found in globals")

	// Check it's a module with attribute access
	mod, ok := utilsVal.(starlark.HasAttrs)
	require.True(t, ok, "expected HasAttrs, got %T", utilsVal)

	greet, err := mod.Attr("greet")
	require.NoError(t, err, "failed to get greet attr")
	assert.Equal(t, `"greet_func"`, greet.String(), "greet value")
}

func TestNewContext_WithMacroRegistry_Nil(t *testing.T) {
	config := starlark.NewDict(0)

	// Nil registry should not cause panic
	ctx := NewContext(config, "prod", nil, nil, WithMacroRegistry(nil))

	globals := ctx.Globals()
	// Should have standard globals
	_, ok := globals["config"]
	assert.True(t, ok, "config not found")
	_, ok = globals["env"]
	assert.True(t, ok, "env not found")
}
