package template

import (
	"testing"

	starctx "github.com/leapstack-labs/leapsql/internal/starlark"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

func newTestContext() *starctx.ExecutionContext {
	config := starlark.NewDict(1)
	config.SetKey(starlark.String("materialized"), starlark.String("table"))

	target := &starctx.TargetInfo{
		Type:     "duckdb",
		Schema:   "analytics",
		Database: "test_db",
	}

	this := &starctx.ThisInfo{
		Name:   "test_model",
		Schema: "public",
	}

	return starctx.NewExecutionContext(config, "dev", target, this)
}

func TestRenderer_Expressions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain text", "SELECT * FROM users", "SELECT * FROM users"},
		{"simple expression", `SELECT * FROM {{ target.schema }}.users`, "SELECT * FROM analytics.users"},
		{"multiple expressions", `{{ target.schema }}.{{ this.name }}`, "analytics.test_model"},
		{"env variable", `{{ env }}`, "dev"},
		{"config access", `{{ config["materialized"] }}`, "table"},
		{"string concatenation", `{{ target.schema + "." + this.name }}`, "analytics.test_model"},
		{"integer expression", `{{ 1 + 2 }}`, "3"},
		{"boolean expression", `{{ True }}`, "True"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newTestContext()
			result, err := RenderString(tt.input, "test.sql", ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRenderer_ForLoop(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    string
		containsAll []string // for cases where exact match is hard due to whitespace
	}{
		{
			name:     "inline loop",
			input:    `{* for x in [1, 2, 3]: *}{{ x }}{* endfor *}`,
			expected: "123",
		},
		{
			name:     "empty loop",
			input:    `before{* for x in []: *}{{ x }}{* endfor *}after`,
			expected: "beforeafter",
		},
		{
			name: "loop with list",
			input: `SELECT
{* for col in ["id", "name", "email"]: *}
    {{ col }},
{* endfor *}
FROM users`,
			containsAll: []string{"id", "name", "email"},
		},
		{
			name: "nested loop",
			input: `{* for i in [0, 1, 2]: *}
{* for j in [0, 1]: *}
({{ i }}, {{ j }})
{* endfor *}
{* endfor *}`,
			containsAll: []string{"(0, 0)", "(0, 1)", "(1, 0)", "(1, 1)", "(2, 0)", "(2, 1)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newTestContext()
			result, err := RenderString(tt.input, "test.sql", ctx)
			require.NoError(t, err)

			if tt.expected != "" {
				assert.Equal(t, tt.expected, result)
			}

			for _, s := range tt.containsAll {
				assert.Contains(t, result, s)
			}
		})
	}
}

func TestRenderer_IfStatement(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"if true", `{* if env == "dev": *}DEV{* endif *}`, "DEV"},
		{"if false", `{* if env == "prod": *}PROD{* endif *}`, ""},
		{"if-else true branch", `{* if env == "dev": *}DEV{* else: *}NOT_DEV{* endif *}`, "DEV"},
		{"if-else false branch", `{* if env == "prod": *}PROD{* else: *}NOT_PROD{* endif *}`, "NOT_PROD"},
		{"if-elif-else", `{* if env == "prod": *}PROD{* elif env == "dev": *}DEV{* else: *}OTHER{* endif *}`, "DEV"},
		{"nested for-if", `{* for x in [1, 2, 3]: *}{* if x > 1: *}{{ x }}{* endif *}{* endfor *}`, "23"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newTestContext()
			result, err := RenderString(tt.input, "test.sql", ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRenderer_TruthyFalsy(t *testing.T) {
	tests := []struct {
		name      string
		condition string
		expected  string
	}{
		{"True", `True`, "yes"},
		{"False", `False`, "no"},
		{"1", `1`, "yes"},
		{"0", `0`, "no"},
		{"empty string", `""`, "no"},
		{"non-empty string", `"hello"`, "yes"},
		{"empty list", `[]`, "no"},
		{"non-empty list", `[1]`, "yes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := `{* if ` + tt.condition + `: *}yes{* else: *}no{* endif *}`
			ctx := newTestContext()

			result, err := RenderString(input, "test.sql", ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRenderer_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"undefined variable", `{{ undefined_variable }}`},
		{"undefined iterator", `{* for x in undefined: *}{{ x }}{* endfor *}`},
		{"undefined condition", `{* if undefined: *}yes{* endif *}`},
		{"non-iterable for", `{* for x in 42: *}{{ x }}{* endfor *}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newTestContext()
			_, err := RenderString(tt.input, "test.sql", ctx)
			assert.Error(t, err)
		})
	}
}

func TestRenderer_FullExample(t *testing.T) {
	input := `SELECT
{* for col in ["id", "name", "created_at"]: *}
    {{ col }},
{* endfor *}
{* if env == "prod": *}
    updated_at
{* else: *}
    *
{* endif *}
FROM {{ target.schema }}.users`

	ctx := newTestContext()

	result, err := RenderString(input, "test.sql", ctx)
	require.NoError(t, err)

	// Should contain all column names
	for _, col := range []string{"id", "name", "created_at"} {
		assert.Contains(t, result, col)
	}

	// Should contain * since env is "dev"
	assert.Contains(t, result, "*")

	// Should not contain "updated_at" since env is "dev"
	assert.NotContains(t, result, "updated_at")

	// Should have correct table reference
	assert.Contains(t, result, "analytics.users")
}
