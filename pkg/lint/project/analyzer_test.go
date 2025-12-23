package project

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzer_Analyze_Empty(t *testing.T) {
	analyzer := NewAnalyzer(nil)
	diags := analyzer.Analyze(nil)
	assert.Nil(t, diags)
}

func TestAnalyzer_Analyze_NoRules(t *testing.T) {
	Clear() // Clear all registered rules

	models := map[string]*ModelInfo{
		"staging.customers": {
			Path:     "staging.customers",
			Name:     "stg_customers",
			FilePath: "/models/staging/stg_customers.sql",
			Type:     lint.ModelTypeStaging,
		},
	}

	ctx := NewContext(models, nil, nil, lint.DefaultProjectHealthConfig())
	analyzer := NewAnalyzer(nil)
	diags := analyzer.Analyze(ctx)

	assert.Empty(t, diags)
}

func TestAnalyzer_DisableRule(t *testing.T) {
	Clear()

	// Register a test rule
	Register(RuleDef{
		ID:       "TEST01",
		Name:     "test-rule",
		Group:    "test",
		Severity: lint.SeverityWarning,
		Check: func(_ *Context) []Diagnostic {
			return []Diagnostic{{
				RuleID:   "TEST01",
				Severity: lint.SeverityWarning,
				Message:  "test message",
			}}
		},
	})

	models := map[string]*ModelInfo{
		"staging.customers": {
			Path:     "staging.customers",
			Name:     "stg_customers",
			FilePath: "/models/staging/stg_customers.sql",
		},
	}

	ctx := NewContext(models, nil, nil, lint.DefaultProjectHealthConfig())

	// Test without disabling
	analyzer := NewAnalyzer(nil)
	diags := analyzer.Analyze(ctx)
	require.Len(t, diags, 1)

	// Test with rule disabled
	cfg := NewAnalyzerConfig()
	cfg.DisabledRules["TEST01"] = true
	analyzer = NewAnalyzer(cfg)
	diags = analyzer.Analyze(ctx)
	assert.Empty(t, diags)
}

func TestAnalyzer_SeverityOverride(t *testing.T) {
	Clear()

	// Register a test rule with Warning severity
	Register(RuleDef{
		ID:       "TEST02",
		Name:     "test-rule-2",
		Group:    "test",
		Severity: lint.SeverityWarning,
		Check: func(_ *Context) []Diagnostic {
			return []Diagnostic{{
				RuleID:   "TEST02",
				Severity: lint.SeverityWarning,
				Message:  "test message",
			}}
		},
	})

	models := map[string]*ModelInfo{
		"staging.customers": {
			Path: "staging.customers",
		},
	}

	ctx := NewContext(models, nil, nil, lint.DefaultProjectHealthConfig())

	// Override severity to Error
	cfg := NewAnalyzerConfig()
	cfg.SeverityOverrides["TEST02"] = lint.SeverityError
	analyzer := NewAnalyzer(cfg)
	diags := analyzer.Analyze(ctx)

	require.Len(t, diags, 1)
	assert.Equal(t, lint.SeverityError, diags[0].Severity)
}
