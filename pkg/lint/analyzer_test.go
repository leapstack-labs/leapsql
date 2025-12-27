package lint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/leapstack-labs/leapsql/pkg/core"
	duckdbdialect "github.com/leapstack-labs/leapsql/pkg/dialects/duckdb"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

// registerTestRule registers a test rule and returns a cleanup function.
func registerTestRule(t *testing.T, id string, checkFunc sql.CheckFunc) {
	t.Helper()
	sql.Register(sql.RuleDef{
		ID:       id,
		Name:     "test-" + id,
		Group:    "test",
		Severity: core.SeverityWarning,
		Check:    checkFunc,
	})
	t.Cleanup(func() {
		lint.Clear()
	})
}

func TestAnalyzer_WithRegisteredRule(t *testing.T) {
	// Register a simple test rule that triggers on DISTINCT
	registerTestRule(t, "TEST01", func(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
		selectStmt, ok := stmt.(*parser.SelectStmt)
		if !ok || selectStmt == nil || selectStmt.Body == nil || selectStmt.Body.Left == nil {
			return nil
		}
		if selectStmt.Body.Left.Distinct {
			return []lint.Diagnostic{{
				RuleID:   "TEST01",
				Severity: core.SeverityWarning,
				Message:  "Found DISTINCT",
			}}
		}
		return nil
	})

	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "distinct in select",
			sql:      "SELECT DISTINCT a FROM t",
			wantDiag: true,
		},
		{
			name:     "no distinct",
			sql:      "SELECT a FROM t",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbdialect.DuckDB)
			require.NoError(t, err)

			analyzer := lint.NewAnalyzer(lint.NewConfig())
			diags := analyzer.Analyze(stmt, duckdbdialect.DuckDB)

			hasDiag := false
			for _, d := range diags {
				if d.RuleID == "TEST01" {
					hasDiag = true
					break
				}
			}

			if tt.wantDiag {
				assert.True(t, hasDiag, "expected TEST01 diagnostic for %q", tt.sql)
			} else {
				assert.False(t, hasDiag, "unexpected TEST01 diagnostic for %q", tt.sql)
			}
		})
	}
}

func TestAnalyzer_DisableRule(t *testing.T) {
	// Register a test rule
	registerTestRule(t, "DISABLE01", func(_ any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
		return []lint.Diagnostic{{
			RuleID:   "DISABLE01",
			Severity: core.SeverityWarning,
			Message:  "Always triggers",
		}}
	})

	cfg := lint.NewConfig().Disable("DISABLE01")

	sql := "SELECT a FROM t"
	stmt, err := parser.ParseWithDialect(sql, duckdbdialect.DuckDB)
	require.NoError(t, err)

	analyzer := lint.NewAnalyzer(cfg)
	diags := analyzer.Analyze(stmt, duckdbdialect.DuckDB)

	// The rule should be disabled
	for _, d := range diags {
		assert.NotEqual(t, "DISABLE01", d.RuleID, "disabled rule should not produce diagnostics")
	}
}

func TestAnalyzer_SeverityOverride(t *testing.T) {
	// Register a test rule with default warning severity
	registerTestRule(t, "SEV01", func(_ any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
		return []lint.Diagnostic{{
			RuleID:   "SEV01",
			Severity: core.SeverityWarning,
			Message:  "Test severity",
		}}
	})

	cfg := lint.NewConfig().SetSeverity("SEV01", core.SeverityError)

	sql := "SELECT a FROM t"
	stmt, err := parser.ParseWithDialect(sql, duckdbdialect.DuckDB)
	require.NoError(t, err)

	analyzer := lint.NewAnalyzer(cfg)
	diags := analyzer.Analyze(stmt, duckdbdialect.DuckDB)

	require.NotEmpty(t, diags)
	for _, d := range diags {
		if d.RuleID == "SEV01" {
			assert.Equal(t, core.SeverityError, d.Severity, "severity should be overridden to error")
		}
	}
}

func TestSeverity_String(t *testing.T) {
	tests := []struct {
		sev  core.Severity
		want string
	}{
		{core.SeverityError, "error"},
		{core.SeverityWarning, "warning"},
		{core.SeverityInfo, "info"},
		{core.SeverityHint, "hint"},
		{core.Severity(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.sev.String())
		})
	}
}

func TestAnalyzer_NilStatement(t *testing.T) {
	analyzer := lint.NewAnalyzer(lint.NewConfig())
	diags := analyzer.Analyze(nil, duckdbdialect.DuckDB)
	assert.Nil(t, diags)
}

func TestAnalyzer_NilConfig(t *testing.T) {
	// Register a test rule
	registerTestRule(t, "NIL01", func(_ any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
		return []lint.Diagnostic{{
			RuleID:   "NIL01",
			Severity: core.SeverityWarning,
			Message:  "Test nil config",
		}}
	})

	// NewAnalyzer should handle nil config
	analyzer := lint.NewAnalyzer(nil)
	require.NotNil(t, analyzer)

	sql := "SELECT a FROM t"
	stmt, err := parser.ParseWithDialect(sql, duckdbdialect.DuckDB)
	require.NoError(t, err)

	// Should still work with default config
	diags := analyzer.Analyze(stmt, duckdbdialect.DuckDB)
	assert.NotEmpty(t, diags)
}

func TestAnalyzer_DialectFilter(t *testing.T) {
	// Clear and register test rules with dialect restrictions
	lint.Clear()

	sql.Register(sql.RuleDef{
		ID:       "PG01",
		Name:     "postgres-only",
		Group:    "test",
		Severity: core.SeverityWarning,
		Dialects: []string{"postgres"},
		Check: func(_ any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
			return []lint.Diagnostic{{RuleID: "PG01", Message: "postgres rule"}}
		},
	})

	sql.Register(sql.RuleDef{
		ID:       "UNI01",
		Name:     "universal",
		Group:    "test",
		Severity: core.SeverityWarning,
		Dialects: nil, // All dialects
		Check: func(_ any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
			return []lint.Diagnostic{{RuleID: "UNI01", Message: "universal rule"}}
		},
	})

	t.Cleanup(func() {
		lint.Clear()
	})

	sqlStr := "SELECT a FROM t"
	stmt, err := parser.ParseWithDialect(sqlStr, duckdbdialect.DuckDB)
	require.NoError(t, err)

	// ANSI dialect should only see UNI01, not PG01
	analyzer := lint.NewAnalyzer(lint.NewConfig())
	diags := analyzer.Analyze(stmt, duckdbdialect.DuckDB)

	var ids []string
	for _, d := range diags {
		ids = append(ids, d.RuleID)
	}

	assert.Contains(t, ids, "UNI01", "universal rule should trigger")
	assert.NotContains(t, ids, "PG01", "postgres-only rule should not trigger for ANSI")
}
