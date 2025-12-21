package lint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/leapstack-labs/leapsql/pkg/dialects/ansi"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func TestAnalyzer_SelectStarWarning(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "select star",
			sql:      "SELECT * FROM t",
			wantDiag: true,
		},
		{
			name:     "explicit columns",
			sql:      "SELECT a, b FROM t",
			wantDiag: false,
		},
		{
			name:     "table star",
			sql:      "SELECT t.* FROM t",
			wantDiag: false, // table.* is not SELECT *
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, ansi.ANSI)
			require.NoError(t, err)

			analyzer := lint.NewAnalyzer(lint.NewConfig())
			diags := analyzer.Analyze(stmt, ansi.ANSI)

			if tt.wantDiag {
				require.NotEmpty(t, diags, "expected diagnostics for %q", tt.sql)
				assert.Equal(t, "ansi/select-star", diags[0].RuleID)
			} else {
				assert.Empty(t, diags, "unexpected diagnostics for %q", tt.sql)
			}
		})
	}
}

func TestAnalyzer_LimitWithoutOrderBy(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "limit without order",
			sql:      "SELECT a FROM t LIMIT 10",
			wantDiag: true,
		},
		{
			name:     "limit with order",
			sql:      "SELECT a FROM t ORDER BY a LIMIT 10",
			wantDiag: false,
		},
		{
			name:     "order without limit",
			sql:      "SELECT a FROM t ORDER BY a",
			wantDiag: false,
		},
		{
			name:     "no limit or order",
			sql:      "SELECT a FROM t",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, ansi.ANSI)
			require.NoError(t, err)

			analyzer := lint.NewAnalyzer(lint.NewConfig())
			diags := analyzer.Analyze(stmt, ansi.ANSI)

			hasLimitWarning := false
			for _, d := range diags {
				if d.RuleID == "ansi/limit-without-order" {
					hasLimitWarning = true
					break
				}
			}

			if tt.wantDiag {
				assert.True(t, hasLimitWarning, "expected limit-without-order warning for %q", tt.sql)
			} else {
				assert.False(t, hasLimitWarning, "unexpected limit-without-order warning for %q", tt.sql)
			}
		})
	}
}

func TestConfig_DisableRule(t *testing.T) {
	cfg := lint.NewConfig().Disable("ansi/select-star")

	sql := "SELECT * FROM t"
	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)

	analyzer := lint.NewAnalyzer(cfg)
	diags := analyzer.Analyze(stmt, ansi.ANSI)

	// The select-star rule should be disabled
	for _, d := range diags {
		assert.NotEqual(t, "ansi/select-star", d.RuleID, "disabled rule should not produce diagnostics")
	}
}

func TestConfig_SeverityOverride(t *testing.T) {
	cfg := lint.NewConfig().SetSeverity("ansi/select-star", lint.SeverityError)

	sql := "SELECT * FROM t"
	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)

	analyzer := lint.NewAnalyzer(cfg)
	diags := analyzer.Analyze(stmt, ansi.ANSI)

	require.NotEmpty(t, diags)
	for _, d := range diags {
		if d.RuleID == "ansi/select-star" {
			assert.Equal(t, lint.SeverityError, d.Severity, "severity should be overridden to error")
		}
	}
}

func TestSeverity_String(t *testing.T) {
	tests := []struct {
		sev  lint.Severity
		want string
	}{
		{lint.SeverityError, "error"},
		{lint.SeverityWarning, "warning"},
		{lint.SeverityInfo, "info"},
		{lint.SeverityHint, "hint"},
		{lint.Severity(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.sev.String())
		})
	}
}

func TestAnalyzer_NilStatement(t *testing.T) {
	analyzer := lint.NewAnalyzer(lint.NewConfig())
	diags := analyzer.Analyze(nil, ansi.ANSI)
	assert.Nil(t, diags)
}

func TestAnalyzer_NilConfig(t *testing.T) {
	// NewAnalyzer should handle nil config
	analyzer := lint.NewAnalyzer(nil)
	require.NotNil(t, analyzer)

	sql := "SELECT * FROM t"
	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)

	// Should still work with default config
	diags := analyzer.Analyze(stmt, ansi.ANSI)
	assert.NotEmpty(t, diags)
}
