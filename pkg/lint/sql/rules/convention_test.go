package rules_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	duckdbdialect "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb/dialect"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules" // register rules
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func TestCV02_PreferCoalesce(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "IFNULL function",
			sql:      "SELECT IFNULL(name, 'unknown') FROM users",
			wantDiag: true,
		},
		{
			name:     "NVL function",
			sql:      "SELECT NVL(name, 'unknown') FROM users",
			wantDiag: true,
		},
		{
			name:     "COALESCE function",
			sql:      "SELECT COALESCE(name, 'unknown') FROM users",
			wantDiag: false,
		},
		{
			name:     "no null handling function",
			sql:      "SELECT name FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "CV02")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected CV02 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected CV02 diagnostic")
			}
		})
	}
}

func TestCV04_CountStyle(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "COUNT(1)",
			sql:      "SELECT COUNT(1) FROM users",
			wantDiag: true,
		},
		{
			name:     "COUNT(*)",
			sql:      "SELECT COUNT(*) FROM users",
			wantDiag: false,
		},
		{
			name:     "COUNT(column)",
			sql:      "SELECT COUNT(id) FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "CV04")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected CV04 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected CV04 diagnostic")
			}
		})
	}
}

func TestCV05_IsNullComparison(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "= NULL",
			sql:      "SELECT * FROM users WHERE name = NULL",
			wantDiag: true,
		},
		{
			name:     "!= NULL",
			sql:      "SELECT * FROM users WHERE name != NULL",
			wantDiag: true,
		},
		{
			name:     "IS NULL",
			sql:      "SELECT * FROM users WHERE name IS NULL",
			wantDiag: false,
		},
		{
			name:     "IS NOT NULL",
			sql:      "SELECT * FROM users WHERE name IS NOT NULL",
			wantDiag: false,
		},
		{
			name:     "normal equality",
			sql:      "SELECT * FROM users WHERE name = 'test'",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "CV05")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected CV05 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected CV05 diagnostic")
			}
		})
	}
}

func TestCV08_PreferLeftJoin(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "RIGHT JOIN",
			sql:      "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id",
			wantDiag: true,
		},
		{
			name:     "LEFT JOIN",
			sql:      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
			wantDiag: false,
		},
		{
			name:     "INNER JOIN",
			sql:      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "CV08")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected CV08 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected CV08 diagnostic")
			}
		})
	}
}

func TestCV09_BlockedWords(t *testing.T) {
	// Note: CV09 currently only checks function names, not statement types
	// This is a limitation since we only parse SELECT statements
	tests := []struct {
		name     string
		sql      string
		config   map[string]any
		wantDiag bool
	}{
		{
			name:     "no blocked words in SELECT",
			sql:      "SELECT * FROM users",
			wantDiag: false,
		},
		{
			name:     "custom blocked function",
			sql:      "SELECT DANGEROUS_FUNC(id) FROM users",
			config:   map[string]any{"blocked_words": []string{"DANGEROUS_FUNC"}},
			wantDiag: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbdialect.DuckDB)
			require.NoError(t, err)

			cfg := lint.NewConfig()
			if tt.config != nil {
				cfg = cfg.SetRuleOptions("CV09", tt.config)
			}

			analyzer := lint.NewAnalyzerWithRegistry(cfg, "duckdb")
			diags := analyzer.AnalyzeWithRegistryRules(stmt, duckdbdialect.DuckDB)

			var cv09Diags []lint.Diagnostic
			for _, d := range diags {
				if d.RuleID == "CV09" {
					cv09Diags = append(cv09Diags, d)
				}
			}

			if tt.wantDiag {
				assert.NotEmpty(t, cv09Diags, "expected CV09 diagnostic")
			} else {
				assert.Empty(t, cv09Diags, "unexpected CV09 diagnostic")
			}
		})
	}
}
