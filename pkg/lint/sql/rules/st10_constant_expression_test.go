package rules_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/leapstack-labs/leapsql/pkg/dialects/ansi"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules" // register rules
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func TestST10_ConstantExpression(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
		message  string
	}{
		{
			name:     "WHERE 1=1",
			sql:      "SELECT * FROM users WHERE 1=1",
			wantDiag: true,
			message:  "always true",
		},
		{
			name:     "WHERE 'a'='a'",
			sql:      "SELECT * FROM users WHERE 'a'='a'",
			wantDiag: true,
			message:  "always true",
		},
		{
			name:     "WHERE true",
			sql:      "SELECT * FROM users WHERE true",
			wantDiag: true,
			message:  "always true",
		},
		{
			name:     "WHERE false",
			sql:      "SELECT * FROM users WHERE false",
			wantDiag: true,
			message:  "always false",
		},
		{
			name:     "WHERE 1",
			sql:      "SELECT * FROM users WHERE 1",
			wantDiag: true,
			message:  "always true",
		},
		{
			name:     "WHERE 0",
			sql:      "SELECT * FROM users WHERE 0",
			wantDiag: true,
			message:  "always false",
		},
		{
			name:     "AND 1=1",
			sql:      "SELECT * FROM users WHERE id > 5 AND 1=1",
			wantDiag: true,
			message:  "always true",
		},
		{
			name:     "OR 1=1",
			sql:      "SELECT * FROM users WHERE id > 5 OR 1=1",
			wantDiag: true,
			message:  "always true",
		},
		{
			name:     "parenthesized constant",
			sql:      "SELECT * FROM users WHERE (1=1)",
			wantDiag: true,
			message:  "always true",
		},
		{
			name:     "normal WHERE condition",
			sql:      "SELECT * FROM users WHERE id = 1",
			wantDiag: false,
		},
		{
			name:     "variable comparison",
			sql:      "SELECT * FROM users WHERE active = true",
			wantDiag: false, // column = literal is fine
		},
		{
			name:     "different values",
			sql:      "SELECT * FROM users WHERE 1=2",
			wantDiag: false, // different values, not a tautology
		},
		{
			name:     "no WHERE clause",
			sql:      "SELECT * FROM users",
			wantDiag: false,
		},
		{
			name:     "NULL comparison",
			sql:      "SELECT * FROM users WHERE NULL=NULL",
			wantDiag: true, // technically this is false, but still constant
			message:  "always true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, ansi.ANSI)
			require.NoError(t, err)

			analyzer := lint.NewAnalyzerWithRegistry(lint.NewConfig(), "ansi")
			diags := analyzer.AnalyzeWithRegistryRules(stmt, ansi.ANSI)

			var st10Diags []lint.Diagnostic
			for _, d := range diags {
				if d.RuleID == "ST10" {
					st10Diags = append(st10Diags, d)
				}
			}

			if tt.wantDiag {
				require.NotEmpty(t, st10Diags, "expected ST10 diagnostic for %q", tt.sql)
				assert.Contains(t, st10Diags[0].Message, tt.message)
			} else {
				assert.Empty(t, st10Diags, "unexpected ST10 diagnostic for %q", tt.sql)
			}
		})
	}
}
