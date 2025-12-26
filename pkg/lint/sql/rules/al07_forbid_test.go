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

func TestAL07_ForbidAlias(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
		message  string
	}{
		{
			name:     "single letter table alias",
			sql:      "SELECT * FROM users t",
			wantDiag: true,
			message:  "Table alias 't' matches forbidden pattern",
		},
		{
			name:     "single letter column alias",
			sql:      "SELECT name AS n FROM users",
			wantDiag: true,
			message:  "Column alias 'n' matches forbidden pattern",
		},
		{
			name:     "numbered table alias t1",
			sql:      "SELECT * FROM users t1",
			wantDiag: true,
			message:  "Table alias 't1' matches forbidden pattern",
		},
		{
			name:     "numbered table alias t2",
			sql:      "SELECT * FROM users t2 JOIN orders t3 ON t2.id = t3.user_id",
			wantDiag: true,
			message:  "matches forbidden pattern",
		},
		{
			name:     "tbl prefix alias",
			sql:      "SELECT * FROM users tbl",
			wantDiag: true,
			message:  "Table alias 'tbl' matches forbidden pattern",
		},
		{
			name:     "tbl1 prefix alias",
			sql:      "SELECT * FROM users tbl1",
			wantDiag: true,
			message:  "Table alias 'tbl1' matches forbidden pattern",
		},
		{
			name:     "descriptive table alias",
			sql:      "SELECT * FROM users usr",
			wantDiag: false,
		},
		{
			name:     "descriptive column alias",
			sql:      "SELECT first_name AS fname FROM users",
			wantDiag: false,
		},
		{
			name:     "full name as alias",
			sql:      "SELECT * FROM users users_table",
			wantDiag: false,
		},
		{
			name:     "no alias",
			sql:      "SELECT * FROM users",
			wantDiag: false,
		},
		{
			name:     "two character alias ok",
			sql:      "SELECT * FROM users us",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, ansi.ANSI)
			require.NoError(t, err)

			analyzer := lint.NewAnalyzerWithRegistry(lint.NewConfig(), "ansi")
			diags := analyzer.AnalyzeWithRegistryRules(stmt, ansi.ANSI)

			var al07Diags []lint.Diagnostic
			for _, d := range diags {
				if d.RuleID == "AL07" {
					al07Diags = append(al07Diags, d)
				}
			}

			if tt.wantDiag {
				require.NotEmpty(t, al07Diags, "expected AL07 diagnostic for %q", tt.sql)
				assert.Contains(t, al07Diags[0].Message, tt.message)
			} else {
				assert.Empty(t, al07Diags, "unexpected AL07 diagnostic for %q", tt.sql)
			}
		})
	}
}

func TestAL07_CustomConfig(t *testing.T) {
	t.Run("custom forbidden names", func(t *testing.T) {
		sql := "SELECT * FROM users temp"
		stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
		require.NoError(t, err)

		cfg := lint.NewConfig().
			SetRuleOptions("AL07", map[string]any{
				"forbidden_names": []string{"temp", "tmp"},
			})
		analyzer := lint.NewAnalyzerWithRegistry(cfg, "ansi")
		diags := analyzer.AnalyzeWithRegistryRules(stmt, ansi.ANSI)

		var al07Diags []lint.Diagnostic
		for _, d := range diags {
			if d.RuleID == "AL07" {
				al07Diags = append(al07Diags, d)
			}
		}

		require.NotEmpty(t, al07Diags)
		assert.Contains(t, al07Diags[0].Message, "forbidden")
	})

	t.Run("custom forbidden patterns", func(t *testing.T) {
		sql := "SELECT * FROM users xxx"
		stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
		require.NoError(t, err)

		cfg := lint.NewConfig().
			SetRuleOptions("AL07", map[string]any{
				"forbidden_patterns": []string{`^x+$`}, // disallow xxx, xxxx, etc.
			})
		analyzer := lint.NewAnalyzerWithRegistry(cfg, "ansi")
		diags := analyzer.AnalyzeWithRegistryRules(stmt, ansi.ANSI)

		var al07Diags []lint.Diagnostic
		for _, d := range diags {
			if d.RuleID == "AL07" {
				al07Diags = append(al07Diags, d)
			}
		}

		require.NotEmpty(t, al07Diags)
		assert.Contains(t, al07Diags[0].Message, "forbidden pattern")
	})
}
