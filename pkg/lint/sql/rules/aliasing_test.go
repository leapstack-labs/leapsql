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

// Helper to run analysis and filter by rule ID
func runRule(t *testing.T, sql string, ruleID string) []lint.Diagnostic {
	t.Helper()
	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)

	analyzer := lint.NewAnalyzerWithRegistry(lint.NewConfig(), "ansi")
	diags := analyzer.AnalyzeWithRegistryRules(stmt, ansi.ANSI)

	var filtered []lint.Diagnostic
	for _, d := range diags {
		if d.RuleID == ruleID {
			filtered = append(filtered, d)
		}
	}
	return filtered
}

func TestAL03_ExpressionAlias(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "function without alias",
			sql:      "SELECT COUNT(*) FROM users",
			wantDiag: true,
		},
		{
			name:     "function with alias",
			sql:      "SELECT COUNT(*) AS total FROM users",
			wantDiag: false,
		},
		{
			name:     "CASE without alias",
			sql:      "SELECT CASE WHEN active THEN 1 ELSE 0 END FROM users",
			wantDiag: true,
		},
		{
			name:     "CASE with alias",
			sql:      "SELECT CASE WHEN active THEN 1 ELSE 0 END AS status FROM users",
			wantDiag: false,
		},
		{
			name:     "binary expression without alias",
			sql:      "SELECT price * quantity FROM orders",
			wantDiag: true,
		},
		{
			name:     "binary expression with alias",
			sql:      "SELECT price * quantity AS total FROM orders",
			wantDiag: false,
		},
		{
			name:     "simple column - no alias needed",
			sql:      "SELECT name FROM users",
			wantDiag: false,
		},
		{
			name:     "star - no alias needed",
			sql:      "SELECT * FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AL03")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AL03 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AL03 diagnostic")
			}
		})
	}
}

func TestAL04_UniqueTableAlias(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "duplicate table alias",
			sql:      "SELECT * FROM users usr JOIN orders usr ON usr.id = usr.user_id",
			wantDiag: true,
		},
		{
			name:     "unique table aliases",
			sql:      "SELECT * FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			wantDiag: false,
		},
		{
			name:     "no aliases",
			sql:      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			wantDiag: false,
		},
		{
			name:     "single table with alias",
			sql:      "SELECT * FROM users usr",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AL04")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AL04 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AL04 diagnostic")
			}
		})
	}
}

func TestAL05_UnusedAlias(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "unused table alias",
			sql:      "SELECT name FROM users usr",
			wantDiag: true,
		},
		{
			name:     "used table alias",
			sql:      "SELECT usr.name FROM users usr",
			wantDiag: false,
		},
		{
			name:     "alias used in join condition",
			sql:      "SELECT * FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			wantDiag: false,
		},
		{
			name:     "no alias",
			sql:      "SELECT name FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AL05")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AL05 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AL05 diagnostic")
			}
		})
	}
}

func TestAL06_AliasLength(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		config   map[string]any
		wantDiag bool
	}{
		{
			name:     "default config - normal alias",
			sql:      "SELECT * FROM users usr",
			wantDiag: false,
		},
		{
			name:     "alias too short with custom min",
			sql:      "SELECT * FROM users us",
			config:   map[string]any{"min_length": 3},
			wantDiag: true,
		},
		{
			name:     "alias too long with custom max",
			sql:      "SELECT * FROM users users_table_alias_very_long",
			config:   map[string]any{"max_length": 10},
			wantDiag: true,
		},
		{
			name:     "column alias too short",
			sql:      "SELECT name AS nm FROM users",
			config:   map[string]any{"min_length": 3},
			wantDiag: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, ansi.ANSI)
			require.NoError(t, err)

			cfg := lint.NewConfig()
			if tt.config != nil {
				cfg = cfg.SetRuleOptions("AL06", tt.config)
			}

			analyzer := lint.NewAnalyzerWithRegistry(cfg, "ansi")
			diags := analyzer.AnalyzeWithRegistryRules(stmt, ansi.ANSI)

			var al06Diags []lint.Diagnostic
			for _, d := range diags {
				if d.RuleID == "AL06" {
					al06Diags = append(al06Diags, d)
				}
			}

			if tt.wantDiag {
				assert.NotEmpty(t, al06Diags, "expected AL06 diagnostic")
			} else {
				assert.Empty(t, al06Diags, "unexpected AL06 diagnostic")
			}
		})
	}
}

func TestAL08_UniqueColumnAlias(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "duplicate column alias",
			sql:      "SELECT first_name AS name, last_name AS name FROM users",
			wantDiag: true,
		},
		{
			name:     "unique column aliases",
			sql:      "SELECT first_name AS fname, last_name AS lname FROM users",
			wantDiag: false,
		},
		{
			name:     "no column aliases",
			sql:      "SELECT first_name, last_name FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AL08")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AL08 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AL08 diagnostic")
			}
		})
	}
}

func TestAL09_SelfAlias(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "self alias - same name",
			sql:      "SELECT * FROM users users",
			wantDiag: true,
		},
		{
			name:     "self alias - case insensitive",
			sql:      "SELECT * FROM users USERS",
			wantDiag: true,
		},
		{
			name:     "different alias",
			sql:      "SELECT * FROM users usr",
			wantDiag: false,
		},
		{
			name:     "no alias",
			sql:      "SELECT * FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AL09")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AL09 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AL09 diagnostic")
			}
		})
	}
}
