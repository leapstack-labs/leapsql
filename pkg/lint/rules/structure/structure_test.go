package structure_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/leapstack-labs/leapsql/pkg/dialects/ansi"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/rules" // register rules
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

func TestST01_ElseNull(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "ELSE NULL - redundant",
			sql:      "SELECT CASE WHEN active THEN 1 ELSE NULL END FROM users",
			wantDiag: true,
		},
		{
			name:     "ELSE with value",
			sql:      "SELECT CASE WHEN active THEN 1 ELSE 0 END FROM users",
			wantDiag: false,
		},
		{
			name:     "no ELSE",
			sql:      "SELECT CASE WHEN active THEN 1 END FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "ST01")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected ST01 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected ST01 diagnostic")
			}
		})
	}
}

func TestST02_SimpleCaseConversion(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "searched CASE that can be simple",
			sql:      "SELECT CASE WHEN status = 'a' THEN 1 WHEN status = 'b' THEN 2 END FROM orders",
			wantDiag: true,
		},
		{
			name:     "simple CASE already",
			sql:      "SELECT CASE status WHEN 'a' THEN 1 WHEN 'b' THEN 2 END FROM orders",
			wantDiag: false,
		},
		{
			name:     "searched CASE with different columns",
			sql:      "SELECT CASE WHEN a = 1 THEN 'x' WHEN b = 2 THEN 'y' END FROM t",
			wantDiag: false,
		},
		{
			name:     "single WHEN clause",
			sql:      "SELECT CASE WHEN status = 'a' THEN 1 END FROM orders",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "ST02")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected ST02 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected ST02 diagnostic")
			}
		})
	}
}

func TestST03_UnusedCTE(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "unused CTE",
			sql:      "WITH cte AS (SELECT 1) SELECT * FROM users",
			wantDiag: true,
		},
		{
			name:     "used CTE",
			sql:      "WITH cte AS (SELECT id FROM users) SELECT * FROM cte",
			wantDiag: false,
		},
		{
			name:     "CTE used by another CTE",
			sql:      "WITH cte1 AS (SELECT 1 AS col_a), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2",
			wantDiag: false,
		},
		{
			name:     "no CTE",
			sql:      "SELECT * FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "ST03")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected ST03 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected ST03 diagnostic")
			}
		})
	}
}

func TestST04_NestedCase(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "nested CASE in THEN",
			sql:      "SELECT CASE WHEN a = 1 THEN CASE WHEN b = 2 THEN 'x' END END FROM t",
			wantDiag: true,
		},
		{
			name:     "nested CASE in ELSE",
			sql:      "SELECT CASE WHEN a = 1 THEN 'y' ELSE CASE WHEN b = 2 THEN 'x' END END FROM t",
			wantDiag: true,
		},
		{
			name:     "simple CASE - no nesting",
			sql:      "SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM t",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "ST04")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected ST04 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected ST04 diagnostic")
			}
		})
	}
}

func TestST06_SelectColumnOrder(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "wildcard before explicit column",
			sql:      "SELECT *, id FROM users",
			wantDiag: true,
		},
		{
			name:     "wildcard last",
			sql:      "SELECT id, * FROM users",
			wantDiag: false,
		},
		{
			name:     "explicit column then table star then explicit",
			sql:      "SELECT id, users.*, name FROM users",
			wantDiag: true,
		},
		{
			name:     "only wildcard",
			sql:      "SELECT * FROM users",
			wantDiag: false,
		},
		{
			name:     "no wildcard",
			sql:      "SELECT id, name FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "ST06")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected ST06 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected ST06 diagnostic")
			}
		})
	}
}

func TestST07_PreferUsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "ON with same column name",
			sql:      "SELECT * FROM users usr JOIN orders ord ON usr.id = ord.id",
			wantDiag: true,
		},
		{
			name:     "ON with different column names",
			sql:      "SELECT * FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			wantDiag: false,
		},
		{
			name:     "USING clause",
			sql:      "SELECT * FROM users JOIN orders USING (id)",
			wantDiag: false,
		},
		{
			name:     "NATURAL JOIN",
			sql:      "SELECT * FROM users NATURAL JOIN orders",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "ST07")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected ST07 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected ST07 diagnostic")
			}
		})
	}
}

func TestST08_DistinctVsGroupBy(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "DISTINCT on simple columns",
			sql:      "SELECT DISTINCT name, status FROM users",
			wantDiag: true,
		},
		{
			name:     "GROUP BY instead",
			sql:      "SELECT name, status FROM users GROUP BY name, status",
			wantDiag: false,
		},
		{
			name:     "DISTINCT with star",
			sql:      "SELECT DISTINCT * FROM users",
			wantDiag: false,
		},
		{
			name:     "no DISTINCT",
			sql:      "SELECT name FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "ST08")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected ST08 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected ST08 diagnostic")
			}
		})
	}
}
