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

func TestST09_JoinConditionOrder(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
		message  string
	}{
		{
			name:     "correct order - left table first",
			sql:      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
			wantDiag: false,
		},
		{
			name:     "wrong order - right table first",
			sql:      "SELECT * FROM users u JOIN orders o ON o.user_id = u.id",
			wantDiag: true,
			message:  "consider rewriting as 'u.id = o.user_id'",
		},
		{
			name:     "no table qualifier on left",
			sql:      "SELECT * FROM users u JOIN orders o ON id = o.user_id",
			wantDiag: false, // Can't determine order without qualifiers
		},
		{
			name:     "no table qualifier on right",
			sql:      "SELECT * FROM users u JOIN orders o ON u.id = user_id",
			wantDiag: false, // Can't determine order without qualifiers
		},
		{
			name:     "correct order with aliased tables",
			sql:      "SELECT * FROM users AS a JOIN orders AS b ON a.id = b.user_id",
			wantDiag: false,
		},
		{
			name:     "wrong order with aliased tables",
			sql:      "SELECT * FROM users AS a JOIN orders AS b ON b.user_id = a.id",
			wantDiag: true,
			message:  "consider rewriting as 'a.id = b.user_id'",
		},
		{
			name:     "chained join - correct order",
			sql:      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.id = i.order_id",
			wantDiag: false,
		},
		{
			name:     "using clause - no check needed",
			sql:      "SELECT * FROM users u JOIN orders o USING (id)",
			wantDiag: false,
		},
		{
			name:     "natural join - no check needed",
			sql:      "SELECT * FROM users u NATURAL JOIN orders o",
			wantDiag: false,
		},
		{
			name:     "cross join - no condition",
			sql:      "SELECT * FROM users u CROSS JOIN orders o",
			wantDiag: false,
		},
		{
			name:     "complex condition - only checks simple equality",
			sql:      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND o.status = 'active'",
			wantDiag: false, // Top-level is AND, not simple equality
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbdialect.DuckDB)
			require.NoError(t, err)

			analyzer := lint.NewAnalyzerWithRegistry(lint.NewConfig(), "duckdb")
			diags := analyzer.AnalyzeWithRegistryRules(stmt, duckdbdialect.DuckDB)

			var st09Diags []lint.Diagnostic
			for _, d := range diags {
				if d.RuleID == "ST09" {
					st09Diags = append(st09Diags, d)
				}
			}

			if tt.wantDiag {
				require.NotEmpty(t, st09Diags, "expected ST09 diagnostic for %q", tt.sql)
				assert.Contains(t, st09Diags[0].Message, tt.message)
			} else {
				assert.Empty(t, st09Diags, "unexpected ST09 diagnostic for %q", tt.sql)
			}
		})
	}
}
