package rules_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	_ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules" // register rules
)

func TestAM01_DistinctWithGroupBy(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "DISTINCT with GROUP BY",
			sql:      "SELECT DISTINCT name FROM users GROUP BY name",
			wantDiag: true,
		},
		{
			name:     "DISTINCT without GROUP BY",
			sql:      "SELECT DISTINCT name FROM users",
			wantDiag: false,
		},
		{
			name:     "GROUP BY without DISTINCT",
			sql:      "SELECT name FROM users GROUP BY name",
			wantDiag: false,
		},
		{
			name:     "no DISTINCT or GROUP BY",
			sql:      "SELECT name FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AM01")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AM01 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AM01 diagnostic")
			}
		})
	}
}

func TestAM02_UnionDistinct(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "UNION without ALL",
			sql:      "SELECT id FROM users UNION SELECT id FROM admins",
			wantDiag: true,
		},
		{
			name:     "UNION ALL",
			sql:      "SELECT id FROM users UNION ALL SELECT id FROM admins",
			wantDiag: false,
		},
		{
			name:     "no union",
			sql:      "SELECT id FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AM02")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AM02 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AM02 diagnostic")
			}
		})
	}
}

func TestAM04_ColumnCountMismatch(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "mismatched column count",
			sql:      "SELECT id, name FROM users UNION ALL SELECT id FROM admins",
			wantDiag: true,
		},
		{
			name:     "matching column count",
			sql:      "SELECT id, name FROM users UNION ALL SELECT id, name FROM admins",
			wantDiag: false,
		},
		{
			name:     "single query",
			sql:      "SELECT id, name FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AM04")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AM04 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AM04 diagnostic")
			}
		})
	}
}

func TestAM05_ImplicitJoin(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "comma-separated tables",
			sql:      "SELECT * FROM users, orders WHERE users.id = orders.user_id",
			wantDiag: true,
		},
		{
			name:     "explicit JOIN",
			sql:      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			wantDiag: false,
		},
		{
			name:     "single table",
			sql:      "SELECT * FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AM05")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AM05 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AM05 diagnostic")
			}
		})
	}
}

func TestAM06_AmbiguousColumnRef(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "unqualified column with multiple tables",
			sql:      "SELECT id FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			wantDiag: true,
		},
		{
			name:     "qualified columns with multiple tables",
			sql:      "SELECT usr.id FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			wantDiag: false,
		},
		{
			name:     "single table - no ambiguity",
			sql:      "SELECT id FROM users",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AM06")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AM06 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AM06 diagnostic")
			}
		})
	}
}

func TestAM08_JoinConditionTables(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDiag bool
	}{
		{
			name:     "join condition references both tables",
			sql:      "SELECT * FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			wantDiag: false,
		},
		{
			name:     "join condition missing right table ref",
			sql:      "SELECT * FROM users usr JOIN orders ord ON usr.id = usr.parent_id",
			wantDiag: true,
		},
		{
			name:     "NATURAL JOIN - no condition needed",
			sql:      "SELECT * FROM users NATURAL JOIN orders",
			wantDiag: false,
		},
		{
			name:     "JOIN with USING",
			sql:      "SELECT * FROM users JOIN orders USING (user_id)",
			wantDiag: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diags := runRule(t, tt.sql, "AM08")
			if tt.wantDiag {
				assert.NotEmpty(t, diags, "expected AM08 diagnostic")
			} else {
				assert.Empty(t, diags, "unexpected AM08 diagnostic")
			}
		})
	}
}
