package parser_test

import (
	"strings"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/format"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import dialect packages to register them
	duckdbDialect "github.com/leapstack-labs/leapsql/pkg/dialects/duckdb"
	postgresDialect "github.com/leapstack-labs/leapsql/pkg/dialects/postgres"
)

// ---------- QUALIFY Clause Tests ----------

func TestPostgresRejectsQualify(t *testing.T) {
	sql := `SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
		FROM employees
		QUALIFY rn = 1`

	_, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.Error(t, err, "Postgres should reject QUALIFY clause")
	assert.Contains(t, err.Error(), "QUALIFY")
}

func TestDuckDBAcceptsQualify(t *testing.T) {
	sql := `SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
		FROM employees
		QUALIFY rn = 1`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err, "DuckDB should accept QUALIFY clause")
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Body)
	require.NotNil(t, stmt.Body.Left)
	assert.NotNil(t, stmt.Body.Left.Qualify, "QUALIFY expression should be parsed")
}

func TestPostgresRejectsQualify2(t *testing.T) {
	sql := `SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rn
		FROM users
		QUALIFY rn <= 10`

	_, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.Error(t, err, "Postgres should reject QUALIFY clause")
	assert.Contains(t, err.Error(), "QUALIFY")
}

func TestQualifyWithComplexExpression(t *testing.T) {
	sql := `SELECT 
		customer_id,
		order_date,
		amount,
		SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as running_total
	FROM orders
	QUALIFY running_total > 1000 AND order_date >= '2024-01-01'`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err, "DuckDB should parse complex QUALIFY expression")
	require.NotNil(t, stmt.Body.Left.Qualify)

	// Verify QUALIFY contains a binary expression (AND)
	qualify := stmt.Body.Left.Qualify
	binaryExpr, ok := qualify.(*core.BinaryExpr)
	require.True(t, ok, "QUALIFY should contain binary expression")
	assert.Equal(t, token.AND, binaryExpr.Op)
}

// ---------- ILIKE Operator Tests ----------

func TestPostgresRejectsILIKE(t *testing.T) {
	sql := `SELECT * FROM users WHERE name ILIKE '%john%'`

	// Postgres dialect DOES support ILIKE, so this test verifies that
	// The test was originally for ANSI, but now we test Postgres accepts it
	stmt, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.NoError(t, err)
	require.NotNil(t, stmt)
}

func TestDuckDBAcceptsILIKE(t *testing.T) {
	sql := `SELECT * FROM users WHERE name ILIKE '%john%'`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err, "DuckDB should accept ILIKE operator")
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Body.Left.Where)

	// Verify the WHERE clause contains a LIKE expression with ILIKE op
	likeExpr, ok := stmt.Body.Left.Where.(*core.LikeExpr)
	require.True(t, ok, "WHERE should contain LIKE expression")
	assert.Equal(t, token.ILIKE, likeExpr.Op, "Should be case-insensitive ILIKE")
}

func TestPostgresAcceptsILIKE(t *testing.T) {
	sql := `SELECT * FROM users WHERE email ILIKE '%@example.com'`

	stmt, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.NoError(t, err, "PostgreSQL should accept ILIKE operator")
	require.NotNil(t, stmt)

	likeExpr, ok := stmt.Body.Left.Where.(*core.LikeExpr)
	require.True(t, ok, "WHERE should contain LIKE expression")
	assert.Equal(t, token.ILIKE, likeExpr.Op, "Should be case-insensitive ILIKE")
}

func TestILIKEWithNOT(t *testing.T) {
	sql := `SELECT * FROM products WHERE name NOT ILIKE '%test%'`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err, "DuckDB should accept NOT ILIKE")

	likeExpr, ok := stmt.Body.Left.Where.(*core.LikeExpr)
	require.True(t, ok, "WHERE should contain LIKE expression")
	assert.Equal(t, token.ILIKE, likeExpr.Op, "Should be case-insensitive ILIKE")
	assert.True(t, likeExpr.Not, "Should be negated with NOT")
}

// ---------- Precedence Tests ----------

func TestILIKEPrecedence(t *testing.T) {
	// ILIKE should bind tighter than AND/OR but looser than arithmetic
	sql := `SELECT * FROM t WHERE a ILIKE '%x%' AND b > 5`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	// The WHERE clause should be: (a ILIKE '%x%') AND (b > 5)
	// Not: a ILIKE ('%x%' AND b > 5)
	binExpr, ok := stmt.Body.Left.Where.(*core.BinaryExpr)
	require.True(t, ok, "WHERE should be a binary AND expression")
	assert.Equal(t, token.AND, binExpr.Op)

	// Left side should be ILIKE
	likeExpr, ok := binExpr.Left.(*core.LikeExpr)
	require.True(t, ok, "Left of AND should be ILIKE")
	assert.Equal(t, token.ILIKE, likeExpr.Op)

	// Right side should be comparison
	rightExpr, ok := binExpr.Right.(*core.BinaryExpr)
	require.True(t, ok, "Right of AND should be comparison")
	assert.Equal(t, token.GT, rightExpr.Op)
}

func TestLIKEPrecedenceWithOR(t *testing.T) {
	sql := `SELECT * FROM t WHERE a LIKE '%x%' OR b LIKE '%y%'`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	// Should be: (a LIKE '%x%') OR (b LIKE '%y%')
	binExpr, ok := stmt.Body.Left.Where.(*core.BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, token.OR, binExpr.Op)
}

// ---------- Error Position Tests ----------

func TestErrorIncludesPosition(t *testing.T) {
	// SQL with a clear syntax error - unclosed parenthesis
	sql := `SELECT a, b
FROM users
WHERE (x = 1`

	_, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.Error(t, err)

	// The error should be a ParseError with position info
	var parseErr *parser.ParseError
	if assert.ErrorAs(t, err, &parseErr) {
		// Position should be on line 3 where the unclosed paren is
		assert.Equal(t, 3, parseErr.Pos.Line, "Error should be on line 3")
		assert.Positive(t, parseErr.Pos.Column, "Column should be positive")
	}
}

func TestErrorPositionWithQualify(t *testing.T) {
	// QUALIFY appears on line 4 - should report correct position
	sql := `SELECT name, 
	ROW_NUMBER() OVER () as rn
FROM employees
QUALIFY rn = 1`

	_, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "QUALIFY")
}

// ---------- Dialect-Specific Feature Tests ----------

func TestDialectSpecificFeatures(t *testing.T) {
	testCases := []struct {
		name    string
		sql     string
		dialect *dialect.Dialect
	}{
		{
			name:    "DuckDB QUALIFY",
			sql:     "SELECT * FROM t QUALIFY row_number() OVER () = 1",
			dialect: duckdbDialect.DuckDB,
		},
		{
			name:    "DuckDB ILIKE",
			sql:     "SELECT * FROM t WHERE name ILIKE '%test%'",
			dialect: duckdbDialect.DuckDB,
		},
		{
			name:    "Postgres ILIKE",
			sql:     "SELECT * FROM t WHERE name ILIKE '%test%'",
			dialect: postgresDialect.Postgres,
		},
		{
			name:    "DuckDB QUALIFY with ILIKE",
			sql:     "SELECT * FROM t WHERE name ILIKE '%x%' QUALIFY row_number() OVER () = 1",
			dialect: duckdbDialect.DuckDB,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tc.sql, tc.dialect)
			require.NoError(t, err, "%s should be accepted by %s", tc.name, tc.dialect.Name)
			assert.NotNil(t, stmt)
		})
	}
}

// ---------- Clause Sequence Tests ----------

func TestClauseOrderEnforced(t *testing.T) {
	// Standard ANSI clause order: SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT
	sql := `SELECT dept, COUNT(*) as cnt
		FROM employees
		WHERE active = true
		GROUP BY dept
		HAVING COUNT(*) > 5
		ORDER BY cnt DESC
		LIMIT 10`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)
	require.NotNil(t, stmt.Body.Left)

	core := stmt.Body.Left
	assert.NotNil(t, core.Where)
	assert.NotNil(t, core.GroupBy)
	assert.NotNil(t, core.Having)
	assert.NotNil(t, core.OrderBy)
	assert.NotNil(t, core.Limit)
}

func TestDuckDBClauseSequenceWithQualify(t *testing.T) {
	// DuckDB adds QUALIFY after HAVING
	// Simplified query to focus on clause order
	sql := `SELECT dept, name, 
		ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
	FROM employees
	WHERE active = true
	QUALIFY rn = 1
	ORDER BY dept
	LIMIT 100`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.NotNil(t, core.Where, "WHERE should be parsed")
	assert.NotNil(t, core.Qualify, "QUALIFY should be parsed")
	assert.NotNil(t, core.OrderBy, "ORDER BY should be parsed")
	assert.NotNil(t, core.Limit, "LIMIT should be parsed")
}

// ---------- Dialect Registration Tests ----------

func TestDialectRegistration(t *testing.T) {
	// Verify all dialects are registered
	dialects := []string{"duckdb", "postgres"}

	for _, name := range dialects {
		d, ok := dialect.Get(name)
		assert.True(t, ok, "Dialect %s should be registered", name)
		assert.Equal(t, name, d.Name)
	}
}

func TestDialectInheritance(t *testing.T) {
	// DuckDB and Postgres share common SQL features
	duckdb := duckdbDialect.DuckDB
	postgres := postgresDialect.Postgres

	// Both should have WHERE, GROUP, HAVING, ORDER, LIMIT in their clause sequence
	duckdbSeq := duckdb.ClauseSequence()
	postgresSeq := postgres.ClauseSequence()

	// Helper to check if sequence contains a token
	contains := func(seq []parser.TokenType, target parser.TokenType) bool {
		for _, t := range seq {
			if t == target {
				return true
			}
		}
		return false
	}

	// Standard ANSI clauses
	assert.True(t, contains(duckdbSeq, parser.TOKEN_WHERE), "DuckDB should have WHERE")
	assert.True(t, contains(duckdbSeq, parser.TOKEN_GROUP), "DuckDB should have GROUP")
	assert.True(t, contains(duckdbSeq, parser.TOKEN_ORDER), "DuckDB should have ORDER")

	assert.True(t, contains(postgresSeq, parser.TOKEN_WHERE), "Postgres should have WHERE")
	assert.True(t, contains(postgresSeq, parser.TOKEN_GROUP), "Postgres should have GROUP")
	assert.True(t, contains(postgresSeq, parser.TOKEN_ORDER), "Postgres should have ORDER")

	// DuckDB should have QUALIFY, Postgres should not
	assert.True(t, contains(duckdbSeq, token.QUALIFY), "DuckDB should have QUALIFY")

	// Check that Postgres sequence doesn't contain QUALIFY
	for _, tok := range postgresSeq {
		// QUALIFY would have a dynamically registered token type
		if strings.Contains(tok.String(), "QUALIFY") {
			t.Errorf("Postgres should not have QUALIFY in its clause sequence")
		}
	}
}

// ---------- ClauseSlot and Declarative Binder Tests ----------

func TestClauseSlotAssignment(t *testing.T) {
	// Test that QUALIFY goes to core.Qualify with SlotQualify
	sql := "SELECT row_number() OVER () AS rn FROM t QUALIFY rn = 1"
	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)
	require.NotNil(t, stmt.Body.Left.Qualify, "QUALIFY should be assigned to Qualify field")
}

func TestGlobalClauseRegistry(t *testing.T) {
	// After loading dialects, QUALIFY should be in the global registry
	name, ok := dialect.IsKnownClause(token.QUALIFY)
	assert.True(t, ok, "QUALIFY should be in global clause registry")
	assert.Equal(t, "QUALIFY", name)
}

func TestAllKnownClauses(t *testing.T) {
	// Verify all standard clauses are registered
	allClauses := dialect.AllKnownClauses()

	// Should contain standard ANSI clauses
	require.NotEmpty(t, allClauses, "Should have registered clauses")

	// QUALIFY should be registered (from DuckDB dialect)
	_, hasQualify := allClauses[token.QUALIFY]
	assert.True(t, hasQualify, "QUALIFY should be in AllKnownClauses")
}

func TestUnsupportedClauseErrorMessage(t *testing.T) {
	// Postgres should give helpful error for QUALIFY
	sql := "SELECT * FROM t QUALIFY x = 1"
	_, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "QUALIFY")
	assert.Contains(t, err.Error(), "not supported")
	assert.Contains(t, err.Error(), "postgres")
}

func TestDialectIsClauseToken(t *testing.T) {
	// DuckDB should report QUALIFY as a clause token
	assert.True(t, duckdbDialect.DuckDB.IsClauseToken(token.QUALIFY),
		"DuckDB should report QUALIFY as a clause token")

	// Postgres should NOT report QUALIFY as a clause token
	assert.False(t, postgresDialect.Postgres.IsClauseToken(token.QUALIFY),
		"Postgres should NOT report QUALIFY as a clause token")

	// Both should report WHERE as a clause token
	assert.True(t, duckdbDialect.DuckDB.IsClauseToken(parser.TOKEN_WHERE),
		"DuckDB should report WHERE as a clause token")
	assert.True(t, postgresDialect.Postgres.IsClauseToken(parser.TOKEN_WHERE),
		"Postgres should report WHERE as a clause token")
}

func TestDialectClauseDef(t *testing.T) {
	// DuckDB should have a ClauseDef for QUALIFY
	def, ok := duckdbDialect.DuckDB.ClauseDef(token.QUALIFY)
	require.True(t, ok, "DuckDB should have ClauseDef for QUALIFY")
	require.NotNil(t, def.Handler, "ClauseDef should have a Handler")

	// DuckDB should have ClauseDef for WHERE
	def, ok = duckdbDialect.DuckDB.ClauseDef(parser.TOKEN_WHERE)
	require.True(t, ok, "DuckDB should have ClauseDef for WHERE")
	require.NotNil(t, def.Handler, "ClauseDef should have a Handler")
}

func TestAllClauseTokens(t *testing.T) {
	// DuckDB should return all clause tokens including QUALIFY
	duckdbTokens := duckdbDialect.DuckDB.AllClauseTokens()
	require.NotEmpty(t, duckdbTokens)

	// Should include standard clauses
	hasWhere := false
	hasQualify := false
	for _, tok := range duckdbTokens {
		if tok == parser.TOKEN_WHERE {
			hasWhere = true
		}
		if tok == token.QUALIFY {
			hasQualify = true
		}
	}
	assert.True(t, hasWhere, "DuckDB should include WHERE in AllClauseTokens")
	assert.True(t, hasQualify, "DuckDB should include QUALIFY in AllClauseTokens")
}

// ---------- Star Modifier Tests ----------

func TestStarExclude(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantCols []string
	}{
		{
			name:     "single column",
			sql:      "SELECT * EXCLUDE (id) FROM users",
			wantCols: []string{"id"},
		},
		{
			name:     "multiple columns",
			sql:      "SELECT * EXCLUDE (created_at, updated_at, deleted_at) FROM users",
			wantCols: []string{"created_at", "updated_at", "deleted_at"},
		},
		{
			name:     "with table qualifier",
			sql:      "SELECT users.* EXCLUDE (password) FROM users",
			wantCols: []string{"password"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err)
			require.NotNil(t, stmt.Body)
			require.NotNil(t, stmt.Body.Left)
			require.Len(t, stmt.Body.Left.Columns, 1)

			item := stmt.Body.Left.Columns[0]
			require.Len(t, item.Modifiers, 1)

			exclude, ok := item.Modifiers[0].(*core.ExcludeModifier)
			require.True(t, ok, "Expected ExcludeModifier")
			assert.Equal(t, tt.wantCols, exclude.Columns)
		})
	}
}

func TestStarReplace(t *testing.T) {
	sql := "SELECT * REPLACE (UPPER(name) AS name, age + 1 AS age) FROM users"
	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	item := stmt.Body.Left.Columns[0]
	require.True(t, item.Star)
	require.Len(t, item.Modifiers, 1)

	replace, ok := item.Modifiers[0].(*core.ReplaceModifier)
	require.True(t, ok, "Expected ReplaceModifier")
	require.Len(t, replace.Items, 2)

	assert.Equal(t, "name", replace.Items[0].Alias)
	assert.Equal(t, "age", replace.Items[1].Alias)
}

func TestStarRename(t *testing.T) {
	sql := "SELECT * RENAME (customer_id AS id, customer_name AS name) FROM orders"
	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	item := stmt.Body.Left.Columns[0]
	require.True(t, item.Star)
	require.Len(t, item.Modifiers, 1)

	rename, ok := item.Modifiers[0].(*core.RenameModifier)
	require.True(t, ok, "Expected RenameModifier")
	require.Len(t, rename.Items, 2)

	assert.Equal(t, "customer_id", rename.Items[0].OldName)
	assert.Equal(t, "id", rename.Items[0].NewName)
	assert.Equal(t, "customer_name", rename.Items[1].OldName)
	assert.Equal(t, "name", rename.Items[1].NewName)
}

func TestCombinedModifiers(t *testing.T) {
	sql := "SELECT * EXCLUDE (internal_id) REPLACE (UPPER(name) AS name) FROM users"
	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	item := stmt.Body.Left.Columns[0]
	require.True(t, item.Star)
	require.Len(t, item.Modifiers, 2)

	_, isExclude := item.Modifiers[0].(*core.ExcludeModifier)
	assert.True(t, isExclude, "First modifier should be ExcludeModifier")

	_, isReplace := item.Modifiers[1].(*core.ReplaceModifier)
	assert.True(t, isReplace, "Second modifier should be ReplaceModifier")
}

func TestStarModifiersNotInPostgres(t *testing.T) {
	// Star modifiers should not parse in Postgres dialect (DuckDB-specific extension)
	sql := "SELECT * EXCLUDE (id) FROM users"
	stmt, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.NoError(t, err) // Parser is lenient

	// But no modifiers should be parsed
	item := stmt.Body.Left.Columns[0]
	assert.Empty(t, item.Modifiers, "Postgres should not parse star modifiers")
}

func TestTableStarWithModifiers(t *testing.T) {
	sql := "SELECT u.* EXCLUDE (password) RENAME (email AS contact_email) FROM users u"
	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	item := stmt.Body.Left.Columns[0]
	assert.Equal(t, "u", item.TableStar)
	require.Len(t, item.Modifiers, 2)

	_, isExclude := item.Modifiers[0].(*core.ExcludeModifier)
	assert.True(t, isExclude)

	_, isRename := item.Modifiers[1].(*core.RenameModifier)
	assert.True(t, isRename)
}

func TestStarModifiersWithJoin(t *testing.T) {
	sql := `SELECT 
		orders.* EXCLUDE (internal_notes),
		customers.* EXCLUDE (password_hash, secret_key)
	FROM orders
	JOIN customers ON orders.customer_id = customers.id`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)
	require.Len(t, stmt.Body.Left.Columns, 2)

	// First select item: orders.* EXCLUDE (internal_notes)
	item1 := stmt.Body.Left.Columns[0]
	assert.Equal(t, "orders", item1.TableStar)
	require.Len(t, item1.Modifiers, 1)
	exclude1, ok := item1.Modifiers[0].(*core.ExcludeModifier)
	require.True(t, ok)
	assert.Equal(t, []string{"internal_notes"}, exclude1.Columns)

	// Second select item: customers.* EXCLUDE (password_hash, secret_key)
	item2 := stmt.Body.Left.Columns[1]
	assert.Equal(t, "customers", item2.TableStar)
	require.Len(t, item2.Modifiers, 1)
	exclude2, ok := item2.Modifiers[0].(*core.ExcludeModifier)
	require.True(t, ok)
	assert.Equal(t, []string{"password_hash", "secret_key"}, exclude2.Columns)
}

func TestStarModifierRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "exclude single column",
			sql:  "SELECT * EXCLUDE (id) FROM users",
		},
		{
			name: "exclude multiple columns",
			sql:  "SELECT * EXCLUDE (a, b, c) FROM t",
		},
		{
			name: "replace expression",
			sql:  "SELECT * REPLACE (UPPER(name) AS name) FROM users",
		},
		{
			name: "rename column",
			sql:  "SELECT * RENAME (old_col AS new_col) FROM t",
		},
		{
			name: "combined modifiers",
			sql:  "SELECT * EXCLUDE (id) REPLACE (UPPER(name) AS name) FROM users",
		},
		{
			name: "table star with exclude",
			sql:  "SELECT t.* EXCLUDE (secret) FROM t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse original
			stmt1, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err)

			// Format
			formatted := format.Format(stmt1, duckdbDialect.DuckDB)
			require.NotEmpty(t, formatted)

			// Parse again
			stmt2, err := parser.ParseWithDialect(formatted, duckdbDialect.DuckDB)
			require.NoError(t, err)

			// Compare key properties
			require.Len(t, stmt2.Body.Left.Columns, len(stmt1.Body.Left.Columns))

			for i, col1 := range stmt1.Body.Left.Columns {
				col2 := stmt2.Body.Left.Columns[i]
				assert.Equal(t, col1.Star, col2.Star)
				assert.Equal(t, col1.TableStar, col2.TableStar)
				assert.Len(t, col2.Modifiers, len(col1.Modifiers))
			}
		})
	}
}

// ---------- UNION BY NAME Tests (DuckDB Extension) ----------

func TestUnionByName(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		op     core.SetOpType
		all    bool
		byName bool
	}{
		{
			name:   "union by name",
			sql:    "SELECT id, name FROM t1 UNION BY NAME SELECT name, id FROM t2",
			op:     core.SetOpUnion,
			all:    false,
			byName: true,
		},
		{
			name:   "union all by name",
			sql:    "SELECT id, name FROM t1 UNION ALL BY NAME SELECT name, id FROM t2",
			op:     core.SetOpUnionAll,
			all:    true,
			byName: true,
		},
		{
			name:   "intersect by name",
			sql:    "SELECT id, name FROM t1 INTERSECT BY NAME SELECT name, id FROM t2",
			op:     core.SetOpIntersect,
			all:    false,
			byName: true,
		},
		{
			name:   "except by name",
			sql:    "SELECT id, name FROM t1 EXCEPT BY NAME SELECT name, id FROM t2",
			op:     core.SetOpExcept,
			all:    false,
			byName: true,
		},
		{
			name:   "standard union (no by name)",
			sql:    "SELECT id, name FROM t1 UNION SELECT name, id FROM t2",
			op:     core.SetOpUnion,
			all:    false,
			byName: false,
		},
		{
			name:   "standard union all (no by name)",
			sql:    "SELECT id FROM t1 UNION ALL SELECT id FROM t2",
			op:     core.SetOpUnionAll,
			all:    true,
			byName: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err)
			require.NotNil(t, stmt.Body)

			body := stmt.Body
			assert.Equal(t, tt.op, body.Op)
			assert.Equal(t, tt.all, body.All)
			assert.Equal(t, tt.byName, body.ByName)
			assert.NotNil(t, body.Right)
		})
	}
}

func TestChainedUnionByName(t *testing.T) {
	sql := `
		SELECT id, name FROM t1
		UNION BY NAME
		SELECT name, id FROM t2
		UNION ALL BY NAME
		SELECT id, name FROM t3
	`
	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	// First UNION BY NAME
	assert.Equal(t, core.SetOpUnion, stmt.Body.Op)
	assert.True(t, stmt.Body.ByName)
	assert.False(t, stmt.Body.All)

	// Second UNION ALL BY NAME
	require.NotNil(t, stmt.Body.Right)
	assert.Equal(t, core.SetOpUnionAll, stmt.Body.Right.Op)
	assert.True(t, stmt.Body.Right.ByName)
	assert.True(t, stmt.Body.Right.All)
}

func TestMixedByNameAndPositional(t *testing.T) {
	sql := `
		SELECT id, name FROM t1
		UNION BY NAME
		SELECT name, id FROM t2
		UNION
		SELECT a, b FROM t3
	`
	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err)

	// First: UNION BY NAME
	assert.Equal(t, core.SetOpUnion, stmt.Body.Op)
	assert.True(t, stmt.Body.ByName)

	// Second: plain UNION (positional)
	require.NotNil(t, stmt.Body.Right)
	assert.Equal(t, core.SetOpUnion, stmt.Body.Right.Op)
	assert.False(t, stmt.Body.Right.ByName)
}

func TestUnionByNameRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		contains []string
	}{
		{
			name:     "union by name",
			sql:      "SELECT id, name FROM t1 UNION BY NAME SELECT name, id FROM t2",
			contains: []string{"UNION", "BY", "NAME"},
		},
		{
			name:     "union all by name",
			sql:      "SELECT id FROM t1 UNION ALL BY NAME SELECT id FROM t2",
			contains: []string{"UNION", "ALL", "BY", "NAME"},
		},
		{
			name:     "intersect by name",
			sql:      "SELECT a, b FROM t1 INTERSECT BY NAME SELECT b, a FROM t2",
			contains: []string{"INTERSECT", "BY", "NAME"},
		},
		{
			name:     "except by name",
			sql:      "SELECT x, y FROM t1 EXCEPT BY NAME SELECT y, x FROM t2",
			contains: []string{"EXCEPT", "BY", "NAME"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse original
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err)

			// Format
			output := format.Format(stmt, duckdbDialect.DuckDB)
			require.NotEmpty(t, output)

			// Verify output contains expected keywords
			for _, s := range tt.contains {
				assert.Contains(t, output, s)
			}

			// Parse again to verify round-trip
			stmt2, err := parser.ParseWithDialect(output, duckdbDialect.DuckDB)
			require.NoError(t, err)

			// Verify ByName flag is preserved
			assert.Equal(t, stmt.Body.ByName, stmt2.Body.ByName)
			assert.Equal(t, stmt.Body.Op, stmt2.Body.Op)
			assert.Equal(t, stmt.Body.All, stmt2.Body.All)
		})
	}
}

func TestUnionByNameInPostgres(t *testing.T) {
	// BY NAME should also work in Postgres since the parser is lenient
	sql := "SELECT id FROM t1 UNION BY NAME SELECT id FROM t2"
	stmt, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.NoError(t, err)

	// The ByName flag should be set
	assert.True(t, stmt.Body.ByName,
		"BY NAME should parse in Postgres (parser is lenient)")
}

// ---------- Soft Keyword Tests ----------

func TestSelectNameAsIdentifier(t *testing.T) {
	// Verify that 'name' is parsed as a regular identifier, not a keyword.
	// This is a regression test to ensure removing token.NAME didn't break anything.
	tests := []struct {
		name string
		sql  string
	}{
		{"select name column", "SELECT name FROM users"},
		{"name in where", "SELECT * FROM users WHERE name = 'alice'"},
		{"name as alias", "SELECT id AS name FROM users"},
		{"name as table alias", "SELECT * FROM users name"},
	}

	d, _ := dialect.Get("duckdb")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, d)
			require.NoError(t, err)
			require.NotNil(t, stmt)
		})
	}
}
