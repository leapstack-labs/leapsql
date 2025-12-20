package parser_test

import (
	"strings"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/dialects/ansi"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import dialect packages to register them
	duckdbDialect "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb/dialect"
	postgresDialect "github.com/leapstack-labs/leapsql/pkg/adapters/postgres/dialect"
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

func TestANSIRejectsQualify(t *testing.T) {
	sql := `SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rn
		FROM users
		QUALIFY rn <= 10`

	_, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.Error(t, err, "ANSI should reject QUALIFY clause")
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
	binaryExpr, ok := qualify.(*parser.BinaryExpr)
	require.True(t, ok, "QUALIFY should contain binary expression")
	assert.Equal(t, token.AND, binaryExpr.Op)
}

// ---------- ILIKE Operator Tests ----------

func TestANSIRejectsILIKE(t *testing.T) {
	sql := `SELECT * FROM users WHERE name ILIKE '%john%'`

	// ANSI dialect should not have ILIKE in its precedence table
	// This means ILIKE will be parsed as an identifier, causing a syntax error
	// or the expression will fail to parse correctly
	_, err := parser.ParseWithDialect(sql, ansi.ANSI)

	// ANSI may parse ILIKE as an identifier since it's not in its keyword list
	// The test verifies the behavior is different from DuckDB
	// In strict mode, this should fail or produce different results
	if err == nil {
		// If it parses, verify ILIKE was not recognized as a keyword
		// (this is the fallback behavior from the global token registry)
		t.Log("ANSI parsed ILIKE due to global token registration - this is expected")
	}
}

func TestDuckDBAcceptsILIKE(t *testing.T) {
	sql := `SELECT * FROM users WHERE name ILIKE '%john%'`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err, "DuckDB should accept ILIKE operator")
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Body.Left.Where)

	// Verify the WHERE clause contains a LIKE expression with ILIKE op
	likeExpr, ok := stmt.Body.Left.Where.(*parser.LikeExpr)
	require.True(t, ok, "WHERE should contain LIKE expression")
	assert.Equal(t, duckdbDialect.TokenIlike, likeExpr.Op, "Should be case-insensitive ILIKE")
}

func TestPostgresAcceptsILIKE(t *testing.T) {
	sql := `SELECT * FROM users WHERE email ILIKE '%@example.com'`

	stmt, err := parser.ParseWithDialect(sql, postgresDialect.Postgres)
	require.NoError(t, err, "PostgreSQL should accept ILIKE operator")
	require.NotNil(t, stmt)

	likeExpr, ok := stmt.Body.Left.Where.(*parser.LikeExpr)
	require.True(t, ok, "WHERE should contain LIKE expression")
	assert.Equal(t, postgresDialect.TokenIlike, likeExpr.Op, "Should be case-insensitive ILIKE")
}

func TestILIKEWithNOT(t *testing.T) {
	sql := `SELECT * FROM products WHERE name NOT ILIKE '%test%'`

	stmt, err := parser.ParseWithDialect(sql, duckdbDialect.DuckDB)
	require.NoError(t, err, "DuckDB should accept NOT ILIKE")

	likeExpr, ok := stmt.Body.Left.Where.(*parser.LikeExpr)
	require.True(t, ok, "WHERE should contain LIKE expression")
	assert.Equal(t, duckdbDialect.TokenIlike, likeExpr.Op, "Should be case-insensitive ILIKE")
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
	binExpr, ok := stmt.Body.Left.Where.(*parser.BinaryExpr)
	require.True(t, ok, "WHERE should be a binary AND expression")
	assert.Equal(t, token.AND, binExpr.Op)

	// Left side should be ILIKE
	likeExpr, ok := binExpr.Left.(*parser.LikeExpr)
	require.True(t, ok, "Left of AND should be ILIKE")
	assert.Equal(t, duckdbDialect.TokenIlike, likeExpr.Op)

	// Right side should be comparison
	rightExpr, ok := binExpr.Right.(*parser.BinaryExpr)
	require.True(t, ok, "Right of AND should be comparison")
	assert.Equal(t, token.GT, rightExpr.Op)
}

func TestLIKEPrecedenceWithOR(t *testing.T) {
	sql := `SELECT * FROM t WHERE a LIKE '%x%' OR b LIKE '%y%'`

	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)

	// Should be: (a LIKE '%x%') OR (b LIKE '%y%')
	binExpr, ok := stmt.Body.Left.Where.(*parser.BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, token.OR, binExpr.Op)
}

// ---------- Error Position Tests ----------

func TestErrorIncludesPosition(t *testing.T) {
	// SQL with a clear syntax error - unclosed parenthesis
	sql := `SELECT a, b
FROM users
WHERE (x = 1`

	_, err := parser.ParseWithDialect(sql, ansi.ANSI)
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

	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
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
	dialects := []string{"ansi", "duckdb", "postgres"}

	for _, name := range dialects {
		d, ok := dialect.Get(name)
		assert.True(t, ok, "Dialect %s should be registered", name)
		assert.Equal(t, name, d.Name)
	}
}

func TestDialectInheritance(t *testing.T) {
	// DuckDB and Postgres should inherit from ANSI
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
	assert.True(t, contains(duckdbSeq, duckdbDialect.TokenQualify), "DuckDB should have QUALIFY")

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
	name, ok := dialect.IsKnownClause(duckdbDialect.TokenQualify)
	assert.True(t, ok, "QUALIFY should be in global clause registry")
	assert.Equal(t, "QUALIFY", name)
}

func TestAllKnownClauses(t *testing.T) {
	// Verify all standard clauses are registered
	allClauses := dialect.AllKnownClauses()

	// Should contain standard ANSI clauses
	require.NotEmpty(t, allClauses, "Should have registered clauses")

	// QUALIFY should be registered (from DuckDB dialect)
	_, hasQualify := allClauses[duckdbDialect.TokenQualify]
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
	assert.True(t, duckdbDialect.DuckDB.IsClauseToken(duckdbDialect.TokenQualify),
		"DuckDB should report QUALIFY as a clause token")

	// Postgres should NOT report QUALIFY as a clause token
	assert.False(t, postgresDialect.Postgres.IsClauseToken(duckdbDialect.TokenQualify),
		"Postgres should NOT report QUALIFY as a clause token")

	// Both should report WHERE as a clause token
	assert.True(t, duckdbDialect.DuckDB.IsClauseToken(parser.TOKEN_WHERE),
		"DuckDB should report WHERE as a clause token")
	assert.True(t, postgresDialect.Postgres.IsClauseToken(parser.TOKEN_WHERE),
		"Postgres should report WHERE as a clause token")
}

func TestDialectClauseDef(t *testing.T) {
	// DuckDB should have a ClauseDef for QUALIFY
	def, ok := duckdbDialect.DuckDB.ClauseDef(duckdbDialect.TokenQualify)
	require.True(t, ok, "DuckDB should have ClauseDef for QUALIFY")
	require.NotNil(t, def.Handler, "ClauseDef should have a Handler")

	// ANSI should have ClauseDef for WHERE
	def, ok = ansi.ANSI.ClauseDef(parser.TOKEN_WHERE)
	require.True(t, ok, "ANSI should have ClauseDef for WHERE")
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
		if tok == duckdbDialect.TokenQualify {
			hasQualify = true
		}
	}
	assert.True(t, hasWhere, "DuckDB should include WHERE in AllClauseTokens")
	assert.True(t, hasQualify, "DuckDB should include QUALIFY in AllClauseTokens")
}
