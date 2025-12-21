package parser_test

import (
	"strings"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/dialects/ansi"
	"github.com/leapstack-labs/leapsql/pkg/format"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- NATURAL JOIN Tests ----------

func TestNaturalJoin(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantType parser.JoinType
		natural  bool
	}{
		{
			name:     "natural inner join",
			sql:      "SELECT * FROM t1 NATURAL JOIN t2",
			wantType: parser.JoinInner,
			natural:  true,
		},
		{
			name:     "natural left join",
			sql:      "SELECT * FROM t1 NATURAL LEFT JOIN t2",
			wantType: parser.JoinLeft,
			natural:  true,
		},
		{
			name:     "natural right join",
			sql:      "SELECT * FROM t1 NATURAL RIGHT JOIN t2",
			wantType: parser.JoinRight,
			natural:  true,
		},
		{
			name:     "natural full join",
			sql:      "SELECT * FROM t1 NATURAL FULL JOIN t2",
			wantType: parser.JoinFull,
			natural:  true,
		},
		{
			name:     "natural left outer join",
			sql:      "SELECT * FROM t1 NATURAL LEFT OUTER JOIN t2",
			wantType: parser.JoinLeft,
			natural:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, ansi.ANSI)
			require.NoError(t, err)
			require.NotNil(t, stmt.Body)
			require.NotNil(t, stmt.Body.Left)
			require.NotNil(t, stmt.Body.Left.From)
			require.Len(t, stmt.Body.Left.From.Joins, 1)

			join := stmt.Body.Left.From.Joins[0]
			assert.Equal(t, tt.wantType, join.Type)
			assert.Equal(t, tt.natural, join.Natural)
			assert.Nil(t, join.Condition, "NATURAL JOIN should not have ON")
			assert.Empty(t, join.Using, "NATURAL JOIN should not have USING")
		})
	}
}

func TestNaturalJoinRejectsOnClause(t *testing.T) {
	sql := "SELECT * FROM t1 NATURAL JOIN t2 ON t1.id = t2.id"
	_, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NATURAL JOIN cannot have ON")
}

func TestNaturalJoinRejectsUsingClause(t *testing.T) {
	sql := "SELECT * FROM t1 NATURAL JOIN t2 USING (id)"
	_, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NATURAL JOIN cannot have USING")
}

// ---------- JOIN ... USING Tests ----------

func TestJoinUsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantCols []string
		joinType parser.JoinType
	}{
		{
			name:     "single column",
			sql:      "SELECT * FROM t1 JOIN t2 USING (id)",
			wantCols: []string{"id"},
			joinType: parser.JoinInner,
		},
		{
			name:     "multiple columns",
			sql:      "SELECT * FROM t1 JOIN t2 USING (id, name, region)",
			wantCols: []string{"id", "name", "region"},
			joinType: parser.JoinInner,
		},
		{
			name:     "left join using",
			sql:      "SELECT * FROM t1 LEFT JOIN t2 USING (customer_id)",
			wantCols: []string{"customer_id"},
			joinType: parser.JoinLeft,
		},
		{
			name:     "right join using",
			sql:      "SELECT * FROM t1 RIGHT JOIN t2 USING (order_id)",
			wantCols: []string{"order_id"},
			joinType: parser.JoinRight,
		},
		{
			name:     "full join using",
			sql:      "SELECT * FROM t1 FULL JOIN t2 USING (key)",
			wantCols: []string{"key"},
			joinType: parser.JoinFull,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, ansi.ANSI)
			require.NoError(t, err)
			require.NotNil(t, stmt.Body.Left.From)
			require.Len(t, stmt.Body.Left.From.Joins, 1)

			join := stmt.Body.Left.From.Joins[0]
			assert.Equal(t, tt.wantCols, join.Using)
			assert.Equal(t, tt.joinType, join.Type)
			assert.Nil(t, join.Condition, "USING should not have ON")
			assert.False(t, join.Natural)
		})
	}
}

// ---------- FETCH Clause Tests ----------

func TestFetchClause(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		count    string // expected count literal value ("" = nil/default)
		percent  bool
		withTies bool
		first    bool
	}{
		{
			name:     "fetch first n rows only",
			sql:      "SELECT * FROM t FETCH FIRST 10 ROWS ONLY",
			count:    "10",
			first:    true,
			withTies: false,
		},
		{
			name:     "fetch next n rows only",
			sql:      "SELECT * FROM t FETCH NEXT 5 ROWS ONLY",
			count:    "5",
			first:    false,
			withTies: false,
		},
		{
			name:     "fetch first row only (singular, no count)",
			sql:      "SELECT * FROM t FETCH FIRST ROW ONLY",
			count:    "", // nil count
			first:    true,
			withTies: false,
		},
		{
			name:     "fetch with ties",
			sql:      "SELECT * FROM t ORDER BY x FETCH FIRST 10 ROWS WITH TIES",
			count:    "10",
			first:    true,
			withTies: true,
		},
		{
			name:    "fetch percent",
			sql:     "SELECT * FROM t FETCH FIRST 10 PERCENT ROWS ONLY",
			count:   "10",
			percent: true,
			first:   true,
		},
		{
			name:     "fetch next with ties",
			sql:      "SELECT * FROM t ORDER BY y FETCH NEXT 3 ROWS WITH TIES",
			count:    "3",
			first:    false,
			withTies: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, ansi.ANSI)
			require.NoError(t, err)
			require.NotNil(t, stmt.Body.Left)
			require.NotNil(t, stmt.Body.Left.Fetch, "FETCH clause should be parsed")

			fetch := stmt.Body.Left.Fetch
			assert.Equal(t, tt.first, fetch.First, "First flag mismatch")
			assert.Equal(t, tt.percent, fetch.Percent, "Percent flag mismatch")
			assert.Equal(t, tt.withTies, fetch.WithTies, "WithTies flag mismatch")

			if tt.count != "" {
				require.NotNil(t, fetch.Count)
				lit, ok := fetch.Count.(*parser.Literal)
				require.True(t, ok, "Count should be a Literal")
				assert.Equal(t, tt.count, lit.Value)
			} else {
				assert.Nil(t, fetch.Count, "Count should be nil")
			}
		})
	}
}

func TestFetchAndLimitCoexist(t *testing.T) {
	// Parser should allow both (lenient) - validation is a separate concern
	sql := "SELECT * FROM t LIMIT 10 FETCH FIRST 5 ROWS ONLY"
	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)
	assert.NotNil(t, stmt.Body.Left.Limit, "LIMIT should be parsed")
	assert.NotNil(t, stmt.Body.Left.Fetch, "FETCH should also be parsed")
}

func TestFetchWithOrderBy(t *testing.T) {
	sql := "SELECT id, name FROM users ORDER BY id DESC FETCH FIRST 10 ROWS ONLY"
	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)

	core := stmt.Body.Left
	require.NotNil(t, core.OrderBy)
	require.NotNil(t, core.Fetch)
	assert.Equal(t, "10", core.Fetch.Count.(*parser.Literal).Value)
	assert.True(t, core.Fetch.First)
	assert.False(t, core.Fetch.WithTies)
}

// ---------- Formatter Round-Trip Tests ----------

func TestFormatNaturalJoin(t *testing.T) {
	input := "SELECT * FROM t1 NATURAL LEFT JOIN t2"
	stmt, err := parser.ParseWithDialect(input, ansi.ANSI)
	require.NoError(t, err)

	output := format.Format(stmt, ansi.ANSI)
	assert.Contains(t, output, "NATURAL")
	assert.Contains(t, output, "LEFT JOIN")
}

func TestFormatJoinUsing(t *testing.T) {
	input := "SELECT * FROM t1 JOIN t2 USING (id, name)"
	stmt, err := parser.ParseWithDialect(input, ansi.ANSI)
	require.NoError(t, err)

	output := format.Format(stmt, ansi.ANSI)
	assert.Contains(t, output, "USING")
	assert.Contains(t, output, "id")
	assert.Contains(t, output, "name")
}

func TestFormatFetch(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect []string
	}{
		{
			name:   "fetch first rows only",
			input:  "SELECT * FROM t FETCH FIRST 10 ROWS ONLY",
			expect: []string{"FETCH", "FIRST", "10", "ROWS", "ONLY"},
		},
		{
			name:   "fetch with ties",
			input:  "SELECT * FROM t ORDER BY x FETCH FIRST 10 ROWS WITH TIES",
			expect: []string{"FETCH", "FIRST", "WITH", "TIES"},
		},
		{
			name:   "fetch next",
			input:  "SELECT * FROM t FETCH NEXT 5 ROWS ONLY",
			expect: []string{"FETCH", "NEXT", "ROWS", "ONLY"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.input, ansi.ANSI)
			require.NoError(t, err)

			output := format.Format(stmt, ansi.ANSI)
			for _, expected := range tt.expect {
				assert.Contains(t, strings.ToUpper(output), expected,
					"Output should contain %s", expected)
			}
		})
	}
}

// ---------- Multiple Join Tests ----------

func TestMultipleJoinsWithDifferentStyles(t *testing.T) {
	sql := `SELECT a.id, b.name, c.value
		FROM table_a a
		JOIN table_b b ON a.id = b.a_id
		NATURAL LEFT JOIN table_c c`

	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)
	require.Len(t, stmt.Body.Left.From.Joins, 2)

	// First join: regular ON
	join1 := stmt.Body.Left.From.Joins[0]
	assert.Equal(t, parser.JoinInner, join1.Type)
	assert.False(t, join1.Natural)
	assert.NotNil(t, join1.Condition)
	assert.Empty(t, join1.Using)

	// Second join: NATURAL LEFT
	join2 := stmt.Body.Left.From.Joins[1]
	assert.Equal(t, parser.JoinLeft, join2.Type)
	assert.True(t, join2.Natural)
	assert.Nil(t, join2.Condition)
	assert.Empty(t, join2.Using)
}

func TestMultipleJoinsWithUsing(t *testing.T) {
	sql := `SELECT *
		FROM orders o
		JOIN customers c USING (customer_id)
		JOIN products p USING (product_id)`

	stmt, err := parser.ParseWithDialect(sql, ansi.ANSI)
	require.NoError(t, err)
	require.Len(t, stmt.Body.Left.From.Joins, 2)

	// First join
	join1 := stmt.Body.Left.From.Joins[0]
	assert.Equal(t, []string{"customer_id"}, join1.Using)

	// Second join
	join2 := stmt.Body.Left.From.Joins[1]
	assert.Equal(t, []string{"product_id"}, join2.Using)
}
