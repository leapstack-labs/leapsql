package duckdb_test

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/dialects/duckdb"
	"github.com/leapstack-labs/leapsql/pkg/format"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- PIVOT Tests ----------

func TestPivotBasic(t *testing.T) {
	sql := `
		SELECT * FROM monthly_sales
		PIVOT (
			SUM(amount) FOR month IN ('Jan', 'Feb', 'Mar')
		)
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse PIVOT")
	require.NotNil(t, stmt.Body)
	require.NotNil(t, stmt.Body.Left)
	require.NotNil(t, stmt.Body.Left.From)

	pivot, ok := stmt.Body.Left.From.Source.(*parser.PivotTable)
	require.True(t, ok, "Expected PivotTable, got %T", stmt.Body.Left.From.Source)

	assert.Equal(t, "month", pivot.ForColumn)
	assert.Len(t, pivot.Aggregates, 1)
	assert.Equal(t, "SUM", pivot.Aggregates[0].Func.Name)
	assert.Len(t, pivot.InValues, 3)
	assert.False(t, pivot.InStar)
}

func TestPivotMultipleAggregates(t *testing.T) {
	sql := `
		SELECT * FROM sales
		PIVOT (
			SUM(amount) AS total,
			COUNT(*) AS cnt
			FOR region IN ('North', 'South')
		)
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse PIVOT with multiple aggregates")

	pivot, ok := stmt.Body.Left.From.Source.(*parser.PivotTable)
	require.True(t, ok)

	assert.Len(t, pivot.Aggregates, 2)
	assert.Equal(t, "total", pivot.Aggregates[0].Alias)
	assert.Equal(t, "cnt", pivot.Aggregates[1].Alias)
}

func TestPivotInStar(t *testing.T) {
	sql := `
		SELECT * FROM sales
		PIVOT (
			SUM(amount) FOR month IN *
		)
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse PIVOT with IN *")

	pivot, ok := stmt.Body.Left.From.Source.(*parser.PivotTable)
	require.True(t, ok)

	assert.True(t, pivot.InStar)
	assert.Empty(t, pivot.InValues)
}

func TestPivotWithAlias(t *testing.T) {
	sql := `
		SELECT * FROM sales
		PIVOT (
			SUM(amount) FOR month IN ('Jan', 'Feb')
		) AS pivoted
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse PIVOT with alias")

	pivot, ok := stmt.Body.Left.From.Source.(*parser.PivotTable)
	require.True(t, ok)

	assert.Equal(t, "pivoted", pivot.Alias)
}

func TestPivotWithAliasNoAS(t *testing.T) {
	sql := `
		SELECT * FROM sales
		PIVOT (
			SUM(amount) FOR month IN ('Jan', 'Feb')
		) p
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse PIVOT with alias (no AS)")

	pivot, ok := stmt.Body.Left.From.Source.(*parser.PivotTable)
	require.True(t, ok)

	assert.Equal(t, "p", pivot.Alias)
}

// ---------- UNPIVOT Tests ----------

func TestUnpivotBasic(t *testing.T) {
	sql := `
		SELECT * FROM quarterly_report
		UNPIVOT (
			value FOR quarter IN (q1, q2, q3, q4)
		)
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse UNPIVOT")

	unpivot, ok := stmt.Body.Left.From.Source.(*parser.UnpivotTable)
	require.True(t, ok, "Expected UnpivotTable, got %T", stmt.Body.Left.From.Source)

	assert.Equal(t, []string{"value"}, unpivot.ValueColumns)
	assert.Equal(t, "quarter", unpivot.NameColumn)
	assert.Len(t, unpivot.InColumns, 4)
}

func TestUnpivotMultipleValueColumns(t *testing.T) {
	sql := `
		SELECT * FROM report
		UNPIVOT (
			(revenue, cost) FOR quarter IN ((q1_rev, q1_cost), (q2_rev, q2_cost))
		)
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse UNPIVOT with multiple value columns")

	unpivot, ok := stmt.Body.Left.From.Source.(*parser.UnpivotTable)
	require.True(t, ok)

	assert.Equal(t, []string{"revenue", "cost"}, unpivot.ValueColumns)
	assert.Len(t, unpivot.InColumns, 2)
	assert.Equal(t, []string{"q1_rev", "q1_cost"}, unpivot.InColumns[0].Columns)
	assert.Equal(t, []string{"q2_rev", "q2_cost"}, unpivot.InColumns[1].Columns)
}

func TestUnpivotWithAliases(t *testing.T) {
	sql := `
		SELECT * FROM report
		UNPIVOT (
			value FOR quarter IN (q1 AS 'Q1 2024', q2 AS 'Q2 2024')
		)
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse UNPIVOT with column aliases")

	unpivot, ok := stmt.Body.Left.From.Source.(*parser.UnpivotTable)
	require.True(t, ok)

	assert.Equal(t, "Q1 2024", unpivot.InColumns[0].Alias)
	assert.Equal(t, "Q2 2024", unpivot.InColumns[1].Alias)
}

func TestUnpivotWithTableAlias(t *testing.T) {
	sql := `
		SELECT * FROM report
		UNPIVOT (
			value FOR quarter IN (q1, q2)
		) AS u
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse UNPIVOT with table alias")

	unpivot, ok := stmt.Body.Left.From.Source.(*parser.UnpivotTable)
	require.True(t, ok)

	assert.Equal(t, "u", unpivot.Alias)
}

// ---------- PIVOT/UNPIVOT with JOIN Tests ----------

func TestPivotWithJoin(t *testing.T) {
	sql := `
		SELECT * FROM sales
		PIVOT (SUM(amount) FOR month IN ('Jan', 'Feb')) p
		JOIN regions r ON p.region_id = r.id
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse PIVOT with JOIN")

	from := stmt.Body.Left.From
	_, ok := from.Source.(*parser.PivotTable)
	require.True(t, ok)

	assert.Len(t, from.Joins, 1)
}

// ---------- PIVOT/UNPIVOT after Subquery Tests ----------

func TestPivotAfterSubquery(t *testing.T) {
	sql := `
		SELECT * FROM (
			SELECT product, month, amount FROM sales
		) s
		PIVOT (SUM(amount) FOR month IN ('Jan', 'Feb'))
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse PIVOT after subquery")

	pivot, ok := stmt.Body.Left.From.Source.(*parser.PivotTable)
	require.True(t, ok)

	// Source should be a DerivedTable
	_, derivedOk := pivot.Source.(*parser.DerivedTable)
	assert.True(t, derivedOk, "PIVOT source should be DerivedTable")
}

// ---------- Round Trip Tests ----------

func TestPivotRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "basic pivot",
			sql:  "SELECT * FROM sales PIVOT (SUM(amount) FOR month IN ('Jan', 'Feb'))",
		},
		{
			name: "pivot with alias",
			sql:  "SELECT * FROM sales PIVOT (SUM(amount) FOR month IN ('Jan', 'Feb')) AS p",
		},
		{
			name: "pivot with aggregate alias",
			sql:  "SELECT * FROM sales PIVOT (SUM(amount) AS total FOR month IN ('Jan'))",
		},
		{
			name: "pivot IN star",
			sql:  "SELECT * FROM sales PIVOT (SUM(amount) FOR month IN *)",
		},
		{
			name: "basic unpivot",
			sql:  "SELECT * FROM report UNPIVOT (value FOR quarter IN (q1, q2))",
		},
		{
			name: "unpivot with alias",
			sql:  "SELECT * FROM report UNPIVOT (value FOR quarter IN (q1, q2)) AS u",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse original
			stmt1, err := parser.ParseWithDialect(tt.sql, duckdb.DuckDB)
			require.NoError(t, err, "Initial parse failed")

			// Format
			formatted := format.Format(stmt1, duckdb.DuckDB)
			require.NotEmpty(t, formatted, "Format produced empty output")

			// Parse formatted output
			stmt2, err := parser.ParseWithDialect(formatted, duckdb.DuckDB)
			require.NoError(t, err, "Re-parse failed for: %s", formatted)

			// Both should parse successfully and have similar structure
			require.NotNil(t, stmt2.Body)
		})
	}
}

// ---------- Integration Tests ----------

func TestPivotUnpivotInCTE(t *testing.T) {
	sql := `
		WITH pivoted AS (
			SELECT * FROM sales
			PIVOT (SUM(amount) FOR month IN ('Jan', 'Feb'))
		)
		SELECT * FROM pivoted
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err, "Failed to parse PIVOT in CTE")
	require.NotNil(t, stmt.With)
	require.Len(t, stmt.With.CTEs, 1)

	cte := stmt.With.CTEs[0]
	pivot, ok := cte.Select.Body.Left.From.Source.(*parser.PivotTable)
	require.True(t, ok, "CTE should contain PivotTable")
	assert.Equal(t, "month", pivot.ForColumn)
}

// ---------- Error Cases ----------

func TestPivotUnpivotErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "pivot missing opening paren",
			sql:  "SELECT * FROM sales PIVOT SUM(amount) FOR month IN ('Jan')",
		},
		{
			name: "pivot missing FOR",
			sql:  "SELECT * FROM sales PIVOT (SUM(amount) month IN ('Jan'))",
		},
		{
			name: "pivot missing IN",
			sql:  "SELECT * FROM sales PIVOT (SUM(amount) FOR month ('Jan'))",
		},
		{
			name: "pivot unclosed",
			sql:  "SELECT * FROM sales PIVOT (SUM(amount) FOR month IN ('Jan')",
		},
		{
			name: "unpivot missing FOR",
			sql:  "SELECT * FROM report UNPIVOT (value quarter IN (q1))",
		},
		{
			name: "unpivot missing IN",
			sql:  "SELECT * FROM report UNPIVOT (value FOR quarter (q1))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.ParseWithDialect(tt.sql, duckdb.DuckDB)
			assert.Error(t, err, "Expected parse error for: %s", tt.sql)
		})
	}
}
