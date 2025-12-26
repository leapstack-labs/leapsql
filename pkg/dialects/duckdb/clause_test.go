package duckdb_test

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/dialects/duckdb"
	"github.com/leapstack-labs/leapsql/pkg/dialects/postgres"
	"github.com/leapstack-labs/leapsql/pkg/format"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- GROUP BY ALL Tests ----------

func TestGroupByAll(t *testing.T) {
	sql := "SELECT category, region, SUM(sales) FROM orders GROUP BY ALL"
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.GroupByAll, "GroupByAll should be true")
	assert.Empty(t, core.GroupBy, "GroupBy should be empty when GroupByAll is true")
}

func TestGroupByAllWithHaving(t *testing.T) {
	sql := "SELECT category, COUNT(*) FROM orders GROUP BY ALL HAVING COUNT(*) > 10"
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.GroupByAll, "GroupByAll should be true")
	assert.NotNil(t, core.Having, "HAVING clause should be present")
}

func TestGroupByAllWithQualify(t *testing.T) {
	sql := "SELECT category, SUM(sales), ROW_NUMBER() OVER (ORDER BY SUM(sales) DESC) AS rn FROM orders GROUP BY ALL QUALIFY rn <= 3"
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.GroupByAll, "GroupByAll should be true")
	assert.NotNil(t, core.Qualify, "QUALIFY clause should be present")
}

func TestGroupByRegular(t *testing.T) {
	// Ensure regular GROUP BY still works
	sql := "SELECT category, COUNT(*) FROM orders GROUP BY category"
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.False(t, core.GroupByAll, "GroupByAll should be false for regular GROUP BY")
	assert.Len(t, core.GroupBy, 1, "Should have one GROUP BY expression")
}

// ---------- ORDER BY ALL Tests ----------

func TestOrderByAll(t *testing.T) {
	sql := "SELECT name, age, city FROM users ORDER BY ALL"
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.OrderByAll, "OrderByAll should be true")
	assert.False(t, core.OrderByAllDesc, "OrderByAllDesc should be false (default ASC)")
	assert.Empty(t, core.OrderBy, "OrderBy should be empty when OrderByAll is true")
}

func TestOrderByAllDesc(t *testing.T) {
	sql := "SELECT name, age FROM users ORDER BY ALL DESC"
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.OrderByAll, "OrderByAll should be true")
	assert.True(t, core.OrderByAllDesc, "OrderByAllDesc should be true for DESC")
}

func TestOrderByAllAsc(t *testing.T) {
	sql := "SELECT name, age FROM users ORDER BY ALL ASC"
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.OrderByAll, "OrderByAll should be true")
	assert.False(t, core.OrderByAllDesc, "OrderByAllDesc should be false for ASC")
}

func TestOrderByRegular(t *testing.T) {
	// Ensure regular ORDER BY still works
	sql := "SELECT name, age FROM users ORDER BY name, age DESC"
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.False(t, core.OrderByAll, "OrderByAll should be false for regular ORDER BY")
	assert.Len(t, core.OrderBy, 2, "Should have two ORDER BY items")
}

// ---------- Combined GROUP BY ALL + ORDER BY ALL Tests ----------

func TestCombinedGroupByAndOrderByAll(t *testing.T) {
	sql := `
		SELECT category, region, SUM(sales) as total
		FROM orders
		GROUP BY ALL
		ORDER BY ALL DESC
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.GroupByAll, "GroupByAll should be true")
	assert.True(t, core.OrderByAll, "OrderByAll should be true")
	assert.True(t, core.OrderByAllDesc, "OrderByAllDesc should be true")
}

// ---------- Postgres Dialect Tests ----------

func TestGroupByAllNotInPostgres(t *testing.T) {
	// In Postgres dialect, GROUP BY ALL is not recognized as a special syntax
	// ALL is a reserved keyword, so it causes a parse error
	sql := "SELECT category, SUM(sales) FROM orders GROUP BY ALL"
	_, err := parser.ParseWithDialect(sql, postgres.Postgres)
	// This should error because ALL is a keyword, not an identifier
	assert.Error(t, err, "Postgres dialect should not support GROUP BY ALL")
}

func TestOrderByAllNotInPostgres(t *testing.T) {
	// In Postgres dialect, ORDER BY ALL is not recognized as a special syntax
	// ALL is a reserved keyword, so it causes a parse error
	sql := "SELECT name FROM users ORDER BY ALL"
	_, err := parser.ParseWithDialect(sql, postgres.Postgres)
	// This should error because ALL is a keyword, not an identifier
	assert.Error(t, err, "Postgres dialect should not support ORDER BY ALL")
}

// ---------- Formatter Round-Trip Tests ----------

func TestFormatGroupByAll(t *testing.T) {
	input := "SELECT category, SUM(sales) FROM orders GROUP BY ALL"
	stmt, err := parser.ParseWithDialect(input, duckdb.DuckDB)
	require.NoError(t, err)

	output := format.Format(stmt, duckdb.DuckDB)
	assert.Contains(t, output, "GROUP BY ALL")
	assert.NotContains(t, output, "GROUP BY category")
}

func TestFormatOrderByAllDesc(t *testing.T) {
	input := "SELECT name, age FROM users ORDER BY ALL DESC"
	stmt, err := parser.ParseWithDialect(input, duckdb.DuckDB)
	require.NoError(t, err)

	output := format.Format(stmt, duckdb.DuckDB)
	assert.Contains(t, output, "ORDER BY ALL DESC")
}

func TestFormatOrderByAllAsc(t *testing.T) {
	// ASC is the default, so we don't necessarily print it
	input := "SELECT name, age FROM users ORDER BY ALL ASC"
	stmt, err := parser.ParseWithDialect(input, duckdb.DuckDB)
	require.NoError(t, err)

	output := format.Format(stmt, duckdb.DuckDB)
	assert.Contains(t, output, "ORDER BY ALL")
	// Note: ASC may or may not be printed since it's the default
}

func TestGroupByAllRoundTrip(t *testing.T) {
	input := "SELECT category, region, SUM(sales) FROM orders GROUP BY ALL"
	stmt1, err := parser.ParseWithDialect(input, duckdb.DuckDB)
	require.NoError(t, err)

	formatted := format.Format(stmt1, duckdb.DuckDB)
	stmt2, err := parser.ParseWithDialect(formatted, duckdb.DuckDB)
	require.NoError(t, err)

	// Both should have GroupByAll set
	assert.True(t, stmt1.Body.Left.GroupByAll, "Original should have GroupByAll")
	assert.True(t, stmt2.Body.Left.GroupByAll, "Reparsed should have GroupByAll")
}

func TestOrderByAllRoundTrip(t *testing.T) {
	input := "SELECT name, age FROM users ORDER BY ALL DESC"
	stmt1, err := parser.ParseWithDialect(input, duckdb.DuckDB)
	require.NoError(t, err)

	formatted := format.Format(stmt1, duckdb.DuckDB)
	stmt2, err := parser.ParseWithDialect(formatted, duckdb.DuckDB)
	require.NoError(t, err)

	// Both should have OrderByAll and OrderByAllDesc set
	assert.True(t, stmt1.Body.Left.OrderByAll, "Original should have OrderByAll")
	assert.True(t, stmt1.Body.Left.OrderByAllDesc, "Original should have OrderByAllDesc")
	assert.True(t, stmt2.Body.Left.OrderByAll, "Reparsed should have OrderByAll")
	assert.True(t, stmt2.Body.Left.OrderByAllDesc, "Reparsed should have OrderByAllDesc")
}

func TestCombinedRoundTrip(t *testing.T) {
	input := "SELECT category, SUM(sales) AS total FROM orders GROUP BY ALL ORDER BY ALL DESC"
	stmt1, err := parser.ParseWithDialect(input, duckdb.DuckDB)
	require.NoError(t, err)

	formatted := format.Format(stmt1, duckdb.DuckDB)
	stmt2, err := parser.ParseWithDialect(formatted, duckdb.DuckDB)
	require.NoError(t, err)

	// Both should have all flags set
	assert.True(t, stmt2.Body.Left.GroupByAll, "Reparsed should have GroupByAll")
	assert.True(t, stmt2.Body.Left.OrderByAll, "Reparsed should have OrderByAll")
	assert.True(t, stmt2.Body.Left.OrderByAllDesc, "Reparsed should have OrderByAllDesc")
}

// ---------- Integration Tests ----------

func TestGroupByAllWithComplexQuery(t *testing.T) {
	sql := `
		WITH sales_data AS (
			SELECT * FROM raw_sales
		)
		SELECT 
			category,
			region,
			year,
			SUM(amount) AS total,
			AVG(amount) AS avg_amount
		FROM sales_data
		WHERE year >= 2020
		GROUP BY ALL
		HAVING SUM(amount) > 1000
		ORDER BY total DESC
		LIMIT 100
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.GroupByAll, "GroupByAll should be true")
	assert.NotNil(t, core.Having, "HAVING should be present")
	assert.NotEmpty(t, core.OrderBy, "Regular ORDER BY should be present")
	assert.NotNil(t, core.Limit, "LIMIT should be present")
}

func TestOrderByAllWithComplexQuery(t *testing.T) {
	sql := `
		SELECT 
			department,
			employee_name,
			salary
		FROM employees
		WHERE department IS NOT NULL
		ORDER BY ALL
		LIMIT 50
		OFFSET 10
	`
	stmt, err := parser.ParseWithDialect(sql, duckdb.DuckDB)
	require.NoError(t, err)

	core := stmt.Body.Left
	assert.True(t, core.OrderByAll, "OrderByAll should be true")
	assert.NotNil(t, core.Limit, "LIMIT should be present")
	assert.NotNil(t, core.Offset, "OFFSET should be present")
}
