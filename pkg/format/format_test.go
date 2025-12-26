package format

import (
	"testing"

	duckdbdialect "github.com/leapstack-labs/leapsql/pkg/dialects/duckdb"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormat_BasicSelect(t *testing.T) {
	d := duckdbdialect.DuckDB
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:  "simple select",
			input: "SELECT a, b FROM t",
			expected: `SELECT
  a,
  b
FROM t
`,
		},
		{
			name:  "select with where",
			input: "SELECT a FROM t WHERE x = 1",
			expected: `SELECT
  a
FROM t
WHERE
  x = 1
`,
		},
		{
			name:  "select with alias",
			input: "SELECT a AS col1, b AS col2 FROM t",
			expected: `SELECT
  a AS col1,
  b AS col2
FROM t
`,
		},
		{
			name:  "select star",
			input: "SELECT * FROM t",
			expected: `SELECT
  *
FROM t
`,
		},
		{
			name:  "select table star",
			input: "SELECT t.* FROM t",
			expected: `SELECT
  t.*
FROM t
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.input, d)
			require.NoError(t, err)

			result := Format(stmt, d)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormat_Joins(t *testing.T) {
	d := duckdbdialect.DuckDB
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:  "inner join",
			input: "SELECT * FROM a JOIN b ON a.id = b.id",
			expected: `SELECT
  *
FROM a
JOIN b
  ON a.id = b.id
`,
		},
		{
			name:  "left join",
			input: "SELECT * FROM a LEFT JOIN b ON a.id = b.id",
			expected: `SELECT
  *
FROM a
LEFT JOIN b
  ON a.id = b.id
`,
		},
		{
			name:  "cross join",
			input: "SELECT * FROM a CROSS JOIN b",
			expected: `SELECT
  *
FROM a
CROSS JOIN b
`,
		},
		{
			name:  "multiple joins",
			input: "SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id",
			expected: `SELECT
  *
FROM a
JOIN b
  ON a.id = b.id
LEFT JOIN c
  ON b.id = c.id
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.input, d)
			require.NoError(t, err)

			result := Format(stmt, d)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormat_CTE(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "WITH cte AS (SELECT a FROM t) SELECT * FROM cte"
	expected := `WITH
  cte AS (
    SELECT
      a
    FROM t
  )
SELECT
  *
FROM cte
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}

func TestFormat_Expressions(t *testing.T) {
	d := duckdbdialect.DuckDB
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:  "case expression",
			input: "SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END FROM t",
			expected: `SELECT
  CASE
    WHEN x = 1 THEN 'a'
    ELSE 'b'
  END
FROM t
`,
		},
		{
			name:  "function call",
			input: "SELECT COUNT(*) FROM t",
			expected: `SELECT
  COUNT(*)
FROM t
`,
		},
		{
			name:  "function with args",
			input: "SELECT COALESCE(a, b, c) FROM t",
			expected: `SELECT
  COALESCE(a, b, c)
FROM t
`,
		},
		{
			name:  "in expression",
			input: "SELECT * FROM t WHERE x IN (1, 2, 3)",
			expected: `SELECT
  *
FROM t
WHERE
  x IN (1, 2, 3)
`,
		},
		{
			name:  "between expression",
			input: "SELECT * FROM t WHERE x BETWEEN 1 AND 10",
			expected: `SELECT
  *
FROM t
WHERE
  x BETWEEN 1 AND 10
`,
		},
		{
			name:  "like expression",
			input: "SELECT * FROM t WHERE name LIKE '%test%'",
			expected: `SELECT
  *
FROM t
WHERE
  name LIKE '%test%'
`,
		},
		{
			name:  "is null",
			input: "SELECT * FROM t WHERE x IS NULL",
			expected: `SELECT
  *
FROM t
WHERE
  x IS NULL
`,
		},
		{
			name:  "is not null",
			input: "SELECT * FROM t WHERE x IS NOT NULL",
			expected: `SELECT
  *
FROM t
WHERE
  x IS NOT NULL
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.input, d)
			require.NoError(t, err)

			result := Format(stmt, d)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormat_GroupByOrderBy(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY a DESC"
	expected := `SELECT
  a,
  COUNT(*)
FROM t
GROUP BY
  a
ORDER BY
  a DESC
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}

func TestFormat_LimitOffset(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "SELECT * FROM t LIMIT 10 OFFSET 5"
	expected := `SELECT
  *
FROM t
LIMIT 10
OFFSET 5
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}

func TestFormat_Union(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "SELECT a FROM t1 UNION SELECT b FROM t2"
	expected := `SELECT
  a
FROM t1
UNION
SELECT
  b
FROM t2
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}

func TestFormat_Subquery(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "SELECT * FROM (SELECT a FROM t) AS sub"
	expected := `SELECT
  *
FROM (
  SELECT
    a
  FROM t
) sub
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}

func TestFormat_MacroExpression(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "SELECT * FROM {{ ref('stg_users') }}"
	expected := `SELECT
  *
FROM {{ ref('stg_users') }}
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}

func TestFormat_WindowFunction(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC) FROM t"
	expected := `SELECT
  id,
  ROW_NUMBER() OVER (
    PARTITION BY region
    ORDER BY sales DESC)
FROM t
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}

func TestFormatSQL_Integration(t *testing.T) {
	d := duckdbdialect.DuckDB

	// Test the one-call API
	input := "select id,   sum(val) from   my_table where active=true"
	result, err := SQL(input, d)
	require.NoError(t, err)

	expected := `SELECT
  id,
  SUM(val)
FROM my_table
WHERE
  active = TRUE
`

	assert.Equal(t, expected, result)
}

func TestFormat_CommentPreservation(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := `-- Leading comment
SELECT id FROM users`

	result, err := SQL(input, d)
	require.NoError(t, err)

	// Comment should be preserved in output
	assert.Contains(t, result, "-- Leading comment")
}

func TestFormat_Cast(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "SELECT CAST(x AS INT) FROM t"
	expected := `SELECT
  CAST(x AS INT)
FROM t
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}

func TestFormat_Exists(t *testing.T) {
	d := duckdbdialect.DuckDB

	input := "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM other)"
	expected := `SELECT
  *
FROM t
WHERE
  EXISTS (
    SELECT
      1
    FROM other
  )
`

	stmt, err := parser.ParseWithDialect(input, d)
	require.NoError(t, err)

	result := Format(stmt, d)
	assert.Equal(t, expected, result)
}
