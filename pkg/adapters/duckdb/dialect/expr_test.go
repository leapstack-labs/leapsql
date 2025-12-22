package dialect_test

import (
	"testing"

	duckdbDialect "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb/dialect"
	"github.com/leapstack-labs/leapsql/pkg/format"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- List Literal Tests ----------

func TestListLiteral(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantElements int
	}{
		{
			name:         "empty list",
			sql:          "SELECT []",
			wantElements: 0,
		},
		{
			name:         "single element",
			sql:          "SELECT [1]",
			wantElements: 1,
		},
		{
			name:         "multiple integers",
			sql:          "SELECT [1, 2, 3]",
			wantElements: 3,
		},
		{
			name:         "strings",
			sql:          "SELECT ['a', 'b', 'c']",
			wantElements: 3,
		},
		{
			name:         "mixed expressions",
			sql:          "SELECT [1 + 2, 3 * 4, 5]",
			wantElements: 3,
		},
		{
			name:         "with column reference",
			sql:          "SELECT [id, name] FROM users",
			wantElements: 2,
		},
		{
			name:         "nested lists",
			sql:          "SELECT [[1, 2], [3, 4]]",
			wantElements: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err, "Failed to parse: %s", tt.sql)
			require.NotNil(t, stmt.Body)
			require.NotNil(t, stmt.Body.Left)
			require.NotEmpty(t, stmt.Body.Left.Columns)

			expr := stmt.Body.Left.Columns[0].Expr
			list, ok := expr.(*parser.ListLiteral)
			require.True(t, ok, "Expected ListLiteral, got %T", expr)
			assert.Len(t, list.Elements, tt.wantElements)
		})
	}
}

// ---------- Struct Literal Tests ----------

func TestStructLiteral(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantFields int
		wantKeys   []string
	}{
		{
			name:       "single field",
			sql:        "SELECT {'name': 'Alice'}",
			wantFields: 1,
			wantKeys:   []string{"name"},
		},
		{
			name:       "multiple fields",
			sql:        "SELECT {'name': 'Alice', 'age': 30}",
			wantFields: 2,
			wantKeys:   []string{"name", "age"},
		},
		{
			name:       "with expressions",
			sql:        "SELECT {'total': 1 + 2, 'doubled': x * 2} FROM t",
			wantFields: 2,
			wantKeys:   []string{"total", "doubled"},
		},
		{
			name:       "nested struct",
			sql:        "SELECT {'person': {'name': 'Bob', 'age': 25}}",
			wantFields: 1,
			wantKeys:   []string{"person"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err, "Failed to parse: %s", tt.sql)

			expr := stmt.Body.Left.Columns[0].Expr
			s, ok := expr.(*parser.StructLiteral)
			require.True(t, ok, "Expected StructLiteral, got %T", expr)
			assert.Len(t, s.Fields, tt.wantFields)

			for i, key := range tt.wantKeys {
				assert.Equal(t, key, s.Fields[i].Key, "Field %d key mismatch", i)
			}
		})
	}
}

// ---------- Lambda Expression Tests ----------

func TestLambdaExpr(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantParams []string
	}{
		{
			name:       "single param simple",
			sql:        "SELECT list_transform([1, 2, 3], x -> x * 2)",
			wantParams: []string{"x"},
		},
		{
			name:       "single param with function",
			sql:        "SELECT list_filter(arr, x -> x > 0) FROM t",
			wantParams: []string{"x"},
		},
		{
			name:       "two params",
			sql:        "SELECT list_reduce([1, 2, 3], (x, y) -> x + y)",
			wantParams: []string{"x", "y"},
		},
		{
			name:       "lambda with comparison",
			sql:        "SELECT list_filter(nums, n -> n >= 10)",
			wantParams: []string{"n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err, "Failed to parse: %s", tt.sql)

			// The lambda is typically the second argument to a function
			funcCall, ok := stmt.Body.Left.Columns[0].Expr.(*parser.FuncCall)
			require.True(t, ok, "Expected FuncCall, got %T", stmt.Body.Left.Columns[0].Expr)
			require.GreaterOrEqual(t, len(funcCall.Args), 2, "Expected at least 2 args")

			lambda, ok := funcCall.Args[1].(*parser.LambdaExpr)
			require.True(t, ok, "Second arg should be LambdaExpr, got %T", funcCall.Args[1])
			assert.Equal(t, tt.wantParams, lambda.Params)
		})
	}
}

// ---------- Index Expression Tests ----------

func TestIndexExpr(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		isSlice  bool
		hasStart bool
		hasEnd   bool
		hasIndex bool
	}{
		{
			name:     "simple index",
			sql:      "SELECT arr[1] FROM t",
			isSlice:  false,
			hasIndex: true,
		},
		{
			name:     "index with expression",
			sql:      "SELECT arr[i + 1] FROM t",
			isSlice:  false,
			hasIndex: true,
		},
		{
			name:     "full slice",
			sql:      "SELECT arr[1:3] FROM t",
			isSlice:  true,
			hasStart: true,
			hasEnd:   true,
		},
		{
			name:     "slice from start",
			sql:      "SELECT arr[:3] FROM t",
			isSlice:  true,
			hasStart: false,
			hasEnd:   true,
		},
		{
			name:     "slice to end",
			sql:      "SELECT arr[2:] FROM t",
			isSlice:  true,
			hasStart: true,
			hasEnd:   false,
		},
		{
			name:     "negative index",
			sql:      "SELECT arr[-1] FROM t",
			isSlice:  false,
			hasIndex: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err, "Failed to parse: %s", tt.sql)

			expr := stmt.Body.Left.Columns[0].Expr
			idx, ok := expr.(*parser.IndexExpr)
			require.True(t, ok, "Expected IndexExpr, got %T", expr)

			assert.Equal(t, tt.isSlice, idx.IsSlice, "IsSlice mismatch")

			if tt.isSlice {
				assert.Equal(t, tt.hasStart, idx.Start != nil, "hasStart mismatch")
				assert.Equal(t, tt.hasEnd, idx.End != nil, "hasEnd mismatch")
			} else {
				assert.Equal(t, tt.hasIndex, idx.Index != nil, "hasIndex mismatch")
			}
		})
	}
}

// ---------- Nested Expression Tests ----------

func TestNestedExpressions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "list inside struct",
			sql:  "SELECT {'items': [1, 2, 3], 'count': 3}",
		},
		{
			name: "struct inside list",
			sql:  "SELECT [{'a': 1}, {'b': 2}]",
		},
		{
			name: "index on list literal",
			sql:  "SELECT [1, 2, 3][0]",
		},
		{
			name: "lambda with list and struct",
			sql:  "SELECT list_transform([1, 2], x -> {'value': x})",
		},
		{
			name: "deeply nested",
			sql:  "SELECT {'data': [{'nested': [1, 2, 3]}]}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err, "Failed to parse: %s", tt.sql)
			require.NotNil(t, stmt.Body.Left.Columns[0].Expr)
		})
	}
}

// ---------- Round Trip Tests (Parse -> Format -> Parse) ----------

func TestExpressionRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "list literal",
			sql:  "SELECT [1, 2, 3]",
		},
		{
			name: "empty list",
			sql:  "SELECT []",
		},
		{
			name: "struct literal",
			sql:  "SELECT {'name': 'Alice', 'age': 30}",
		},
		{
			name: "array index",
			sql:  "SELECT arr[1] FROM t",
		},
		{
			name: "array slice",
			sql:  "SELECT arr[1:3] FROM t",
		},
		{
			name: "lambda single param",
			sql:  "SELECT list_transform([1, 2], x -> x * 2)",
		},
		{
			name: "lambda two params",
			sql:  "SELECT list_reduce([1, 2], (a, b) -> a + b)",
		},
		{
			name: "complex nested",
			sql:  "SELECT {'items': [1, 2], 'total': 3}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse original
			stmt1, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err, "Initial parse failed")

			// Format
			formatted := format.Format(stmt1, duckdbDialect.DuckDB)
			require.NotEmpty(t, formatted, "Format produced empty output")

			// Parse formatted output
			stmt2, err := parser.ParseWithDialect(formatted, duckdbDialect.DuckDB)
			require.NoError(t, err, "Re-parse failed for: %s", formatted)

			// Both should have the same number of columns
			require.Len(t, stmt2.Body.Left.Columns, len(stmt1.Body.Left.Columns))
		})
	}
}

// ---------- Integration Tests ----------

func TestExpressionsInContext(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "list in WHERE",
			sql:  "SELECT * FROM t WHERE id IN ([1, 2, 3])",
		},
		{
			name: "struct in SELECT with alias",
			sql:  "SELECT {'x': 1, 'y': 2} AS point",
		},
		{
			name: "list with function",
			sql:  "SELECT list_sum([1, 2, 3])",
		},
		{
			name: "array access in expression",
			sql:  "SELECT arr[0] + arr[1] AS sum FROM t",
		},
		{
			name: "lambda with filter and transform",
			sql:  "SELECT list_transform(list_filter([1, 2, 3, 4], x -> x > 2), y -> y * 10)",
		},
		{
			name: "struct access equivalent",
			sql:  "SELECT {'a': 1, 'b': 2} AS data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			require.NoError(t, err, "Failed to parse: %s", tt.sql)
			require.NotNil(t, stmt)
		})
	}
}

// ---------- Error Cases ----------

func TestExpressionErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "unclosed list",
			sql:  "SELECT [1, 2, 3",
		},
		{
			name: "unclosed struct",
			sql:  "SELECT {'a': 1",
		},
		{
			name: "missing colon in struct",
			sql:  "SELECT {'a' 1}",
		},
		{
			name: "unclosed index",
			sql:  "SELECT arr[1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.ParseWithDialect(tt.sql, duckdbDialect.DuckDB)
			assert.Error(t, err, "Expected parse error for: %s", tt.sql)
		})
	}
}
