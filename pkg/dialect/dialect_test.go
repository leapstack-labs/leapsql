package dialect

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLineageTypeString(t *testing.T) {
	tests := []struct {
		lineageType Type
		want        string
	}{
		{LineagePassthrough, "passthrough"},
		{LineageAggregate, "aggregate"},
		{LineageGenerator, "generator"},
		{LineageWindow, "window"},
		{LineageTable, "table"},
		{Type(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.lineageType.String())
		})
	}
}

func TestIsTableFunction(t *testing.T) {
	d := NewDialect("test").
		TableFunctions("read_csv", "generate_series", "read_parquet").
		Build()

	tests := []struct {
		name string
		want bool
	}{
		{"read_csv", true},
		{"READ_CSV", true}, // case insensitive
		{"generate_series", true},
		{"read_parquet", true},
		{"unknown_func", false},
		{"sum", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, d.IsTableFunction(tt.name))
		})
	}
}

func TestFunctionLineageType_TableFunctionPriority(t *testing.T) {
	// Table functions should have highest priority
	d := NewDialect("test").
		Aggregates("sum").
		TableFunctions("sum"). // Same function as both - table should win
		Build()

	assert.Equal(t, LineageTable, d.FunctionLineageType("sum"))
}

func TestGetDoc(t *testing.T) {
	docs := map[string]FunctionDoc{
		"COUNT": {
			Description: "Count non-null values",
			Signatures:  []string{"count(expr) -> BIGINT", "count(*) -> BIGINT"},
			ReturnType:  "BIGINT",
			Example:     "SELECT COUNT(*) FROM users",
		},
		"SUM": {
			Description: "Sum of values",
			Signatures:  []string{"sum(expr) -> NUMERIC"},
			ReturnType:  "NUMERIC",
		},
	}

	d := NewDialect("test").
		WithDocs(docs).
		Build()

	t.Run("existing function", func(t *testing.T) {
		doc, ok := d.GetDoc("COUNT")
		require.True(t, ok)
		assert.Equal(t, "Count non-null values", doc.Description)
		assert.Len(t, doc.Signatures, 2)
		assert.Equal(t, "BIGINT", doc.ReturnType)
		assert.Equal(t, "SELECT COUNT(*) FROM users", doc.Example)
	})

	t.Run("case insensitive", func(t *testing.T) {
		doc, ok := d.GetDoc("count")
		require.True(t, ok)
		assert.Equal(t, "Count non-null values", doc.Description)
	})

	t.Run("non-existent function", func(t *testing.T) {
		_, ok := d.GetDoc("nonexistent")
		assert.False(t, ok)
	})
}

func TestAllFunctions(t *testing.T) {
	d := NewDialect("test").
		Aggregates("sum", "count", "avg").
		Generators("now", "uuid").
		Windows("row_number", "rank").
		TableFunctions("read_csv", "generate_series").
		WithDocs(map[string]FunctionDoc{
			"coalesce": {Description: "Return first non-null value"}, // Not in any category
		}).
		Build()

	funcs := d.AllFunctions()

	// Should include all functions from all categories
	expected := []string{"sum", "count", "avg", "now", "uuid", "row_number", "rank", "read_csv", "generate_series", "coalesce"}
	sort.Strings(expected)
	sort.Strings(funcs)

	assert.Equal(t, expected, funcs)
}

func TestKeywords(t *testing.T) {
	d := NewDialect("test").
		WithKeywords("SELECT", "FROM", "WHERE", "JOIN").
		Build()

	keywords := d.Keywords()
	sort.Strings(keywords)

	// Keywords are normalized to lowercase
	assert.Equal(t, []string{"from", "join", "select", "where"}, keywords)
}

func TestDataTypes(t *testing.T) {
	d := NewDialect("test").
		WithDataTypes("BIGINT", "VARCHAR", "BOOLEAN", "DATE").
		Build()

	types := d.DataTypes()

	assert.Equal(t, []string{"BIGINT", "VARCHAR", "BOOLEAN", "DATE"}, types)
}

func TestBuilderChaining(t *testing.T) {
	// Test that all builder methods can be chained
	d := NewDialect("test").
		Identifiers(`"`, `"`, `""`, NormCaseInsensitive).
		Operators(true, true).
		Aggregates("sum", "count").
		Generators("now").
		Windows("row_number").
		TableFunctions("read_csv").
		WithDocs(map[string]FunctionDoc{"sum": {Description: "Sum"}}).
		WithKeywords("SELECT", "FROM").
		WithDataTypes("INTEGER", "VARCHAR").
		Build()

	require.NotNil(t, d)
	assert.Equal(t, "test", d.Name)
	assert.True(t, d.IsAggregate("sum"))
	assert.True(t, d.IsGenerator("now"))
	assert.True(t, d.IsWindow("row_number"))
	assert.True(t, d.IsTableFunction("read_csv"))

	doc, ok := d.GetDoc("sum")
	assert.True(t, ok)
	assert.Equal(t, "Sum", doc.Description)
}

func TestNormalizationStrategies(t *testing.T) {
	tests := []struct {
		name  string
		norm  NormalizationStrategy
		input string
		want  string
	}{
		{"lowercase", NormLowercase, "FooBar", "foobar"},
		{"uppercase", NormUppercase, "FooBar", "FOOBAR"},
		{"case sensitive", NormCaseSensitive, "FooBar", "FooBar"},
		{"case insensitive", NormCaseInsensitive, "FooBar", "foobar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDialect("test").
				Identifiers(`"`, `"`, `""`, tt.norm).
				Build()

			assert.Equal(t, tt.want, d.NormalizeName(tt.input))
		})
	}
}
