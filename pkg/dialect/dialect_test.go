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

func TestFormatPlaceholder(t *testing.T) {
	tests := []struct {
		name  string
		style PlaceholderStyle
		index int
		want  string
	}{
		{"question style 1", PlaceholderQuestion, 1, "?"},
		{"question style 2", PlaceholderQuestion, 2, "?"},
		{"dollar style 1", PlaceholderDollar, 1, "$1"},
		{"dollar style 2", PlaceholderDollar, 2, "$2"},
		{"dollar style 10", PlaceholderDollar, 10, "$10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDialect("test").
				PlaceholderStyle(tt.style).
				Build()

			assert.Equal(t, tt.want, d.FormatPlaceholder(tt.index))
		})
	}
}

func TestDefaultSchema(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"duckdb default", "main"},
		{"postgres default", "public"},
		{"custom schema", "myschema"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDialect("test").
				DefaultSchema(tt.schema).
				Build()

			assert.Equal(t, tt.schema, d.DefaultSchema)
		})
	}
}

func TestIsReservedWord(t *testing.T) {
	d := NewDialect("test").
		WithReservedWords("SELECT", "FROM", "WHERE", "user", "ORDER").
		Build()

	tests := []struct {
		word string
		want bool
	}{
		{"SELECT", true},
		{"select", true}, // case insensitive
		{"FROM", true},
		{"user", true},
		{"ORDER", true},
		{"foo", false},
		{"mycolumn", false},
	}

	for _, tt := range tests {
		t.Run(tt.word, func(t *testing.T) {
			assert.Equal(t, tt.want, d.IsReservedWord(tt.word))
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		quote    string
		quoteEnd string
		escape   string
		input    string
		want     string
	}{
		{"double quote simple", `"`, `"`, `""`, "users", `"users"`},
		{"double quote with quote", `"`, `"`, `""`, `foo"bar`, `"foo""bar"`},
		{"backtick simple", "`", "`", "``", "users", "`users`"},
		{"bracket simple", "[", "]", "]]", "users", "[users]"},
		{"bracket with bracket", "[", "]", "]]", "foo]bar", "[foo]]bar]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDialect("test").
				Identifiers(tt.quote, tt.quoteEnd, tt.escape, NormLowercase).
				Build()

			assert.Equal(t, tt.want, d.QuoteIdentifier(tt.input))
		})
	}
}

func TestQuoteIdentifierIfNeeded(t *testing.T) {
	d := NewDialect("test").
		Identifiers(`"`, `"`, `""`, NormLowercase).
		WithReservedWords("SELECT", "user", "ORDER").
		Build()

	tests := []struct {
		name string
		want string
	}{
		{"users", "users"},       // not reserved
		{"user", `"user"`},       // reserved
		{"USER", `"USER"`},       // reserved (case insensitive)
		{"select", `"select"`},   // reserved
		{"mycolumn", "mycolumn"}, // not reserved
		{"order", `"order"`},     // reserved
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, d.QuoteIdentifierIfNeeded(tt.name))
		})
	}
}

func TestBuilderWithAllNewMethods(t *testing.T) {
	// Test that all new builder methods can be chained
	d := NewDialect("postgres").
		Identifiers(`"`, `"`, `""`, NormLowercase).
		Operators(true, false).
		DefaultSchema("public").
		PlaceholderStyle(PlaceholderDollar).
		Aggregates("sum", "count").
		Generators("now").
		Windows("row_number").
		TableFunctions("generate_series").
		WithKeywords("SELECT", "FROM").
		WithReservedWords("user", "order", "table").
		WithDataTypes("INTEGER", "VARCHAR").
		Build()

	require.NotNil(t, d)
	assert.Equal(t, "postgres", d.Name)
	assert.Equal(t, "public", d.DefaultSchema)
	assert.Equal(t, PlaceholderDollar, d.Placeholder)
	assert.Equal(t, "$1", d.FormatPlaceholder(1))
	assert.True(t, d.IsReservedWord("user"))
	assert.True(t, d.IsReservedWord("ORDER"))
	assert.False(t, d.IsReservedWord("foo"))
}
