package snowflake

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuild(t *testing.T) {
	d := Snowflake

	require.NotNil(t, d)

	// Verify dialect properties
	assert.Equal(t, "snowflake", d.Name)
	assert.Equal(t, `"`, d.Identifiers.Quote)
	assert.Equal(t, "PUBLIC", d.DefaultSchema)

	// Verify features via Config flags
	assert.True(t, Config.SupportsQualify)
	assert.True(t, Config.SupportsIlike)
	assert.True(t, Config.SupportsCastOperator)
	assert.False(t, Config.SupportsGroupByAll)
	assert.False(t, Config.SupportsOrderByAll)
	assert.False(t, Config.SupportsSemiAntiJoins)
}

func TestDialectRegistration(t *testing.T) {
	// Verify the Snowflake dialect is registered and can be retrieved
	d, ok := dialect.Get("snowflake")
	require.True(t, ok, "snowflake dialect should be registered")
	require.NotNil(t, d)
	assert.Equal(t, "snowflake", d.Name)
}

func TestFunctionClassifications(t *testing.T) {
	d := Snowflake

	// Verify aggregates
	assert.True(t, d.IsAggregate("sum"), "SUM should be an aggregate")
	assert.True(t, d.IsAggregate("count"), "COUNT should be an aggregate")
	assert.True(t, d.IsAggregate("avg"), "AVG should be an aggregate")

	// Verify windows
	assert.True(t, d.IsWindow("row_number"), "ROW_NUMBER should be a window function")
	assert.True(t, d.IsWindow("rank"), "RANK should be a window function")
	assert.True(t, d.IsWindow("lag"), "LAG should be a window function")
	assert.True(t, d.IsWindow("lead"), "LEAD should be a window function")

	// Verify generators
	assert.True(t, d.IsGenerator("current_date"), "CURRENT_DATE should be a generator")
	assert.True(t, d.IsGenerator("current_timestamp"), "CURRENT_TIMESTAMP should be a generator")
	assert.True(t, d.IsGenerator("random"), "RANDOM should be a generator")
}

func TestIdentifierQuoting(t *testing.T) {
	d := Snowflake

	// Snowflake uses double quotes for identifier quoting
	assert.Equal(t, `"my_table"`, d.QuoteIdentifier("my_table"))
	assert.Equal(t, `"MY_TABLE"`, d.QuoteIdentifier("MY_TABLE"))
	// Test escaping embedded quotes
	assert.Equal(t, `"table""name"`, d.QuoteIdentifier(`table"name`))
}

func TestReservedWords(t *testing.T) {
	d := Snowflake

	// Check some reserved words
	assert.True(t, d.IsReservedWord("SELECT"))
	assert.True(t, d.IsReservedWord("FROM"))
	assert.True(t, d.IsReservedWord("WHERE"))
	assert.True(t, d.IsReservedWord("QUALIFY"))

	// Non-reserved words
	assert.False(t, d.IsReservedWord("my_column"))
}

func TestNormalization(t *testing.T) {
	d := Snowflake

	// Snowflake normalizes to uppercase
	assert.Equal(t, "MY_TABLE", d.NormalizeName("my_table"))
	assert.Equal(t, "MY_TABLE", d.NormalizeName("MY_TABLE"))
	assert.Equal(t, "MY_TABLE", d.NormalizeName("My_Table"))
}
