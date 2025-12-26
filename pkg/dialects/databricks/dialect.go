// Package databricks provides the Databricks SQL dialect definition.
// This package is pure Go with no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package databricks

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

//go:generate go run ../../../scripts/gendatabricks -gen=all -outdir=.

func init() {
	dialect.Register(Databricks)
}

// Databricks-specific tokens
// Standard tokens (QUALIFY, ILIKE, SEMI, ANTI, DCOLON) use builtin token constants
var (
	// TokenRlike is regex match (RLIKE)
	TokenRlike = token.Register("RLIKE")
	// TokenRegexp is regex match (REGEXP alias)
	TokenRegexp = token.Register("REGEXP")
	// TokenColon is the : JSON path operator
	TokenColon = token.Register(":")
	// TokenQDcolon is the ?:: try-cast operator
	TokenQDcolon = token.Register("?::")
	// TokenDiv is integer division
	TokenDiv = token.Register("DIV")

	// LATERAL VIEW tokens
	TokenLateral = token.Register("LATERAL")
	TokenView    = token.Register("VIEW")
	TokenOuter   = token.Register("OUTER")

	// TABLESAMPLE tokens
	TokenTablesample = token.Register("TABLESAMPLE")
	TokenBucket      = token.Register("BUCKET")
	TokenPercent     = token.Register("PERCENT")
	TokenRows        = token.Register("ROWS")
)

// --- Databricks-specific Clause Definitions ---

// DatabricksQualify is the QUALIFY clause for window function filtering.
var DatabricksQualify = dialect.ClauseDef{
	Token:   token.QUALIFY,
	Handler: parseQualify,
	Slot:    spi.SlotQualify,
}

// parseQualify handles the QUALIFY clause (Databricks/Spark-specific).
// The QUALIFY keyword has already been consumed.
func parseQualify(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// --- Databricks-specific Operators ---

var databricksOperators = []dialect.OperatorDef{
	{Token: token.ILIKE, Precedence: spi.PrecedenceComparison},
	{Token: TokenRlike, Precedence: spi.PrecedenceComparison},
	{Token: TokenRegexp, Precedence: spi.PrecedenceComparison},
	{Token: token.DCOLON, Symbol: "::", Precedence: spi.PrecedencePostfix},
	{Token: TokenColon, Symbol: ":", Precedence: spi.PrecedencePostfix},
	{Token: TokenQDcolon, Symbol: "?::", Precedence: spi.PrecedencePostfix},
	{Token: TokenDiv, Symbol: "DIV", Precedence: spi.PrecedenceMultiply},
}

// --- Databricks-specific Join Types ---

var databricksJoinTypes = []dialect.JoinTypeDef{
	{
		Token:       token.SEMI,
		Type:        JoinSemi,
		RequiresOn:  true,
		AllowsUsing: true,
	},
	{
		Token:       token.ANTI,
		Type:        JoinAnti,
		RequiresOn:  true,
		AllowsUsing: true,
	},
}

// Join type constants for Databricks-specific joins.
const (
	JoinSemi = "SEMI"
	JoinAnti = "ANTI"
)

// Databricks is the Databricks SQL dialect configuration.
var Databricks = dialect.NewDialect("databricks").
	// Static Configuration
	// Databricks uses backticks for identifier quoting
	Identifiers("`", "`", "``", core.NormCaseInsensitive).
	DefaultSchema("default").
	PlaceholderStyle(core.PlaceholderQuestion).
	// Register Databricks-specific keywords for the lexer
	AddKeyword("QUALIFY", token.QUALIFY).
	AddKeyword("ILIKE", token.ILIKE).
	AddKeyword("RLIKE", TokenRlike).
	AddKeyword("REGEXP", TokenRegexp).
	AddKeyword("DIV", TokenDiv).
	AddKeyword("SEMI", token.SEMI).
	AddKeyword("ANTI", token.ANTI).
	AddKeyword("LATERAL", TokenLateral).
	AddKeyword("VIEW", TokenView).
	AddKeyword("OUTER", TokenOuter).
	AddKeyword("TABLESAMPLE", TokenTablesample).
	AddKeyword("BUCKET", TokenBucket).
	AddKeyword("PERCENT", TokenPercent).
	AddKeyword("ROWS", TokenRows).
	// Clause Sequence
	Clauses(
		dialect.StandardWhere,
		dialect.StandardGroupBy,
		dialect.StandardHaving,
		DatabricksQualify, // Databricks supports QUALIFY
		dialect.StandardWindow,
		dialect.StandardOrderBy,
		dialect.StandardLimit,
		dialect.StandardOffset,
		dialect.StandardFetch,
	).
	// Operators - compose from standard + custom
	Operators(
		dialect.ANSIOperators,
		databricksOperators,
	).
	// Join Types - compose from standard + custom
	JoinTypes(
		dialect.ANSIJoinTypes,
		databricksJoinTypes,
	).
	// Function classifications (from generated files)
	Aggregates(databricksAggregates...).
	Generators(databricksGenerators...).
	Windows(databricksWindows...).
	TableFunctions(databricksTableFunctions...).
	WithDocs(databricksFunctionDocs).
	WithKeywords(databricksCompletionKeywords...).
	WithReservedWords(databricksReservedWords...).
	WithDataTypes(databricksTypes...).
	Build()
