// Package dialect provides the Databricks SQL dialect definition.
// This package is lightweight and has no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

//go:generate go run ../../../../scripts/gendatabricks -gen=all -outdir=.

func init() {
	dialect.Register(Databricks)
}

// Databricks-specific tokens (registered dynamically)
var (
	// TokenQualify is the QUALIFY clause for filtering window functions
	TokenQualify = token.Register("QUALIFY")
	// TokenIlike is case-insensitive LIKE
	TokenIlike = token.Register("ILIKE")
	// TokenRlike is regex match (RLIKE)
	TokenRlike = token.Register("RLIKE")
	// TokenRegexp is regex match (REGEXP alias)
	TokenRegexp = token.Register("REGEXP")
	// TokenDcolon is the :: cast operator
	TokenDcolon = token.Register("::")
	// TokenColon is the : JSON path operator
	TokenColon = token.Register(":")
	// TokenQDcolon is the ?:: try-cast operator
	TokenQDcolon = token.Register("?::")
	// TokenDiv is integer division
	TokenDiv = token.Register("DIV")

	// Join type tokens
	TokenSemi = token.Register("SEMI")
	TokenAnti = token.Register("ANTI")

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
	Token:   TokenQualify,
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
	{Token: TokenIlike, Precedence: spi.PrecedenceComparison},
	{Token: TokenRlike, Precedence: spi.PrecedenceComparison},
	{Token: TokenRegexp, Precedence: spi.PrecedenceComparison},
	{Token: TokenDcolon, Symbol: "::", Precedence: spi.PrecedencePostfix},
	{Token: TokenColon, Symbol: ":", Precedence: spi.PrecedencePostfix},
	{Token: TokenQDcolon, Symbol: "?::", Precedence: spi.PrecedencePostfix},
	{Token: TokenDiv, Symbol: "DIV", Precedence: spi.PrecedenceMultiply},
}

// --- Databricks-specific Join Types ---

var databricksJoinTypes = []dialect.JoinTypeDef{
	{
		Token:       TokenSemi,
		Type:        JoinSemi,
		RequiresOn:  true,
		AllowsUsing: true,
	},
	{
		Token:       TokenAnti,
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
	AddKeyword("QUALIFY", TokenQualify).
	AddKeyword("ILIKE", TokenIlike).
	AddKeyword("RLIKE", TokenRlike).
	AddKeyword("REGEXP", TokenRegexp).
	AddKeyword("DIV", TokenDiv).
	AddKeyword("SEMI", TokenSemi).
	AddKeyword("ANTI", TokenAnti).
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
