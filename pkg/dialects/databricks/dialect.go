// Package databricks provides the Databricks SQL dialect definition.
// This package is pure Go with no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package databricks

import (
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

//go:generate go run ../../../scripts/gendatabricks -gen=all -outdir=.

func init() {
	dialect.Register(Databricks)
}

// Databricks-specific tokens (edge features not auto-wired by framework)
// Standard tokens (QUALIFY, ILIKE, SEMI, ANTI, DCOLON) are auto-wired via Config flags
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

// Join type constants for Databricks-specific joins.
const (
	JoinSemi = "SEMI"
	JoinAnti = "ANTI"
)

// --- Databricks-specific Operators ---

var databricksOperators = []dialect.OperatorDef{
	{Token: TokenRlike, Precedence: spi.PrecedenceComparison},
	{Token: TokenRegexp, Precedence: spi.PrecedenceComparison},
	{Token: TokenColon, Symbol: ":", Precedence: spi.PrecedencePostfix},
	{Token: TokenQDcolon, Symbol: "?::", Precedence: spi.PrecedencePostfix},
	{Token: TokenDiv, Symbol: "DIV", Precedence: spi.PrecedenceMultiply},
}

// Databricks is the Databricks SQL dialect.
// Builder reads Config flags and auto-wires standard features:
// - QUALIFY clause (SupportsQualify)
// - ILIKE operator (SupportsIlike)
// - :: cast operator (SupportsCastOperator)
// - SEMI/ANTI joins (SupportsSemiAntiJoins)
var Databricks = dialect.New(Config).
	// Edge keywords - Databricks-specific (not auto-wired)
	AddKeyword("RLIKE", TokenRlike).
	AddKeyword("REGEXP", TokenRegexp).
	AddKeyword("DIV", TokenDiv).
	AddKeyword("LATERAL", TokenLateral).
	AddKeyword("VIEW", TokenView).
	AddKeyword("OUTER", TokenOuter).
	AddKeyword("TABLESAMPLE", TokenTablesample).
	AddKeyword("BUCKET", TokenBucket).
	AddKeyword("PERCENT", TokenPercent).
	AddKeyword("ROWS", TokenRows).
	// Clause Sequence
	// QUALIFY is auto-wired via Config.SupportsQualify
	Clauses(
		dialect.StandardWhere,
		dialect.StandardGroupBy,
		dialect.StandardHaving,
		dialect.StandardQualify, // Will be added by Build() if not present
		dialect.StandardWindow,
		dialect.StandardOrderBy,
		dialect.StandardLimit,
		dialect.StandardOffset,
		dialect.StandardFetch,
	).
	// Operators - standard ANSI + Databricks-specific
	// ILIKE and DCOLON are auto-wired via Config flags
	Operators(
		dialect.ANSIOperators,
		databricksOperators,
	).
	// Join Types - standard ANSI
	// SEMI/ANTI are auto-wired via Config.SupportsSemiAntiJoins
	JoinTypes(dialect.ANSIJoinTypes).
	// Documentation and metadata
	WithDocs(databricksFunctionDocs).
	WithReservedWords(databricksReservedWords...).
	Build()
