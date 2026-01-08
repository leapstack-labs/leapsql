// Package snowflake provides the Snowflake SQL dialect definition.
// This package is pure Go with no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package snowflake

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

//go:generate go run ../../../scripts/gensnowflake -gen=all -outdir=.

func init() {
	dialect.Register(Snowflake)
}

// Snowflake-specific tokens (edge features not auto-wired by framework)
// Standard tokens (QUALIFY, ILIKE, DCOLON) are auto-wired via Config flags
var (
	// TokenRlike is regex match (RLIKE)
	TokenRlike = token.Register("RLIKE")
	// TokenRegexp is regex match (REGEXP alias)
	TokenRegexp = token.Register("REGEXP")

	// TABLESAMPLE tokens
	TokenTablesample = token.Register("TABLESAMPLE")
	TokenSample      = token.Register("SAMPLE")
)

// --- Snowflake-specific Operators ---

var snowflakeOperators = []core.OperatorDef{
	{Token: TokenRlike, Precedence: core.PrecedenceComparison},
	{Token: TokenRegexp, Precedence: core.PrecedenceComparison},
}

// Snowflake is the Snowflake SQL dialect.
// Builder reads Config flags and auto-wires standard features:
// - QUALIFY clause (SupportsQualify)
// - ILIKE operator (SupportsIlike)
// - :: cast operator (SupportsCastOperator)
var Snowflake = dialect.New(Config).
	// Edge keywords - Snowflake-specific (not auto-wired)
	AddKeyword("RLIKE", TokenRlike).
	AddKeyword("REGEXP", TokenRegexp).
	AddKeyword("TABLESAMPLE", TokenTablesample).
	AddKeyword("SAMPLE", TokenSample).
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
	// Operators - standard ANSI + Snowflake-specific
	// ILIKE and DCOLON are auto-wired via Config flags
	Operators(
		dialect.ANSIOperators,
		snowflakeOperators,
	).
	// Join Types - standard ANSI
	JoinTypes(dialect.ANSIJoinTypes).
	// Documentation and metadata
	WithDocs(snowflakeFunctionDocs).
	WithReservedWords(snowflakeReservedWords...).
	Build()
