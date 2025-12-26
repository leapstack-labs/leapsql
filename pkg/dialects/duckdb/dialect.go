// Package duckdb provides the DuckDB SQL dialect definition.
// This package is pure Go with no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package duckdb

import (
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

//go:generate go run ../../../scripts/gendialect -dialect=duckdb -gen=all -outdir=.

func init() {
	dialect.Register(DuckDB)
}

// DuckDB-specific tokens (edge features not auto-wired by framework)
// Standard tokens (QUALIFY, ILIKE, SEMI, ANTI, DCOLON) are auto-wired via Config flags
var (
	// TokenDslash is the // integer division operator
	TokenDslash = token.Register("//")

	// DuckDB-specific join type tokens (not in standard token set)
	TokenAsof       = token.Register("ASOF")
	TokenPositional = token.Register("POSITIONAL")

	// DuckDB-specific PIVOT/UNPIVOT tokens
	TokenPivot   = token.Register("PIVOT")
	TokenUnpivot = token.Register("UNPIVOT")
	TokenFor     = token.Register("FOR")
)

// DuckDB is the DuckDB dialect.
// Builder reads Config flags and auto-wires standard features:
// - QUALIFY clause (SupportsQualify)
// - ILIKE operator (SupportsIlike)
// - :: cast operator (SupportsCastOperator)
// - SEMI/ANTI joins (SupportsSemiAntiJoins)
// - GROUP BY ALL (SupportsGroupByAll)
// - ORDER BY ALL (SupportsOrderByAll)
var DuckDB = dialect.New(Config).
	// Edge keywords - DuckDB-specific (not auto-wired)
	AddKeyword("ASOF", TokenAsof).
	AddKeyword("POSITIONAL", TokenPositional).
	AddKeyword("EXCLUDE", TokenExclude).
	AddKeyword("REPLACE", TokenReplace).
	AddKeyword("RENAME", TokenRename).
	AddKeyword("PIVOT", TokenPivot).
	AddKeyword("UNPIVOT", TokenUnpivot).
	AddKeyword("FOR", TokenFor).
	// Edge operators
	AddOperator("//", TokenDslash).
	AddInfix(TokenDslash, spi.PrecedenceMultiply).
	// Clause Sequence - DuckDB uses standard ANSI clauses
	// Build() will replace handlers for clauses based on Config flags:
	// - SupportsGroupByAll -> GROUP BY handler replaced with ALL-aware version
	// - SupportsOrderByAll -> ORDER BY handler replaced with ALL-aware version
	// - SupportsQualify -> QUALIFY clause added if not present
	Clauses(
		dialect.StandardWhere,
		dialect.StandardGroupBy, // Handler replaced by Build() with ALL support
		dialect.StandardHaving,
		dialect.StandardQualify, // Added via Config.SupportsQualify
		dialect.StandardWindow,
		dialect.StandardOrderBy, // Handler replaced by Build() with ALL support
		dialect.StandardLimit,
		dialect.StandardOffset,
		dialect.StandardFetch,
	).
	// Operators - standard ANSI operators
	Operators(dialect.ANSIOperators).
	// Join Types - standard ANSI + edge DuckDB-specific types
	// SEMI/ANTI are auto-wired via Config.SupportsSemiAntiJoins
	JoinTypes(dialect.ANSIJoinTypes).
	AddJoinType(TokenAsof, dialect.JoinTypeDef{
		Token:       TokenAsof,
		Type:        JoinAsof,
		RequiresOn:  true,
		AllowsUsing: false, // ASOF requires inequality conditions
	}).
	AddJoinType(TokenPositional, dialect.JoinTypeDef{
		Token:       TokenPositional,
		Type:        JoinPositional,
		RequiresOn:  false, // No condition for positional join
		AllowsUsing: false,
	}).
	// Star modifiers
	AddStarModifier(TokenExclude, parseExclude).
	AddStarModifier(TokenReplace, parseReplace).
	AddStarModifier(TokenRename, parseRename).
	// Expression extensions
	AddPrefix(token.LBRACKET, parseListLiteral).
	AddPrefix(token.LBRACE, parseStructLiteral).
	AddInfixWithHandler(token.LBRACKET, spi.PrecedencePostfix, parseIndexOrSlice).
	AddInfixWithHandler(token.ARROW, spi.PrecedenceOr, parseLambdaBody).
	// FROM extensions
	AddFromItem(TokenPivot, parsePivot).
	AddFromItem(TokenUnpivot, parseUnpivot).
	// Documentation and metadata
	WithDocs(duckDBFunctionDocs).
	WithDocs(duckDBWindowDocs).
	WithReservedWords(duckDBAllKeywords...).
	Build()
