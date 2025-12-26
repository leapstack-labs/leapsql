// Package duckdb provides the DuckDB SQL dialect definition.
// This package is pure Go with no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package duckdb

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

//go:generate go run ../../../scripts/gendialect -dialect=duckdb -gen=all -outdir=.

func init() {
	dialect.Register(DuckDB)
}

// DuckDB-specific tokens
// Standard tokens (QUALIFY, ILIKE, SEMI, ANTI, DCOLON) use builtin token constants
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

// --- DuckDB-specific Clause Definitions ---

// DuckDBGroupBy is GROUP BY with ALL support.
var DuckDBGroupBy = dialect.ClauseDef{
	Token:    token.GROUP,
	Handler:  parseGroupByWithAll,
	Slot:     spi.SlotGroupBy,
	Keywords: []string{"GROUP", "BY"},
}

// DuckDBOrderBy is ORDER BY with ALL support.
var DuckDBOrderBy = dialect.ClauseDef{
	Token:    token.ORDER,
	Handler:  parseOrderByWithAll,
	Slot:     spi.SlotOrderBy,
	Keywords: []string{"ORDER", "BY"},
}

// DuckDBQualify is the QUALIFY clause for window function filtering.
var DuckDBQualify = dialect.ClauseDef{
	Token:   token.QUALIFY,
	Handler: parseQualify,
	Slot:    spi.SlotQualify,
}

// parseQualify handles the QUALIFY clause (DuckDB-specific).
// The QUALIFY keyword has already been consumed.
func parseQualify(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// --- DuckDB-specific Operators ---

var duckDBOperators = []dialect.OperatorDef{
	{Token: token.ILIKE, Precedence: spi.PrecedenceComparison},
	{Token: token.DCOLON, Symbol: "::", Precedence: spi.PrecedencePostfix},
	{Token: TokenDslash, Symbol: "//", Precedence: spi.PrecedenceMultiply},
}

// --- DuckDB-specific Join Types ---

var duckDBJoinTypes = []dialect.JoinTypeDef{
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
	{
		Token:       TokenAsof,
		Type:        JoinAsof,
		RequiresOn:  true,
		AllowsUsing: false, // ASOF requires inequality conditions
	},
	{
		Token:       TokenPositional,
		Type:        JoinPositional,
		RequiresOn:  false, // No condition for positional join
		AllowsUsing: false,
	},
}

// DuckDB is the DuckDB dialect configuration.
// Uses explicit composition - no inheritance from ANSI.
var DuckDB = dialect.NewDialect("duckdb").
	// Static Configuration
	Identifiers(`"`, `"`, `""`, core.NormCaseInsensitive).
	DefaultSchema("main").
	PlaceholderStyle(core.PlaceholderQuestion).
	// Register DuckDB-specific keywords for the lexer
	AddKeyword("QUALIFY", token.QUALIFY).
	AddKeyword("ILIKE", token.ILIKE).
	AddKeyword("SEMI", token.SEMI).
	AddKeyword("ANTI", token.ANTI).
	AddKeyword("ASOF", TokenAsof).
	AddKeyword("POSITIONAL", TokenPositional).
	AddKeyword("EXCLUDE", TokenExclude).
	AddKeyword("REPLACE", TokenReplace).
	AddKeyword("RENAME", TokenRename).
	AddKeyword("PIVOT", TokenPivot).
	AddKeyword("UNPIVOT", TokenUnpivot).
	AddKeyword("FOR", TokenFor).
	// Clause Sequence - EXPLICIT, no inheritance
	// DuckDB uses standard ANSI clauses with overrides and additions
	Clauses(
		dialect.StandardWhere,
		DuckDBGroupBy, // Override: GROUP BY ALL support
		dialect.StandardHaving,
		DuckDBQualify, // DuckDB-specific: QUALIFY
		dialect.StandardWindow,
		DuckDBOrderBy, // Override: ORDER BY ALL support
		dialect.StandardLimit,
		dialect.StandardOffset,
		dialect.StandardFetch,
	).
	// Operators - compose from standard + custom
	Operators(
		dialect.ANSIOperators,
		duckDBOperators,
	).
	// Join Types - compose from standard + custom
	JoinTypes(
		dialect.ANSIJoinTypes,
		duckDBJoinTypes,
	).
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
	// Function classifications
	Aggregates(duckDBAggregates...).
	Generators(duckDBGenerators...).
	Windows(duckDBWindows...).
	TableFunctions(duckDBTableFunctions...).
	WithDocs(duckDBFunctionDocs).
	WithDocs(duckDBWindowDocs).
	WithKeywords(duckDBCompletionKeywords...).
	WithReservedWords(duckDBAllKeywords...).
	WithDataTypes(duckDBTypes...).
	Build()
