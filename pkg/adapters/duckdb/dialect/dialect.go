// Package dialect provides the DuckDB SQL dialect definition.
// This package is lightweight and has no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/dialects/ansi"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

//go:generate go run ../../../../scripts/gendialect -dialect=duckdb -gen=all -outdir=.

func init() {
	dialect.Register(DuckDB)
}

// DuckDB-specific tokens (registered dynamically)
var (
	// TokenQualify is a DuckDB-specific clause for filtering window functions
	TokenQualify = token.Register("QUALIFY")
	// TokenIlike is case-insensitive LIKE (DuckDB and Postgres)
	TokenIlike = token.Register("ILIKE")
	// TokenDcolon is the :: cast operator
	TokenDcolon = token.Register("::")
	// TokenDslash is the // integer division operator
	TokenDslash = token.Register("//")

	// DuckDB-specific join type tokens
	TokenSemi       = token.Register("SEMI")
	TokenAnti       = token.Register("ANTI")
	TokenAsof       = token.Register("ASOF")
	TokenPositional = token.Register("POSITIONAL")

	// DuckDB-specific PIVOT/UNPIVOT tokens
	TokenPivot   = token.Register("PIVOT")
	TokenUnpivot = token.Register("UNPIVOT")
	TokenFor     = token.Register("FOR")
)

// parseQualify handles the QUALIFY clause (DuckDB-specific).
// The QUALIFY keyword has already been consumed.
func parseQualify(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// DuckDB is the DuckDB dialect configuration.
var DuckDB = dialect.NewDialect("duckdb").
	// Inherit from ANSI base dialect (includes || operator)
	Extends(ansi.ANSI).
	// DuckDB-specific configuration
	Identifiers(`"`, `"`, `""`, core.NormCaseInsensitive).
	DefaultSchema("main").
	PlaceholderStyle(core.PlaceholderQuestion).
	// Register DuckDB-specific keywords for the lexer
	AddKeyword("QUALIFY", TokenQualify).
	AddKeyword("ILIKE", TokenIlike).
	AddKeyword("SEMI", TokenSemi).
	AddKeyword("ANTI", TokenAnti).
	AddKeyword("ASOF", TokenAsof).
	AddKeyword("POSITIONAL", TokenPositional).
	// Register DuckDB-specific operators for the lexer
	AddOperator("::", TokenDcolon).
	AddOperator("//", TokenDslash).
	// Override GROUP BY handler to support GROUP BY ALL
	ClauseHandler(token.GROUP, parseGroupByWithAll, spi.SlotGroupBy, dialect.WithKeywords("GROUP", "BY")).
	// Override ORDER BY handler to support ORDER BY ALL
	ClauseHandler(token.ORDER, parseOrderByWithAll, spi.SlotOrderBy, dialect.WithKeywords("ORDER", "BY")).
	// Add QUALIFY clause after HAVING in the clause sequence with slot
	AddClauseAfter(token.HAVING, TokenQualify, parseQualify, spi.SlotQualify).
	// Add ILIKE operator with same precedence as LIKE
	AddInfix(TokenIlike, spi.PrecedenceComparison).
	// Add :: cast operator (postfix precedence)
	AddInfix(TokenDcolon, spi.PrecedencePostfix).
	// Add // integer division (same as regular division)
	AddInfix(TokenDslash, spi.PrecedenceMultiply).
	// Register DuckDB-specific join types
	AddJoinType(TokenSemi, dialect.JoinTypeDef{
		Type:        "SEMI",
		RequiresOn:  true,
		AllowsUsing: true,
	}).
	AddJoinType(TokenAnti, dialect.JoinTypeDef{
		Type:        "ANTI",
		RequiresOn:  true,
		AllowsUsing: true,
	}).
	AddJoinType(TokenAsof, dialect.JoinTypeDef{
		Type:        "ASOF",
		RequiresOn:  true,
		AllowsUsing: false, // ASOF requires inequality conditions
	}).
	AddJoinType(TokenPositional, dialect.JoinTypeDef{
		Type:        "POSITIONAL",
		RequiresOn:  false, // No condition for positional join
		AllowsUsing: false,
	}).
	// Register DuckDB-specific star modifier keywords
	AddKeyword("EXCLUDE", TokenExclude).
	AddKeyword("REPLACE", TokenReplace).
	AddKeyword("RENAME", TokenRename).
	// Register star modifier handlers
	AddStarModifier(TokenExclude, parseExclude).
	AddStarModifier(TokenReplace, parseReplace).
	AddStarModifier(TokenRename, parseRename).
	// Register DuckDB expression extensions (Phase 3)
	// List literals: [1, 2, 3]
	AddPrefix(token.LBRACKET, parseListLiteral).
	// Struct literals: {'name': 'Alice', 'age': 30}
	AddPrefix(token.LBRACE, parseStructLiteral).
	// Array indexing/slicing: arr[1], arr[1:3]
	AddInfixWithHandler(token.LBRACKET, spi.PrecedencePostfix, parseIndexOrSlice).
	// Lambda expressions: x -> x * 2
	// Use PrecedenceOr (lowest positive precedence) to allow x -> x + y to work
	AddInfixWithHandler(token.ARROW, spi.PrecedenceOr, parseLambdaBody).
	// Register PIVOT/UNPIVOT keywords for the lexer
	AddKeyword("PIVOT", TokenPivot).
	AddKeyword("UNPIVOT", TokenUnpivot).
	AddKeyword("FOR", TokenFor).
	// Register PIVOT/UNPIVOT FROM item handlers
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
	// DuckDB-specific lint rules
	LintRulesAdd(AllRules...).
	Build()
