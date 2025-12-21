// Package dialect provides the DuckDB SQL dialect definition.
// This package is lightweight and has no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package dialect

import (
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
	Identifiers(`"`, `"`, `""`, dialect.NormCaseInsensitive).
	DefaultSchema("main").
	PlaceholderStyle(dialect.PlaceholderQuestion).
	// Register DuckDB-specific keywords for the lexer
	AddKeyword("QUALIFY", TokenQualify).
	AddKeyword("ILIKE", TokenIlike).
	// Register DuckDB-specific operators for the lexer
	AddOperator("::", TokenDcolon).
	AddOperator("//", TokenDslash).
	// Add QUALIFY clause after HAVING in the clause sequence with slot
	AddClauseAfter(token.HAVING, TokenQualify, parseQualify, spi.SlotQualify).
	// Add ILIKE operator with same precedence as LIKE
	AddInfix(TokenIlike, spi.PrecedenceComparison).
	// Add :: cast operator (postfix precedence)
	AddInfix(TokenDcolon, spi.PrecedencePostfix).
	// Add // integer division (same as regular division)
	AddInfix(TokenDslash, spi.PrecedenceMultiply).
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
