// Package parser provides SQL parsing and column-level lineage extraction.
// This file provides backward-compatible token type aliases.
package parser

import "github.com/leapstack-labs/leapsql/pkg/token"

// TokenType is an alias for token.TokenType for backward compatibility.
type TokenType = token.TokenType

// Token is an alias for token.Token for backward compatibility.
type Token = token.Token

// Position is an alias for token.Position for backward compatibility.
type Position = token.Position

// LookupIdent is re-exported from token package.
var LookupIdent = token.LookupIdent

//nolint:revive // TOKEN_* names are intentionally ALL_CAPS for SQL token conventions
const (
	// Special tokens
	TOKEN_EOF     = token.EOF
	TOKEN_ILLEGAL = token.ILLEGAL

	// Literals
	TOKEN_IDENT  = token.IDENT
	TOKEN_NUMBER = token.NUMBER
	TOKEN_STRING = token.STRING

	// Operators
	TOKEN_PLUS     = token.PLUS
	TOKEN_MINUS    = token.MINUS
	TOKEN_STAR     = token.STAR
	TOKEN_SLASH    = token.SLASH
	TOKEN_PERCENT  = token.PERCENT
	TOKEN_DPIPE    = token.DPIPE
	TOKEN_EQ       = token.EQ
	TOKEN_NE       = token.NE
	TOKEN_LT       = token.LT
	TOKEN_GT       = token.GT
	TOKEN_LE       = token.LE
	TOKEN_GE       = token.GE
	TOKEN_DOT      = token.DOT
	TOKEN_COMMA    = token.COMMA
	TOKEN_LPAREN   = token.LPAREN
	TOKEN_RPAREN   = token.RPAREN
	TOKEN_LBRACKET = token.LBRACKET
	TOKEN_RBRACKET = token.RBRACKET

	// Keywords (alphabetical)
	TOKEN_ALL       = token.ALL
	TOKEN_AND       = token.AND
	TOKEN_AS        = token.AS
	TOKEN_ASC       = token.ASC
	TOKEN_BETWEEN   = token.BETWEEN
	TOKEN_BY        = token.BY
	TOKEN_CASE      = token.CASE
	TOKEN_CAST      = token.CAST
	TOKEN_CROSS     = token.CROSS
	TOKEN_CURRENT   = token.CURRENT
	TOKEN_DESC      = token.DESC
	TOKEN_DISTINCT  = token.DISTINCT
	TOKEN_ELSE      = token.ELSE
	TOKEN_END       = token.END
	TOKEN_EXCEPT    = token.EXCEPT
	TOKEN_FALSE     = token.FALSE
	TOKEN_FILTER    = token.FILTER
	TOKEN_FIRST     = token.FIRST
	TOKEN_FOLLOWING = token.FOLLOWING
	TOKEN_FROM      = token.FROM
	TOKEN_FULL      = token.FULL
	TOKEN_GROUP     = token.GROUP
	TOKEN_GROUPS    = token.GROUPS
	TOKEN_HAVING    = token.HAVING
	TOKEN_IN        = token.IN
	TOKEN_INNER     = token.INNER
	TOKEN_INTERSECT = token.INTERSECT
	TOKEN_IS        = token.IS
	TOKEN_JOIN      = token.JOIN
	TOKEN_LAST      = token.LAST
	TOKEN_LATERAL   = token.LATERAL
	TOKEN_LEFT      = token.LEFT
	TOKEN_LIKE      = token.LIKE
	TOKEN_LIMIT     = token.LIMIT
	TOKEN_NOT       = token.NOT
	TOKEN_NULL      = token.NULL
	TOKEN_NULLS     = token.NULLS
	TOKEN_OFFSET    = token.OFFSET
	TOKEN_ON        = token.ON
	TOKEN_OR        = token.OR
	TOKEN_ORDER     = token.ORDER
	TOKEN_OUTER     = token.OUTER
	TOKEN_OVER      = token.OVER
	TOKEN_PARTITION = token.PARTITION
	TOKEN_PRECEDING = token.PRECEDING
	TOKEN_RANGE     = token.RANGE
	TOKEN_RECURSIVE = token.RECURSIVE
	TOKEN_RIGHT     = token.RIGHT
	TOKEN_ROW       = token.ROW
	TOKEN_ROWS      = token.ROWS
	TOKEN_SELECT    = token.SELECT
	TOKEN_THEN      = token.THEN
	TOKEN_TRUE      = token.TRUE
	TOKEN_UNBOUNDED = token.UNBOUNDED
	TOKEN_UNION     = token.UNION
	TOKEN_WHEN      = token.WHEN
	TOKEN_WHERE     = token.WHERE
	TOKEN_WINDOW    = token.WINDOW
	TOKEN_WITH      = token.WITH
	TOKEN_WITHIN    = token.WITHIN
)

// Dialect-specific tokens - these will be registered dynamically by dialects.
// We keep them as package-level variables for backward compatibility.
// Dialects should call token.Register() to get proper IDs.
var (
	// TokenQualify is registered by DuckDB/Snowflake/BigQuery/Teradata dialects.
	//nolint:revive // Keep old name pattern for backward compatibility
	TOKEN_QUALIFY = token.Register("QUALIFY")
	// TokenIlike is registered by Postgres/DuckDB dialects.
	//nolint:revive // Keep old name pattern for backward compatibility
	TOKEN_ILIKE = token.Register("ILIKE")
)

// init registers the dialect-specific keywords with the token package
// so the lexer can recognize them.
func init() {
	// Register with lowercase for lexer lookup
	registerKeyword("qualify", TOKEN_QUALIFY)
	registerKeyword("ilike", TOKEN_ILIKE)
}

// registeredKeywords stores dynamically registered keywords for lexer lookup.
var registeredKeywords = make(map[string]TokenType)

// registerKeyword registers a keyword for lexer lookup.
func registerKeyword(name string, t TokenType) {
	registeredKeywords[name] = t
}

// lookupDynamicKeyword checks if an identifier is a dynamically registered keyword.
func lookupDynamicKeyword(ident string) (TokenType, bool) {
	t, ok := registeredKeywords[ident]
	return t, ok
}
