// Package token defines the token types for SQL parsing.
//
// ANSI core tokens are defined as constants (IDs 0-999) for switch performance.
// Dialect-specific tokens are registered dynamically via Register().
package token

import "fmt"

// TokenType represents the type of a lexical token.
// Note: Named TokenType instead of Type because it's used extensively across
// the codebase and changing it would require a large refactor.
//
//nolint:revive // Accept stutter as token.TokenType is clear and widely used
type TokenType int32

//nolint:revive // TOKEN_* names are intentionally ALL_CAPS for SQL token conventions
const (
	// Special tokens
	EOF TokenType = iota
	ILLEGAL

	// Literals
	IDENT  // identifier
	NUMBER // 123, 45.67, 1e10
	STRING // 'hello'

	// Operators (ANSI)
	PLUS     // +
	MINUS    // -
	STAR     // *
	SLASH    // /
	PERCENT  // %
	DPIPE    // ||
	EQ       // =
	NE       // != or <>
	LT       // <
	GT       // >
	LE       // <=
	GE       // >=
	DOT      // .
	COMMA    // ,
	LPAREN   // (
	RPAREN   // )
	LBRACKET // [
	RBRACKET // ]

	// ANSI Keywords (alphabetical)
	ALL
	AND
	AS
	ASC
	BETWEEN
	BY
	CASE
	CAST
	CROSS
	CURRENT
	DESC
	DISTINCT
	ELSE
	END
	EXCEPT
	FALSE
	FILTER
	FIRST
	FOLLOWING
	FROM
	FULL
	GROUP
	GROUPS
	HAVING
	IN
	INNER
	INTERSECT
	IS
	JOIN
	LAST
	LATERAL
	LEFT
	LIKE
	LIMIT
	NOT
	NULL
	NULLS
	OFFSET
	ON
	OR
	ORDER
	OUTER
	OVER
	PARTITION
	PRECEDING
	RANGE
	RECURSIVE
	RIGHT
	ROW
	ROWS
	SELECT
	THEN
	TRUE
	UNBOUNDED
	UNION
	WHEN
	WHERE
	WINDOW // Named window definitions
	WITH
	WITHIN

	// Sentinel - dynamic tokens start after this
	maxBuiltin TokenType = 999
)

// String returns a human-readable representation of the token type.
func (t TokenType) String() string {
	// Check dynamic tokens first
	if name, ok := getDynamicName(t); ok {
		return name
	}
	// Then check builtin tokens
	if name, ok := tokenNames[t]; ok {
		return name
	}
	return fmt.Sprintf("TOKEN(%d)", t)
}

// tokenNames maps builtin token types to their string representations.
var tokenNames = map[TokenType]string{
	EOF:     "EOF",
	ILLEGAL: "ILLEGAL",

	IDENT:  "IDENT",
	NUMBER: "NUMBER",
	STRING: "STRING",

	PLUS:     "+",
	MINUS:    "-",
	STAR:     "*",
	SLASH:    "/",
	PERCENT:  "%",
	DPIPE:    "||",
	EQ:       "=",
	NE:       "!=",
	LT:       "<",
	GT:       ">",
	LE:       "<=",
	GE:       ">=",
	DOT:      ".",
	COMMA:    ",",
	LPAREN:   "(",
	RPAREN:   ")",
	LBRACKET: "[",
	RBRACKET: "]",

	ALL:       "ALL",
	AND:       "AND",
	AS:        "AS",
	ASC:       "ASC",
	BETWEEN:   "BETWEEN",
	BY:        "BY",
	CASE:      "CASE",
	CAST:      "CAST",
	CROSS:     "CROSS",
	CURRENT:   "CURRENT",
	DESC:      "DESC",
	DISTINCT:  "DISTINCT",
	ELSE:      "ELSE",
	END:       "END",
	EXCEPT:    "EXCEPT",
	FALSE:     "FALSE",
	FILTER:    "FILTER",
	FIRST:     "FIRST",
	FOLLOWING: "FOLLOWING",
	FROM:      "FROM",
	FULL:      "FULL",
	GROUP:     "GROUP",
	GROUPS:    "GROUPS",
	HAVING:    "HAVING",
	IN:        "IN",
	INNER:     "INNER",
	INTERSECT: "INTERSECT",
	IS:        "IS",
	JOIN:      "JOIN",
	LAST:      "LAST",
	LATERAL:   "LATERAL",
	LEFT:      "LEFT",
	LIKE:      "LIKE",
	LIMIT:     "LIMIT",
	NOT:       "NOT",
	NULL:      "NULL",
	NULLS:     "NULLS",
	OFFSET:    "OFFSET",
	ON:        "ON",
	OR:        "OR",
	ORDER:     "ORDER",
	OUTER:     "OUTER",
	OVER:      "OVER",
	PARTITION: "PARTITION",
	PRECEDING: "PRECEDING",
	RANGE:     "RANGE",
	RECURSIVE: "RECURSIVE",
	RIGHT:     "RIGHT",
	ROW:       "ROW",
	ROWS:      "ROWS",
	SELECT:    "SELECT",
	THEN:      "THEN",
	TRUE:      "TRUE",
	UNBOUNDED: "UNBOUNDED",
	UNION:     "UNION",
	WHEN:      "WHEN",
	WHERE:     "WHERE",
	WINDOW:    "WINDOW",
	WITH:      "WITH",
	WITHIN:    "WITHIN",
}

// keywords maps lowercase keyword strings to their token types.
var keywords = map[string]TokenType{
	"all":       ALL,
	"and":       AND,
	"as":        AS,
	"asc":       ASC,
	"between":   BETWEEN,
	"by":        BY,
	"case":      CASE,
	"cast":      CAST,
	"cross":     CROSS,
	"current":   CURRENT,
	"desc":      DESC,
	"distinct":  DISTINCT,
	"else":      ELSE,
	"end":       END,
	"except":    EXCEPT,
	"false":     FALSE,
	"filter":    FILTER,
	"first":     FIRST,
	"following": FOLLOWING,
	"from":      FROM,
	"full":      FULL,
	"group":     GROUP,
	"groups":    GROUPS,
	"having":    HAVING,
	"in":        IN,
	"inner":     INNER,
	"intersect": INTERSECT,
	"is":        IS,
	"join":      JOIN,
	"last":      LAST,
	"lateral":   LATERAL,
	"left":      LEFT,
	"like":      LIKE,
	"limit":     LIMIT,
	"not":       NOT,
	"null":      NULL,
	"nulls":     NULLS,
	"offset":    OFFSET,
	"on":        ON,
	"or":        OR,
	"order":     ORDER,
	"outer":     OUTER,
	"over":      OVER,
	"partition": PARTITION,
	"preceding": PRECEDING,
	"range":     RANGE,
	"recursive": RECURSIVE,
	"right":     RIGHT,
	"row":       ROW,
	"rows":      ROWS,
	"select":    SELECT,
	"then":      THEN,
	"true":      TRUE,
	"unbounded": UNBOUNDED,
	"union":     UNION,
	"when":      WHEN,
	"where":     WHERE,
	"window":    WINDOW,
	"with":      WITH,
	"within":    WITHIN,
}

// LookupIdent returns the token type for the given identifier.
// If the identifier is a keyword, the keyword token type is returned.
// Otherwise, IDENT is returned.
// This only checks builtin keywords; use LookupIdentWithDialect for dynamic keywords.
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}

// IsKeyword returns true if the token type is a keyword.
func IsKeyword(t TokenType) bool {
	return t >= ALL && t <= WITHIN
}

// IsOperator returns true if the token type is an operator.
func IsOperator(t TokenType) bool {
	return t >= PLUS && t <= RBRACKET
}

// Token represents a lexical token with position information.
type Token struct {
	Type    TokenType
	Literal string
	Pos     Position
}
