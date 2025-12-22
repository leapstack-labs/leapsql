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
	MOD      // %
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
	LBRACE   // {
	RBRACE   // }
	COLON    // :
	ARROW    // ->

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
	EXISTS
	EXCEPT
	FALSE
	FETCH
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
	NAME
	NATURAL
	NEXT
	NOT
	NULL
	NULLS
	OFFSET
	ON
	ONLY
	OR
	ORDER
	OUTER
	OVER
	PARTITION
	PERCENT
	PRECEDING
	RANGE
	RECURSIVE
	RIGHT
	ROW
	ROWS
	SELECT
	THEN
	TIES
	TRUE
	UNBOUNDED
	UNION
	USING
	WHEN
	WHERE
	WINDOW // Named window definitions
	WITH
	WITHIN

	// Template tokens
	MACRO // {{ ... }} content

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
	MOD:      "%",
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
	LBRACE:   "{",
	RBRACE:   "}",
	COLON:    ":",
	ARROW:    "->",

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
	EXISTS:    "EXISTS",
	EXCEPT:    "EXCEPT",
	FALSE:     "FALSE",
	FETCH:     "FETCH",
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
	NAME:      "NAME",
	NATURAL:   "NATURAL",
	NEXT:      "NEXT",
	NOT:       "NOT",
	NULL:      "NULL",
	NULLS:     "NULLS",
	OFFSET:    "OFFSET",
	ON:        "ON",
	ONLY:      "ONLY",
	OR:        "OR",
	ORDER:     "ORDER",
	OUTER:     "OUTER",
	OVER:      "OVER",
	PARTITION: "PARTITION",
	PERCENT:   "PERCENT",
	PRECEDING: "PRECEDING",
	RANGE:     "RANGE",
	RECURSIVE: "RECURSIVE",
	RIGHT:     "RIGHT",
	ROW:       "ROW",
	ROWS:      "ROWS",
	SELECT:    "SELECT",
	THEN:      "THEN",
	TIES:      "TIES",
	TRUE:      "TRUE",
	UNBOUNDED: "UNBOUNDED",
	UNION:     "UNION",
	USING:     "USING",
	WHEN:      "WHEN",
	WHERE:     "WHERE",
	WINDOW:    "WINDOW",
	WITH:      "WITH",
	WITHIN:    "WITHIN",
	MACRO:     "MACRO",
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
	"exists":    EXISTS,
	"except":    EXCEPT,
	"false":     FALSE,
	"fetch":     FETCH,
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
	"natural":   NATURAL,
	"next":      NEXT,
	"not":       NOT,
	"null":      NULL,
	"nulls":     NULLS,
	"offset":    OFFSET,
	"on":        ON,
	"only":      ONLY,
	"or":        OR,
	"order":     ORDER,
	"outer":     OUTER,
	"over":      OVER,
	"partition": PARTITION,
	"percent":   PERCENT,
	"preceding": PRECEDING,
	"range":     RANGE,
	"recursive": RECURSIVE,
	"right":     RIGHT,
	"row":       ROW,
	"rows":      ROWS,
	"select":    SELECT,
	"then":      THEN,
	"ties":      TIES,
	"true":      TRUE,
	"unbounded": UNBOUNDED,
	"union":     UNION,
	"using":     USING,
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
