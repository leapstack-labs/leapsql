package lineage

import "fmt"

// TokenType represents the type of a lexical token.
type TokenType int

//nolint:revive // TOKEN_* names are intentionally ALL_CAPS for SQL token conventions
const (
	// TokenEOF represents end of file.
	TOKEN_EOF TokenType = iota
	// TokenIllegal represents an illegal/unrecognized token.
	TOKEN_ILLEGAL

	// TokenIdent represents an identifier.
	TOKEN_IDENT
	// TokenNumber represents a numeric literal.
	TOKEN_NUMBER // 123, 45.67, 1e10
	// TokenString represents a string literal.
	TOKEN_STRING // 'hello'

	// TokenPlus represents the + operator.
	TOKEN_PLUS     // +
	TOKEN_MINUS    // -
	TOKEN_STAR     // *
	TOKEN_SLASH    // /
	TOKEN_PERCENT  // %
	TOKEN_DPIPE    // ||
	TOKEN_EQ       // =
	TOKEN_NE       // != or <>
	TOKEN_LT       // <
	TOKEN_GT       // >
	TOKEN_LE       // <=
	TOKEN_GE       // >=
	TOKEN_DOT      // .
	TOKEN_COMMA    // ,
	TOKEN_LPAREN   // (
	TOKEN_RPAREN   // )
	TOKEN_LBRACKET // [
	TOKEN_RBRACKET // ]

	// Keywords (alphabetical)
	TOKEN_ALL
	TOKEN_AND
	TOKEN_AS
	TOKEN_ASC
	TOKEN_BETWEEN
	TOKEN_BY
	TOKEN_CASE
	TOKEN_CAST
	TOKEN_CROSS
	TOKEN_CURRENT
	TOKEN_DESC
	TOKEN_DISTINCT
	TOKEN_ELSE
	TOKEN_END
	TOKEN_EXCEPT
	TOKEN_FALSE
	TOKEN_FILTER
	TOKEN_FIRST
	TOKEN_FOLLOWING
	TOKEN_FROM
	TOKEN_FULL
	TOKEN_GROUP
	TOKEN_GROUPS
	TOKEN_HAVING
	TOKEN_ILIKE
	TOKEN_IN
	TOKEN_INNER
	TOKEN_INTERSECT
	TOKEN_IS
	TOKEN_JOIN
	TOKEN_LAST
	TOKEN_LATERAL
	TOKEN_LEFT
	TOKEN_LIKE
	TOKEN_LIMIT
	TOKEN_NOT
	TOKEN_NULL
	TOKEN_NULLS
	TOKEN_OFFSET
	TOKEN_ON
	TOKEN_OR
	TOKEN_ORDER
	TOKEN_OUTER
	TOKEN_OVER
	TOKEN_PARTITION
	TOKEN_PRECEDING
	TOKEN_QUALIFY
	TOKEN_RANGE
	TOKEN_RECURSIVE
	TOKEN_RIGHT
	TOKEN_ROW
	TOKEN_ROWS
	TOKEN_SELECT
	TOKEN_THEN
	TOKEN_TRUE
	TOKEN_UNBOUNDED
	TOKEN_UNION
	TOKEN_WHEN
	TOKEN_WHERE
	TOKEN_WITH
	TOKEN_WITHIN
)

// Token represents a lexical token with position information.
type Token struct {
	Type    TokenType
	Literal string
	Pos     Position
}

// Position represents a location in the source code.
type Position struct {
	Line   int // 1-based line number
	Column int // 1-based column number
	Offset int // 0-based byte offset
}

// String returns a human-readable representation of the token type.
func (t TokenType) String() string {
	if name, ok := tokenNames[t]; ok {
		return name
	}
	return fmt.Sprintf("TOKEN(%d)", t)
}

// tokenNames maps token types to their string representations.
var tokenNames = map[TokenType]string{
	TOKEN_EOF:     "EOF",
	TOKEN_ILLEGAL: "ILLEGAL",

	TOKEN_IDENT:  "IDENT",
	TOKEN_NUMBER: "NUMBER",
	TOKEN_STRING: "STRING",

	TOKEN_PLUS:     "+",
	TOKEN_MINUS:    "-",
	TOKEN_STAR:     "*",
	TOKEN_SLASH:    "/",
	TOKEN_PERCENT:  "%",
	TOKEN_DPIPE:    "||",
	TOKEN_EQ:       "=",
	TOKEN_NE:       "!=",
	TOKEN_LT:       "<",
	TOKEN_GT:       ">",
	TOKEN_LE:       "<=",
	TOKEN_GE:       ">=",
	TOKEN_DOT:      ".",
	TOKEN_COMMA:    ",",
	TOKEN_LPAREN:   "(",
	TOKEN_RPAREN:   ")",
	TOKEN_LBRACKET: "[",
	TOKEN_RBRACKET: "]",

	TOKEN_ALL:       "ALL",
	TOKEN_AND:       "AND",
	TOKEN_AS:        "AS",
	TOKEN_ASC:       "ASC",
	TOKEN_BETWEEN:   "BETWEEN",
	TOKEN_BY:        "BY",
	TOKEN_CASE:      "CASE",
	TOKEN_CAST:      "CAST",
	TOKEN_CROSS:     "CROSS",
	TOKEN_CURRENT:   "CURRENT",
	TOKEN_DESC:      "DESC",
	TOKEN_DISTINCT:  "DISTINCT",
	TOKEN_ELSE:      "ELSE",
	TOKEN_END:       "END",
	TOKEN_EXCEPT:    "EXCEPT",
	TOKEN_FALSE:     "FALSE",
	TOKEN_FILTER:    "FILTER",
	TOKEN_FIRST:     "FIRST",
	TOKEN_FOLLOWING: "FOLLOWING",
	TOKEN_FROM:      "FROM",
	TOKEN_FULL:      "FULL",
	TOKEN_GROUP:     "GROUP",
	TOKEN_GROUPS:    "GROUPS",
	TOKEN_HAVING:    "HAVING",
	TOKEN_ILIKE:     "ILIKE",
	TOKEN_IN:        "IN",
	TOKEN_INNER:     "INNER",
	TOKEN_INTERSECT: "INTERSECT",
	TOKEN_IS:        "IS",
	TOKEN_JOIN:      "JOIN",
	TOKEN_LAST:      "LAST",
	TOKEN_LATERAL:   "LATERAL",
	TOKEN_LEFT:      "LEFT",
	TOKEN_LIKE:      "LIKE",
	TOKEN_LIMIT:     "LIMIT",
	TOKEN_NOT:       "NOT",
	TOKEN_NULL:      "NULL",
	TOKEN_NULLS:     "NULLS",
	TOKEN_OFFSET:    "OFFSET",
	TOKEN_ON:        "ON",
	TOKEN_OR:        "OR",
	TOKEN_ORDER:     "ORDER",
	TOKEN_OUTER:     "OUTER",
	TOKEN_OVER:      "OVER",
	TOKEN_PARTITION: "PARTITION",
	TOKEN_PRECEDING: "PRECEDING",
	TOKEN_QUALIFY:   "QUALIFY",
	TOKEN_RANGE:     "RANGE",
	TOKEN_RECURSIVE: "RECURSIVE",
	TOKEN_RIGHT:     "RIGHT",
	TOKEN_ROW:       "ROW",
	TOKEN_ROWS:      "ROWS",
	TOKEN_SELECT:    "SELECT",
	TOKEN_THEN:      "THEN",
	TOKEN_TRUE:      "TRUE",
	TOKEN_UNBOUNDED: "UNBOUNDED",
	TOKEN_UNION:     "UNION",
	TOKEN_WHEN:      "WHEN",
	TOKEN_WHERE:     "WHERE",
	TOKEN_WITH:      "WITH",
	TOKEN_WITHIN:    "WITHIN",
}

// keywords maps lowercase keyword strings to their token types.
var keywords = map[string]TokenType{
	"all":       TOKEN_ALL,
	"and":       TOKEN_AND,
	"as":        TOKEN_AS,
	"asc":       TOKEN_ASC,
	"between":   TOKEN_BETWEEN,
	"by":        TOKEN_BY,
	"case":      TOKEN_CASE,
	"cast":      TOKEN_CAST,
	"cross":     TOKEN_CROSS,
	"current":   TOKEN_CURRENT,
	"desc":      TOKEN_DESC,
	"distinct":  TOKEN_DISTINCT,
	"else":      TOKEN_ELSE,
	"end":       TOKEN_END,
	"except":    TOKEN_EXCEPT,
	"false":     TOKEN_FALSE,
	"filter":    TOKEN_FILTER,
	"first":     TOKEN_FIRST,
	"following": TOKEN_FOLLOWING,
	"from":      TOKEN_FROM,
	"full":      TOKEN_FULL,
	"group":     TOKEN_GROUP,
	"groups":    TOKEN_GROUPS,
	"having":    TOKEN_HAVING,
	"ilike":     TOKEN_ILIKE,
	"in":        TOKEN_IN,
	"inner":     TOKEN_INNER,
	"intersect": TOKEN_INTERSECT,
	"is":        TOKEN_IS,
	"join":      TOKEN_JOIN,
	"last":      TOKEN_LAST,
	"lateral":   TOKEN_LATERAL,
	"left":      TOKEN_LEFT,
	"like":      TOKEN_LIKE,
	"limit":     TOKEN_LIMIT,
	"not":       TOKEN_NOT,
	"null":      TOKEN_NULL,
	"nulls":     TOKEN_NULLS,
	"offset":    TOKEN_OFFSET,
	"on":        TOKEN_ON,
	"or":        TOKEN_OR,
	"order":     TOKEN_ORDER,
	"outer":     TOKEN_OUTER,
	"over":      TOKEN_OVER,
	"partition": TOKEN_PARTITION,
	"preceding": TOKEN_PRECEDING,
	"qualify":   TOKEN_QUALIFY,
	"range":     TOKEN_RANGE,
	"recursive": TOKEN_RECURSIVE,
	"right":     TOKEN_RIGHT,
	"row":       TOKEN_ROW,
	"rows":      TOKEN_ROWS,
	"select":    TOKEN_SELECT,
	"then":      TOKEN_THEN,
	"true":      TOKEN_TRUE,
	"unbounded": TOKEN_UNBOUNDED,
	"union":     TOKEN_UNION,
	"when":      TOKEN_WHEN,
	"where":     TOKEN_WHERE,
	"with":      TOKEN_WITH,
	"within":    TOKEN_WITHIN,
}

// LookupIdent returns the token type for the given identifier.
// If the identifier is a keyword, the keyword token type is returned.
// Otherwise, TOKEN_IDENT is returned.
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TOKEN_IDENT
}
