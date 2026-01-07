package parser

import (
	"sort"
	"strings"
	"unicode"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Lexer tokenizes SQL input.
type Lexer struct {
	input   string
	pos     int  // current position in input
	readPos int  // reading position (after current char)
	ch      byte // current char under examination
	line    int  // current line number (1-based)
	col     int  // current column number (1-based)

	// Dialect support (optional)
	dialect *core.Dialect

	// Comments collected during lexing (for formatter)
	Comments []*token.Comment
}

// NewLexer creates a new Lexer for the given input.
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input: input,
		line:  1,
		col:   0,
	}
	l.readChar()
	return l
}

// NewLexerWithDialect creates a new dialect-aware Lexer for the given input.
func NewLexerWithDialect(input string, d *core.Dialect) *Lexer {
	l := &Lexer{
		input:   input,
		line:    1,
		col:     0,
		dialect: d,
	}
	l.readChar()
	return l
}

// readChar advances to the next character.
func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0 // ASCII NUL = EOF
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++

	if l.ch == '\n' {
		l.line++
		l.col = 0
	} else {
		l.col++
	}
}

// peekChar returns the next character without advancing.
func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// currentPos returns the current position.
func (l *Lexer) currentPos() Position {
	return Position{
		Line:   l.line,
		Column: l.col,
		Offset: l.pos,
	}
}

// NextToken returns the next token.
func (l *Lexer) NextToken() Token {
	l.skipWhitespaceAndComments()

	pos := l.currentPos()

	// Check for macro start {{ before other processing
	if l.ch == '{' && l.peekChar() == '{' {
		return l.readMacro(pos)
	}

	// Check dialect-specific symbols first (longest match)
	if tok, ok := l.matchDialectSymbol(pos); ok {
		return tok
	}

	var tok Token
	tok.Pos = pos

	switch l.ch {
	case 0:
		tok.Type = TOKEN_EOF
		tok.Literal = ""
	case '+':
		tok = l.newToken(TOKEN_PLUS, "+")
	case '-':
		if l.peekChar() == '>' {
			l.readChar()
			tok = Token{Type: TOKEN_ARROW, Literal: "->", Pos: pos}
		} else {
			// Could be negative number or minus operator
			tok = l.newToken(TOKEN_MINUS, "-")
		}
	case '*':
		tok = l.newToken(TOKEN_STAR, "*")
	case '/':
		tok = l.newToken(TOKEN_SLASH, "/")
	case '%':
		tok = l.newToken(TOKEN_MOD, "%")
	case '=':
		tok = l.newToken(TOKEN_EQ, "=")
	case '<':
		switch l.peekChar() {
		case '=':
			l.readChar()
			tok = Token{Type: TOKEN_LE, Literal: "<=", Pos: pos}
		case '>':
			l.readChar()
			tok = Token{Type: TOKEN_NE, Literal: "<>", Pos: pos}
		default:
			tok = l.newToken(TOKEN_LT, "<")
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TOKEN_GE, Literal: ">=", Pos: pos}
		} else {
			tok = l.newToken(TOKEN_GT, ">")
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TOKEN_NE, Literal: "!=", Pos: pos}
		} else {
			tok = l.newToken(TOKEN_ILLEGAL, string(l.ch))
		}
	case '|':
		if l.peekChar() == '|' {
			l.readChar()
			tok = Token{Type: TOKEN_DPIPE, Literal: "||", Pos: pos}
		} else {
			tok = l.newToken(TOKEN_ILLEGAL, string(l.ch))
		}
	case '.':
		tok = l.newToken(TOKEN_DOT, ".")
	case ',':
		tok = l.newToken(TOKEN_COMMA, ",")
	case '(':
		tok = l.newToken(TOKEN_LPAREN, "(")
	case ')':
		tok = l.newToken(TOKEN_RPAREN, ")")
	case '[':
		tok = l.newToken(TOKEN_LBRACKET, "[")
	case ']':
		tok = l.newToken(TOKEN_RBRACKET, "]")
	case '{':
		tok = l.newToken(TOKEN_LBRACE, "{")
	case '}':
		tok = l.newToken(TOKEN_RBRACE, "}")
	case ':':
		tok = l.newToken(TOKEN_COLON, ":")
	case '\'':
		tok.Type = TOKEN_STRING
		tok.Literal = l.readString()
		tok.Pos = pos
		return tok
	case '"':
		// Quoted identifier (DuckDB/ANSI style)
		tok.Type = TOKEN_IDENT
		tok.Literal = l.readQuotedIdentifier()
		tok.Pos = pos
		return tok
	default:
		switch {
		case isLetter(l.ch) || l.ch == '_':
			tok.Literal = l.readIdentifier()
			lowerIdent := strings.ToLower(tok.Literal)
			// Check builtin keywords first
			tok.Type = LookupIdent(lowerIdent)
			// If not a builtin keyword, check dialect keywords
			if tok.Type == TOKEN_IDENT && l.dialect != nil {
				if dynTok, ok := l.dialect.LookupKeyword(lowerIdent); ok {
					tok.Type = dynTok
				}
			}
			// Fallback to dynamically registered keywords (from dialect packages)
			if tok.Type == TOKEN_IDENT {
				if dynTok, ok := token.LookupDynamicKeyword(lowerIdent); ok {
					tok.Type = dynTok
				}
			}
			tok.Pos = pos
			return tok
		case isDigit(l.ch):
			tok.Type = TOKEN_NUMBER
			tok.Literal = l.readNumber()
			tok.Pos = pos
			return tok
		default:
			tok = l.newToken(TOKEN_ILLEGAL, string(l.ch))
		}
	}

	l.readChar()
	return tok
}

// matchDialectSymbol checks if the current position matches a dialect-specific symbol.
// Returns the longest matching symbol (e.g., "::" before ":").
func (l *Lexer) matchDialectSymbol(pos Position) (Token, bool) {
	if l.dialect == nil {
		return Token{}, false
	}

	symbols := l.dialect.SymbolsMap()
	if len(symbols) == 0 {
		return Token{}, false
	}

	// Safety check: don't try to match if we're at or past end of input
	if l.pos >= len(l.input) {
		return Token{}, false
	}

	remaining := l.input[l.pos:]

	// Find all matching symbols
	var matches []string
	for sym := range symbols {
		if strings.HasPrefix(remaining, sym) {
			matches = append(matches, sym)
		}
	}

	if len(matches) == 0 {
		return Token{}, false
	}

	// Sort by length descending (longest match first)
	sort.Slice(matches, func(i, j int) bool {
		return len(matches[i]) > len(matches[j])
	})

	symbol := matches[0]
	tokenType := symbols[symbol]

	// Consume the symbol characters
	for range symbol {
		l.readChar()
	}

	return Token{Type: tokenType, Literal: symbol, Pos: pos}, true
}

// newToken creates a new token.
func (l *Lexer) newToken(tokenType TokenType, literal string) Token {
	return Token{Type: tokenType, Literal: literal, Pos: l.currentPos()}
}

// skipWhitespaceAndComments skips whitespace and collects comments.
func (l *Lexer) skipWhitespaceAndComments() {
	for {
		// Skip whitespace
		for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
			l.readChar()
		}

		// Collect line comment (-- ...)
		if l.ch == '-' && l.peekChar() == '-' {
			l.collectLineComment()
			continue
		}

		// Collect block comment (/* ... */)
		if l.ch == '/' && l.peekChar() == '*' {
			l.collectBlockComment()
			continue
		}

		break
	}
}

// collectLineComment collects a line comment.
func (l *Lexer) collectLineComment() {
	startPos := l.currentPos()
	startOffset := l.pos

	// Consume until end of line
	for l.ch != '\n' && l.ch != 0 {
		l.readChar()
	}

	l.Comments = append(l.Comments, &token.Comment{
		Kind: token.LineComment,
		Text: l.input[startOffset:l.pos],
		Span: token.Span{Start: startPos, End: l.currentPos()},
	})
}

// collectBlockComment collects a block comment.
func (l *Lexer) collectBlockComment() {
	startPos := l.currentPos()
	startOffset := l.pos

	l.readChar() // skip '/'
	l.readChar() // skip '*'

	for l.ch != 0 {
		if l.ch == '*' && l.peekChar() == '/' {
			l.readChar() // skip '*'
			l.readChar() // skip '/'
			break
		}
		l.readChar()
	}

	l.Comments = append(l.Comments, &token.Comment{
		Kind: token.BlockComment,
		Text: l.input[startOffset:l.pos],
		Span: token.Span{Start: startPos, End: l.currentPos()},
	})
}

// readString reads a single-quoted string literal.
// Handles doubled single quotes as escape: 'it"s' -> it's
func (l *Lexer) readString() string {
	l.readChar() // skip opening quote

	var result strings.Builder
	for l.ch != 0 {
		if l.ch == '\'' {
			if l.peekChar() == '\'' {
				// Doubled quote escape
				result.WriteByte('\'')
				l.readChar() // skip first quote
				l.readChar() // skip second quote
			} else {
				// End of string
				l.readChar() // skip closing quote
				break
			}
		} else {
			result.WriteByte(l.ch)
			l.readChar()
		}
	}
	return result.String()
}

// readQuotedIdentifier reads a double-quoted identifier.
// Handles doubled double quotes as escape: "col""name" -> col"name
func (l *Lexer) readQuotedIdentifier() string {
	l.readChar() // skip opening quote

	var result strings.Builder
	for l.ch != 0 {
		if l.ch == '"' {
			if l.peekChar() == '"' {
				// Doubled quote escape
				result.WriteByte('"')
				l.readChar() // skip first quote
				l.readChar() // skip second quote
			} else {
				// End of identifier
				l.readChar() // skip closing quote
				break
			}
		} else {
			result.WriteByte(l.ch)
			l.readChar()
		}
	}
	return result.String()
}

// readIdentifier reads an unquoted identifier.
func (l *Lexer) readIdentifier() string {
	start := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[start:l.pos]
}

// readNumber reads a numeric literal (integer, decimal, or scientific).
func (l *Lexer) readNumber() string {
	start := l.pos

	// Read integer part
	for isDigit(l.ch) {
		l.readChar()
	}

	// Read decimal part
	if l.ch == '.' && isDigit(l.peekChar()) {
		l.readChar() // skip '.'
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	// Read exponent part (e.g., 1e10, 1E-5)
	if l.ch == 'e' || l.ch == 'E' {
		l.readChar() // skip 'e' or 'E'
		if l.ch == '+' || l.ch == '-' {
			l.readChar() // skip sign
		}
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	return l.input[start:l.pos]
}

// isLetter returns true if ch is a letter.
func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}

// isDigit returns true if ch is a digit.
func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// Tokenize returns all tokens from the input.
func Tokenize(input string) []Token {
	l := NewLexer(input)
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TOKEN_EOF {
			break
		}
	}
	return tokens
}

// readMacro scans a {{ ... }} macro token.
// Handles nested braces and skips over quoted strings to avoid
// miscounting braces inside string literals.
func (l *Lexer) readMacro(startPos Position) Token {
	startOffset := l.pos
	l.readChar() // skip first {
	l.readChar() // skip second {

	depth := 1
	for l.ch != 0 && depth > 0 {
		switch l.ch {
		case '\'', '"':
			// Skip quoted strings to avoid counting braces inside them
			l.skipQuotedInMacro(l.ch)
		case '{':
			if l.peekChar() == '{' {
				depth++
				l.readChar()
			}
			l.readChar()
		case '}':
			if l.peekChar() == '}' {
				depth--
				l.readChar()
				if depth == 0 {
					l.readChar() // consume final }
				}
			} else {
				l.readChar()
			}
		default:
			l.readChar()
		}
	}

	return Token{
		Type:    TOKEN_MACRO,
		Literal: l.input[startOffset:l.pos],
		Pos:     startPos,
	}
}

// skipQuotedInMacro skips over a quoted string inside a macro.
func (l *Lexer) skipQuotedInMacro(quote byte) {
	l.readChar() // skip opening quote
	for l.ch != 0 {
		if l.ch == quote {
			l.readChar() // skip closing quote
			return
		}
		if l.ch == '\\' && l.peekChar() != 0 {
			l.readChar() // skip escape
		}
		l.readChar()
	}
}
