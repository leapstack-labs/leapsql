package lineage

import (
	"strings"
	"unicode"
)

// Lexer tokenizes SQL input.
type Lexer struct {
	input   string
	pos     int  // current position in input
	readPos int  // reading position (after current char)
	ch      byte // current char under examination
	line    int  // current line number (1-based)
	col     int  // current column number (1-based)
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

	var tok Token
	tok.Pos = pos

	switch l.ch {
	case 0:
		tok.Type = TOKEN_EOF
		tok.Literal = ""
	case '+':
		tok = l.newToken(TOKEN_PLUS, "+")
	case '-':
		// Could be negative number or minus operator
		tok = l.newToken(TOKEN_MINUS, "-")
	case '*':
		tok = l.newToken(TOKEN_STAR, "*")
	case '/':
		tok = l.newToken(TOKEN_SLASH, "/")
	case '%':
		tok = l.newToken(TOKEN_PERCENT, "%")
	case '=':
		tok = l.newToken(TOKEN_EQ, "=")
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TOKEN_LE, Literal: "<=", Pos: pos}
		} else if l.peekChar() == '>' {
			l.readChar()
			tok = Token{Type: TOKEN_NE, Literal: "<>", Pos: pos}
		} else {
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
		if isLetter(l.ch) || l.ch == '_' {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(strings.ToLower(tok.Literal))
			tok.Pos = pos
			return tok
		} else if isDigit(l.ch) {
			tok.Type = TOKEN_NUMBER
			tok.Literal = l.readNumber()
			tok.Pos = pos
			return tok
		} else {
			tok = l.newToken(TOKEN_ILLEGAL, string(l.ch))
		}
	}

	l.readChar()
	return tok
}

// newToken creates a new token.
func (l *Lexer) newToken(tokenType TokenType, literal string) Token {
	return Token{Type: tokenType, Literal: literal, Pos: l.currentPos()}
}

// skipWhitespaceAndComments skips whitespace and comments.
func (l *Lexer) skipWhitespaceAndComments() {
	for {
		// Skip whitespace
		for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
			l.readChar()
		}

		// Skip line comment (-- ...)
		if l.ch == '-' && l.peekChar() == '-' {
			l.skipLineComment()
			continue
		}

		// Skip block comment (/* ... */)
		if l.ch == '/' && l.peekChar() == '*' {
			l.skipBlockComment()
			continue
		}

		break
	}
}

// skipLineComment skips a line comment.
func (l *Lexer) skipLineComment() {
	for l.ch != '\n' && l.ch != 0 {
		l.readChar()
	}
}

// skipBlockComment skips a block comment.
func (l *Lexer) skipBlockComment() {
	l.readChar() // skip '/'
	l.readChar() // skip '*'

	for {
		if l.ch == 0 {
			return // Unterminated block comment
		}
		if l.ch == '*' && l.peekChar() == '/' {
			l.readChar() // skip '*'
			l.readChar() // skip '/'
			return
		}
		l.readChar()
	}
}

// readString reads a single-quoted string literal.
// Handles doubled single quotes as escape: 'it”s' -> it's
func (l *Lexer) readString() string {
	l.readChar() // skip opening quote

	var result strings.Builder
	for {
		if l.ch == 0 {
			// Unterminated string
			break
		}
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
	for {
		if l.ch == 0 {
			// Unterminated identifier
			break
		}
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
