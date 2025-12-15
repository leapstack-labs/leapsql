package template

import (
	"strings"
	"unicode/utf8"
)

// TokenType identifies the type of token.
type TokenType int

// TokenType constants for template token types.
const (
	TokenText      TokenType = iota // Literal text (SQL)
	TokenExprStart                  // {{
	TokenExprEnd                    // }}
	TokenStmtStart                  // {*
	TokenStmtEnd                    // *}
	TokenExpr                       // Expression content (between {{ and }})
	TokenStmt                       // Statement content (between {* and *})
	TokenEOF                        // End of input
)

func (t TokenType) String() string {
	switch t {
	case TokenText:
		return "TEXT"
	case TokenExprStart:
		return "EXPR_START"
	case TokenExprEnd:
		return "EXPR_END"
	case TokenStmtStart:
		return "STMT_START"
	case TokenStmtEnd:
		return "STMT_END"
	case TokenExpr:
		return "EXPR"
	case TokenStmt:
		return "STMT"
	case TokenEOF:
		return "EOF"
	default:
		return "UNKNOWN"
	}
}

// Token represents a lexical token.
type Token struct {
	Type  TokenType
	Value string
	Pos   Position
}

// Lexer tokenizes a template string.
type Lexer struct {
	input    string
	file     string
	pos      int // current position in input
	line     int // current line number (1-based)
	col      int // current column number (1-based)
	lastLine int // line at start of current token
	lastCol  int // column at start of current token
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input, file string) *Lexer {
	return &Lexer{
		input: input,
		file:  file,
		pos:   0,
		line:  1,
		col:   1,
	}
}

// Tokenize converts the input into a slice of tokens.
func (l *Lexer) Tokenize() ([]Token, error) {
	var tokens []Token

	for {
		tok, err := l.nextToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}

	return tokens, nil
}

// nextToken returns the next token from the input.
func (l *Lexer) nextToken() (Token, error) {
	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF, Pos: l.position()}, nil
	}

	// Check for expression start {{
	if l.matchString("{{") {
		return l.scanExpression()
	}

	// Check for statement start {*
	if l.matchString("{*") {
		return l.scanStatement()
	}

	// Otherwise, scan text until we hit a delimiter or EOF
	return l.scanText()
}

// scanText scans literal text until a delimiter or EOF.
func (l *Lexer) scanText() (Token, error) {
	l.markStart()
	start := l.pos

	for l.pos < len(l.input) {
		// Check for expression or statement start
		if l.matchString("{{") || l.matchString("{*") {
			break
		}
		l.advance()
	}

	if l.pos == start {
		// No text consumed, something is wrong
		return Token{}, NewLexError(l.position(), "unexpected state in lexer")
	}

	return Token{
		Type:  TokenText,
		Value: l.input[start:l.pos],
		Pos:   l.startPosition(),
	}, nil
}

// scanExpression scans a {{ expr }} expression.
func (l *Lexer) scanExpression() (Token, error) {
	l.markStart()

	// Skip {{
	l.pos += 2
	l.col += 2

	// Skip leading whitespace
	l.skipWhitespace()

	exprStart := l.pos
	depth := 0 // Track nested braces

	for l.pos < len(l.input) {
		if l.matchString("}}") && depth == 0 {
			// Found closing delimiter
			exprEnd := l.pos

			// Trim trailing whitespace from expression
			expr := strings.TrimSpace(l.input[exprStart:exprEnd])

			// Skip }}
			l.pos += 2
			l.col += 2

			return Token{
				Type:  TokenExpr,
				Value: expr,
				Pos:   l.startPosition(),
			}, nil
		}

		// Track nested braces to handle {{ in strings or dicts
		r := l.peek()
		if r == '{' {
			depth++
		} else if r == '}' && depth > 0 {
			depth--
		}

		l.advance()
	}

	return Token{}, NewLexError(l.startPosition(), "unclosed expression: missing '}}'")
}

// scanStatement scans a {* stmt *} statement.
func (l *Lexer) scanStatement() (Token, error) {
	l.markStart()

	// Skip {*
	l.pos += 2
	l.col += 2

	// Skip leading whitespace
	l.skipWhitespace()

	stmtStart := l.pos

	for l.pos < len(l.input) {
		if l.matchString("*}") {
			// Found closing delimiter
			stmtEnd := l.pos

			// Trim trailing whitespace from statement
			stmt := strings.TrimSpace(l.input[stmtStart:stmtEnd])

			// Skip *}
			l.pos += 2
			l.col += 2

			return Token{
				Type:  TokenStmt,
				Value: stmt,
				Pos:   l.startPosition(),
			}, nil
		}
		l.advance()
	}

	return Token{}, NewLexError(l.startPosition(), "unclosed statement: missing '*}'")
}

// Helper methods

// peek returns the current rune without advancing.
func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[l.pos:])
	return r
}

// advance moves to the next rune, updating position tracking.
func (l *Lexer) advance() {
	if l.pos >= len(l.input) {
		return
	}

	r, size := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += size

	if r == '\n' {
		l.line++
		l.col = 1
	} else {
		l.col++
	}
}

// matchString checks if the input at current position matches s.
func (l *Lexer) matchString(s string) bool {
	return strings.HasPrefix(l.input[l.pos:], s)
}

// skipWhitespace skips whitespace characters.
func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		r := l.peek()
		if r != ' ' && r != '\t' {
			break
		}
		l.advance()
	}
}

// markStart records the start position for the current token.
func (l *Lexer) markStart() {
	l.lastLine = l.line
	l.lastCol = l.col
}

// position returns the current position.
func (l *Lexer) position() Position {
	return Position{File: l.file, Line: l.line, Column: l.col}
}

// startPosition returns the position where the current token started.
func (l *Lexer) startPosition() Position {
	return Position{File: l.file, Line: l.lastLine, Column: l.lastCol}
}
