package parser

import "fmt"

// ParseError represents a parsing error with position information.
type ParseError struct {
	Pos     Position
	Message string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at line %d, column %d: %s", e.Pos.Line, e.Pos.Column, e.Message)
}

// LexError represents a lexical analysis error.
type LexError struct {
	Pos     Position
	Message string
}

func (e *LexError) Error() string {
	return fmt.Sprintf("lexer error at line %d, column %d: %s", e.Pos.Line, e.Pos.Column, e.Message)
}

// ResolutionError represents a column/table resolution error.
type ResolutionError struct {
	Message string
}

func (e *ResolutionError) Error() string {
	return fmt.Sprintf("resolution error: %s", e.Message)
}

// Common error messages
const (
	ErrUnexpectedToken    = "unexpected token %s, expected %s"
	ErrUnterminatedString = "unterminated string literal"
	ErrInvalidNumber      = "invalid number literal"
	ErrUnknownColumn      = "unknown column %q"
	ErrUnknownTable       = "unknown table or alias %q"
	ErrAmbiguousColumn    = "ambiguous column reference %q"

	// Dialect-specific error messages
	ErrUnsupportedClause   = "%s is not supported in %s dialect"
	ErrUnsupportedOperator = "operator %s is not supported in %s dialect"
	ErrNoClauseHandler     = "no handler registered for clause %s"
)
