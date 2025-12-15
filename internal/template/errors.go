package template

import "fmt"

// Error is the base interface for all template errors.
type Error interface {
	error
	Position() Position
}

// baseError provides common error functionality.
type baseError struct {
	pos Position
	msg string
}

func (e *baseError) Position() Position { return e.pos }
func (e *baseError) Error() string {
	if e.pos.File != "" {
		return fmt.Sprintf("%s:%d:%d: %s", e.pos.File, e.pos.Line, e.pos.Column, e.msg)
	}
	return fmt.Sprintf("%d:%d: %s", e.pos.Line, e.pos.Column, e.msg)
}

// LexError represents an error during lexical analysis.
type LexError struct {
	baseError
}

// NewLexError creates a new lexer error.
func NewLexError(pos Position, msg string) *LexError {
	return &LexError{baseError: baseError{pos: pos, msg: msg}}
}

// ParseError represents an error during parsing.
type ParseError struct {
	baseError
}

// NewParseError creates a new parser error.
func NewParseError(pos Position, msg string) *ParseError {
	return &ParseError{baseError: baseError{pos: pos, msg: msg}}
}

// NewParseErrorf creates a new parser error with formatting.
func NewParseErrorf(pos Position, format string, args ...any) *ParseError {
	return &ParseError{baseError: baseError{pos: pos, msg: fmt.Sprintf(format, args...)}}
}

// RenderError represents an error during template rendering.
type RenderError struct {
	baseError
	Cause error // Underlying Starlark error, if any
}

// NewRenderError creates a new render error.
func NewRenderError(pos Position, msg string) *RenderError {
	return &RenderError{baseError: baseError{pos: pos, msg: msg}}
}

// NewRenderErrorf creates a new render error with formatting.
func NewRenderErrorf(pos Position, format string, args ...any) *RenderError {
	return &RenderError{baseError: baseError{pos: pos, msg: fmt.Sprintf(format, args...)}}
}

// WrapRenderError wraps an underlying error as a render error.
func WrapRenderError(pos Position, msg string, cause error) *RenderError {
	return &RenderError{
		baseError: baseError{pos: pos, msg: msg},
		Cause:     cause,
	}
}

func (e *RenderError) Error() string {
	base := e.baseError.Error()
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", base, e.Cause)
	}
	return base
}

func (e *RenderError) Unwrap() error {
	return e.Cause
}

// UnmatchedBlockError indicates a control flow block without its closing counterpart.
type UnmatchedBlockError struct {
	baseError
	BlockKind StmtKind // The kind of block that was unmatched
}

// NewUnmatchedBlockError creates a new unmatched block error.
func NewUnmatchedBlockError(pos Position, kind StmtKind) *UnmatchedBlockError {
	var msg string
	switch kind {
	case StmtFor:
		msg = "unclosed 'for' block (missing 'endfor')"
	case StmtIf:
		msg = "unclosed 'if' block (missing 'endif')"
	case StmtEndFor:
		msg = "'endfor' without matching 'for'"
	case StmtEndIf:
		msg = "'endif' without matching 'if'"
	case StmtElse:
		msg = "'else' without matching 'if'"
	case StmtElif:
		msg = "'elif' without matching 'if'"
	default:
		msg = fmt.Sprintf("unmatched block: %s", kind)
	}
	return &UnmatchedBlockError{
		baseError: baseError{pos: pos, msg: msg},
		BlockKind: kind,
	}
}
