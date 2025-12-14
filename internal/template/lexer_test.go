package template

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLexer_PlainText(t *testing.T) {
	input := "SELECT * FROM users"
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	require.Len(t, tokens, 2, "expected 2 tokens") // TEXT + EOF

	assert.Equal(t, TokenText, tokens[0].Type, "expected TEXT")
	assert.Equal(t, input, tokens[0].Value, "expected input value")
	assert.Equal(t, TokenEOF, tokens[1].Type, "expected EOF")
}

func TestLexer_SimpleExpression(t *testing.T) {
	input := "SELECT {{ column }} FROM users"
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenText, "SELECT "},
		{TokenExpr, "column"},
		{TokenText, " FROM users"},
		{TokenEOF, ""},
	}

	require.Len(t, tokens, len(expected), "wrong number of tokens")

	for i, exp := range expected {
		assert.Equal(t, exp.typ, tokens[i].Type, "token[%d] type", i)
		if exp.typ != TokenEOF {
			assert.Equal(t, exp.val, tokens[i].Value, "token[%d] value", i)
		}
	}
}

func TestLexer_MultipleExpressions(t *testing.T) {
	input := "{{ a }} + {{ b }}"
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenExpr, "a"},
		{TokenText, " + "},
		{TokenExpr, "b"},
		{TokenEOF, ""},
	}

	require.Len(t, tokens, len(expected), "wrong number of tokens")

	for i, exp := range expected {
		assert.Equal(t, exp.typ, tokens[i].Type, "token[%d] type", i)
	}
}

func TestLexer_Statement(t *testing.T) {
	input := "{* for x in items: *}"
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	require.Len(t, tokens, 2, "expected 2 tokens") // STMT + EOF

	assert.Equal(t, TokenStmt, tokens[0].Type, "expected STMT")
	assert.Equal(t, "for x in items:", tokens[0].Value, "expected statement value")
}

func TestLexer_ForLoop(t *testing.T) {
	input := `SELECT
{* for col in columns: *}
    {{ col }},
{* endfor *}
FROM users`
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	expectedTypes := []TokenType{
		TokenText, // "SELECT\n"
		TokenStmt, // "for col in columns:"
		TokenText, // "\n    "
		TokenExpr, // "col"
		TokenText, // ",\n"
		TokenStmt, // "endfor"
		TokenText, // "\nFROM users"
		TokenEOF,
	}

	require.Len(t, tokens, len(expectedTypes), "wrong number of tokens")

	for i, exp := range expectedTypes {
		assert.Equal(t, exp, tokens[i].Type, "token[%d] type", i)
	}
}

func TestLexer_IfElse(t *testing.T) {
	input := `{* if condition: *}
yes
{* else: *}
no
{* endif *}`
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	expectedTypes := []TokenType{
		TokenStmt, // "if condition:"
		TokenText, // "\nyes\n"
		TokenStmt, // "else:"
		TokenText, // "\nno\n"
		TokenStmt, // "endif"
		TokenEOF,
	}

	require.Len(t, tokens, len(expectedTypes), "wrong number of tokens")

	for i, exp := range expectedTypes {
		assert.Equal(t, exp, tokens[i].Type, "token[%d] type", i)
	}
}

func TestLexer_UnclosedExpression(t *testing.T) {
	input := "SELECT {{ column FROM users"
	lexer := NewLexer(input, "test.sql")

	_, err := lexer.Tokenize()
	require.Error(t, err, "expected error for unclosed expression")

	lexErr, ok := err.(*LexError)
	require.True(t, ok, "expected LexError, got %T", err)

	assert.Equal(t, 1, lexErr.Position().Line, "expected line 1")
}

func TestLexer_UnclosedStatement(t *testing.T) {
	input := "{* for x in items: SELECT"
	lexer := NewLexer(input, "test.sql")

	_, err := lexer.Tokenize()
	assert.Error(t, err, "expected error for unclosed statement")
}

func TestLexer_NestedBraces(t *testing.T) {
	// Expression with dict literal
	input := `{{ {"key": "value"} }}`
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	require.Len(t, tokens, 2, "expected 2 tokens") // EXPR + EOF

	assert.Equal(t, TokenExpr, tokens[0].Type, "expected EXPR")
	assert.Equal(t, `{"key": "value"}`, tokens[0].Value, "expected dict literal")
}

func TestLexer_PositionTracking(t *testing.T) {
	input := "line1\nline2\n{{ expr }}"
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	// The expression should be on line 3
	exprToken := tokens[1] // Skip first text token
	require.Equal(t, TokenExpr, exprToken.Type, "expected EXPR")
	assert.Equal(t, 3, exprToken.Pos.Line, "expected line 3")
}

func TestLexer_WhitespaceHandling(t *testing.T) {
	// Whitespace inside delimiters should be trimmed
	tests := []struct {
		input    string
		expected string
	}{
		{"{{  x  }}", "x"},
		{"{{x}}", "x"},
		{"{{  x + y  }}", "x + y"},
		{"{*  for x in y:  *}", "for x in y:"},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input, "test.sql")
		tokens, err := lexer.Tokenize()
		require.NoError(t, err, "input %q: unexpected error", tt.input)

		assert.Equal(t, tt.expected, tokens[0].Value, "input %q", tt.input)
	}
}

func TestLexer_EmptyExpression(t *testing.T) {
	input := "{{ }}"
	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	assert.Empty(t, tokens[0].Value, "expected empty string")
}

func TestLexer_ComplexTemplate(t *testing.T) {
	input := `/*---
name: test
---*/

SELECT
{* for col in ["id", "name", "email"]: *}
    {{ col }},
{* endfor *}
{* if env == "prod": *}
    created_at
{* else: *}
    *
{* endif *}
FROM {{ target.schema }}.users`

	lexer := NewLexer(input, "test.sql")

	tokens, err := lexer.Tokenize()
	require.NoError(t, err, "unexpected error")

	// Count tokens by type
	counts := make(map[TokenType]int)
	for _, tok := range tokens {
		counts[tok.Type]++
	}

	// Expressions: {{ col }}, {{ target.schema }} = 2
	assert.Equal(t, 2, counts[TokenExpr], "expected 2 expressions")

	// Statements: for, endfor, if, else, endif = 5
	assert.Equal(t, 5, counts[TokenStmt], "expected 5 statements")
}
