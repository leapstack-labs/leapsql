package template

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_ValidInput(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantNodes int
		checkFunc func(t *testing.T, tmpl *Template)
	}{
		{
			name:      "plain text",
			input:     "SELECT * FROM users",
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				text, ok := tmpl.Nodes[0].(*TextNode)
				require.True(t, ok, "expected TextNode, got %T", tmpl.Nodes[0])
				assert.Equal(t, "SELECT * FROM users", text.Text)
			},
		},
		{
			name:      "simple expression",
			input:     "SELECT {{ column }} FROM users",
			wantNodes: 3,
			checkFunc: func(t *testing.T, tmpl *Template) {
				text1, ok := tmpl.Nodes[0].(*TextNode)
				require.True(t, ok, "node[0]: expected TextNode, got %T", tmpl.Nodes[0])
				assert.Equal(t, "SELECT ", text1.Text)

				expr, ok := tmpl.Nodes[1].(*ExprNode)
				require.True(t, ok, "node[1]: expected ExprNode, got %T", tmpl.Nodes[1])
				assert.Equal(t, "column", expr.Expr)

				text2, ok := tmpl.Nodes[2].(*TextNode)
				require.True(t, ok, "node[2]: expected TextNode, got %T", tmpl.Nodes[2])
				assert.Equal(t, " FROM users", text2.Text)
			},
		},
		{
			name: "for loop",
			input: `{* for col in columns: *}
{{ col }}
{* endfor *}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				forBlock, ok := tmpl.Nodes[0].(*ForBlock)
				require.True(t, ok, "expected ForBlock, got %T", tmpl.Nodes[0])
				assert.Equal(t, "col", forBlock.VarName)
				assert.Equal(t, "columns", forBlock.IterExpr)
				require.Len(t, forBlock.Body, 3)
				expr, ok := forBlock.Body[1].(*ExprNode)
				require.True(t, ok, "body[1]: expected ExprNode, got %T", forBlock.Body[1])
				assert.Equal(t, "col", expr.Expr)
			},
		},
		{
			name:      "for loop with list",
			input:     `{* for x in ["a", "b", "c"]: *}{{ x }}{* endfor *}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				forBlock, ok := tmpl.Nodes[0].(*ForBlock)
				require.True(t, ok, "expected ForBlock, got %T", tmpl.Nodes[0])
				assert.Equal(t, "x", forBlock.VarName)
				assert.Equal(t, `["a", "b", "c"]`, forBlock.IterExpr)
			},
		},
		{
			name: "if-else",
			input: `{* if condition: *}
yes
{* else: *}
no
{* endif *}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				ifBlock, ok := tmpl.Nodes[0].(*IfBlock)
				require.True(t, ok, "expected IfBlock, got %T", tmpl.Nodes[0])
				assert.Equal(t, "condition", ifBlock.Condition)
				assert.Len(t, ifBlock.Body, 1)
				require.NotNil(t, ifBlock.Else)
				assert.Len(t, ifBlock.Else, 1)
			},
		},
		{
			name: "if-elif",
			input: `{* if a: *}
A
{* elif b: *}
B
{* elif c: *}
C
{* endif *}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				ifBlock, ok := tmpl.Nodes[0].(*IfBlock)
				require.True(t, ok, "expected IfBlock, got %T", tmpl.Nodes[0])
				assert.Equal(t, "a", ifBlock.Condition)
				require.Len(t, ifBlock.ElseIfs, 2)
				assert.Equal(t, "b", ifBlock.ElseIfs[0].Condition)
				assert.Equal(t, "c", ifBlock.ElseIfs[1].Condition)
			},
		},
		{
			name: "if-elif-else",
			input: `{* if a: *}
A
{* elif b: *}
B
{* else: *}
C
{* endif *}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				ifBlock, ok := tmpl.Nodes[0].(*IfBlock)
				require.True(t, ok, "expected IfBlock, got %T", tmpl.Nodes[0])
				assert.Equal(t, "a", ifBlock.Condition)
				assert.Len(t, ifBlock.ElseIfs, 1)
				assert.NotNil(t, ifBlock.Else)
			},
		},
		{
			name: "nested blocks",
			input: `{* for x in items: *}
{* if x > 0: *}
{{ x }}
{* endif *}
{* endfor *}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				forBlock, ok := tmpl.Nodes[0].(*ForBlock)
				require.True(t, ok, "expected ForBlock, got %T", tmpl.Nodes[0])

				var foundIf bool
				for _, node := range forBlock.Body {
					if _, ok := node.(*IfBlock); ok {
						foundIf = true
						break
					}
				}
				assert.True(t, foundIf, "expected nested IfBlock in ForBlock body")
			},
		},
		{
			name:      "complex expression",
			input:     `{{ target.schema + "." + this.name }}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				expr, ok := tmpl.Nodes[0].(*ExprNode)
				require.True(t, ok, "expected ExprNode, got %T", tmpl.Nodes[0])
				assert.Equal(t, `target.schema + "." + this.name`, expr.Expr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl, err := ParseString(tt.input, "test.sql")
			require.NoError(t, err)
			require.Len(t, tmpl.Nodes, tt.wantNodes)
			if tt.checkFunc != nil {
				tt.checkFunc(t, tmpl)
			}
		})
	}
}

func TestParser_ForWithoutColon(t *testing.T) {
	// Both with and without colon should work
	inputs := []string{
		`{* for x in items: *}{{ x }}{* endfor *}`,
		`{* for x in items *}{{ x }}{* endfor *}`,
	}

	for _, input := range inputs {
		t.Run(input[:20]+"...", func(t *testing.T) {
			tmpl, err := ParseString(input, "test.sql")
			require.NoError(t, err, "input %q", input)

			forBlock, ok := tmpl.Nodes[0].(*ForBlock)
			require.True(t, ok, "input %q: expected ForBlock, got %T", input, tmpl.Nodes[0])
			assert.Equal(t, "x", forBlock.VarName)
		})
	}
}

func TestParser_Errors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		errType string // optional: specific error type expected
	}{
		{
			name: "unmatched for",
			input: `{* for x in items: *}
{{ x }}`,
			errType: "UnmatchedBlockError",
		},
		{
			name: "unmatched endfor",
			input: `{{ x }}
{* endfor *}`,
		},
		{
			name: "unmatched if",
			input: `{* if condition: *}
yes`,
		},
		{
			name: "unmatched else",
			input: `yes
{* else: *}
no`,
		},
		{
			name:  "invalid statement",
			input: `{* while true: *}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseString(tt.input, "test.sql")
			require.Error(t, err)

			if tt.errType == "UnmatchedBlockError" {
				_, ok := err.(*UnmatchedBlockError)
				assert.True(t, ok, "expected UnmatchedBlockError, got %T: %v", err, err)
			}
		})
	}
}
