package template

import (
	"testing"
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
				if !ok {
					t.Fatalf("expected TextNode, got %T", tmpl.Nodes[0])
				}
				if text.Text != "SELECT * FROM users" {
					t.Errorf("expected %q, got %q", "SELECT * FROM users", text.Text)
				}
			},
		},
		{
			name:      "simple expression",
			input:     "SELECT {{ column }} FROM users",
			wantNodes: 3,
			checkFunc: func(t *testing.T, tmpl *Template) {
				text1, ok := tmpl.Nodes[0].(*TextNode)
				if !ok {
					t.Fatalf("node[0]: expected TextNode, got %T", tmpl.Nodes[0])
				}
				if text1.Text != "SELECT " {
					t.Errorf("node[0]: expected %q, got %q", "SELECT ", text1.Text)
				}

				expr, ok := tmpl.Nodes[1].(*ExprNode)
				if !ok {
					t.Fatalf("node[1]: expected ExprNode, got %T", tmpl.Nodes[1])
				}
				if expr.Expr != "column" {
					t.Errorf("node[1]: expected %q, got %q", "column", expr.Expr)
				}

				text2, ok := tmpl.Nodes[2].(*TextNode)
				if !ok {
					t.Fatalf("node[2]: expected TextNode, got %T", tmpl.Nodes[2])
				}
				if text2.Text != " FROM users" {
					t.Errorf("node[2]: expected %q, got %q", " FROM users", text2.Text)
				}
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
				if !ok {
					t.Fatalf("expected ForBlock, got %T", tmpl.Nodes[0])
				}
				if forBlock.VarName != "col" {
					t.Errorf("expected var name 'col', got %q", forBlock.VarName)
				}
				if forBlock.IterExpr != "columns" {
					t.Errorf("expected iter expr 'columns', got %q", forBlock.IterExpr)
				}
				if len(forBlock.Body) != 3 {
					t.Fatalf("expected 3 body nodes, got %d", len(forBlock.Body))
				}
				expr, ok := forBlock.Body[1].(*ExprNode)
				if !ok {
					t.Fatalf("body[1]: expected ExprNode, got %T", forBlock.Body[1])
				}
				if expr.Expr != "col" {
					t.Errorf("body[1]: expected %q, got %q", "col", expr.Expr)
				}
			},
		},
		{
			name:      "for loop with list",
			input:     `{* for x in ["a", "b", "c"]: *}{{ x }}{* endfor *}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				forBlock, ok := tmpl.Nodes[0].(*ForBlock)
				if !ok {
					t.Fatalf("expected ForBlock, got %T", tmpl.Nodes[0])
				}
				if forBlock.VarName != "x" {
					t.Errorf("expected var name 'x', got %q", forBlock.VarName)
				}
				if forBlock.IterExpr != `["a", "b", "c"]` {
					t.Errorf("expected iter expr '[\"a\", \"b\", \"c\"]', got %q", forBlock.IterExpr)
				}
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
				if !ok {
					t.Fatalf("expected IfBlock, got %T", tmpl.Nodes[0])
				}
				if ifBlock.Condition != "condition" {
					t.Errorf("expected condition 'condition', got %q", ifBlock.Condition)
				}
				if len(ifBlock.Body) != 1 {
					t.Fatalf("expected 1 if body node, got %d", len(ifBlock.Body))
				}
				if ifBlock.Else == nil {
					t.Fatal("expected else body")
				}
				if len(ifBlock.Else) != 1 {
					t.Fatalf("expected 1 else body node, got %d", len(ifBlock.Else))
				}
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
				if !ok {
					t.Fatalf("expected IfBlock, got %T", tmpl.Nodes[0])
				}
				if ifBlock.Condition != "a" {
					t.Errorf("expected condition 'a', got %q", ifBlock.Condition)
				}
				if len(ifBlock.ElseIfs) != 2 {
					t.Fatalf("expected 2 elif branches, got %d", len(ifBlock.ElseIfs))
				}
				if ifBlock.ElseIfs[0].Condition != "b" {
					t.Errorf("elif[0]: expected condition 'b', got %q", ifBlock.ElseIfs[0].Condition)
				}
				if ifBlock.ElseIfs[1].Condition != "c" {
					t.Errorf("elif[1]: expected condition 'c', got %q", ifBlock.ElseIfs[1].Condition)
				}
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
				if !ok {
					t.Fatalf("expected IfBlock, got %T", tmpl.Nodes[0])
				}
				if ifBlock.Condition != "a" {
					t.Errorf("expected condition 'a', got %q", ifBlock.Condition)
				}
				if len(ifBlock.ElseIfs) != 1 {
					t.Fatalf("expected 1 elif branch, got %d", len(ifBlock.ElseIfs))
				}
				if ifBlock.Else == nil {
					t.Fatal("expected else body")
				}
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
				if !ok {
					t.Fatalf("expected ForBlock, got %T", tmpl.Nodes[0])
				}

				var foundIf bool
				for _, node := range forBlock.Body {
					if _, ok := node.(*IfBlock); ok {
						foundIf = true
						break
					}
				}
				if !foundIf {
					t.Error("expected nested IfBlock in ForBlock body")
				}
			},
		},
		{
			name:      "complex expression",
			input:     `{{ target.schema + "." + this.name }}`,
			wantNodes: 1,
			checkFunc: func(t *testing.T, tmpl *Template) {
				expr, ok := tmpl.Nodes[0].(*ExprNode)
				if !ok {
					t.Fatalf("expected ExprNode, got %T", tmpl.Nodes[0])
				}
				expected := `target.schema + "." + this.name`
				if expr.Expr != expected {
					t.Errorf("expected %q, got %q", expected, expr.Expr)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl, err := ParseString(tt.input, "test.sql")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tmpl.Nodes) != tt.wantNodes {
				t.Fatalf("expected %d node(s), got %d", tt.wantNodes, len(tmpl.Nodes))
			}
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
			if err != nil {
				t.Fatalf("input %q: unexpected error: %v", input, err)
			}

			forBlock, ok := tmpl.Nodes[0].(*ForBlock)
			if !ok {
				t.Fatalf("input %q: expected ForBlock, got %T", input, tmpl.Nodes[0])
			}

			if forBlock.VarName != "x" {
				t.Errorf("input %q: expected var 'x', got %q", input, forBlock.VarName)
			}
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
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if tt.errType == "UnmatchedBlockError" {
				if _, ok := err.(*UnmatchedBlockError); !ok {
					t.Errorf("expected UnmatchedBlockError, got %T: %v", err, err)
				}
			}
		})
	}
}
