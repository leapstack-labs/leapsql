// Package template provides a template processor for SQL files with Starlark expressions.
// It supports {{ expr }} for expression evaluation and {* stmt *} for control flow.
package template

// Position tracks source location for error reporting.
type Position struct {
	File   string
	Line   int
	Column int
}

// Node is the interface for all template AST nodes.
type Node interface {
	Pos() Position
	node() // marker method to restrict implementation
}

// nodeBase provides common Position handling for all nodes.
type nodeBase struct {
	pos Position
}

func (n *nodeBase) Pos() Position { return n.pos }
func (n *nodeBase) node()         {}

// TextNode represents literal SQL text (passed through unchanged).
type TextNode struct {
	nodeBase
	Text string
}

// ExprNode represents a {{ expr }} expression.
// The Expr field contains the Starlark expression source (without delimiters).
type ExprNode struct {
	nodeBase
	Expr string
}

// StmtKind identifies the type of control flow statement.
type StmtKind int

// StmtKind constants for control flow statement types.
const (
	StmtUnknown StmtKind = iota // Unknown/invalid statement
	StmtFor                     // {* for x in items: *}
	StmtEndFor                  // {* endfor *}
	StmtIf                      // {* if cond: *}
	StmtElif                    // {* elif cond: *}
	StmtElse                    // {* else: *}
	StmtEndIf                   // {* endif *}
)

func (k StmtKind) String() string {
	switch k {
	case StmtUnknown:
		return "unknown"
	case StmtFor:
		return "for"
	case StmtEndFor:
		return "endfor"
	case StmtIf:
		return "if"
	case StmtElif:
		return "elif"
	case StmtElse:
		return "else"
	case StmtEndIf:
		return "endif"
	default:
		return "unknown"
	}
}

// StmtNode represents a {* stmt *} statement (raw from lexer, before parsing into blocks).
type StmtNode struct {
	nodeBase
	Kind    StmtKind
	Expr    string // Condition (if/elif) or iterator expression (for)
	VarName string // Loop variable name (for loops only)
}

// ForBlock represents a complete for loop with its body.
// Created by the parser from StmtNode pairs.
type ForBlock struct {
	nodeBase
	VarName  string // Loop variable name
	IterExpr string // Iterator expression (evaluated by Starlark)
	Body     []Node // Nodes inside the loop
}

// IfBlock represents a complete if/elif/else conditional.
// Created by the parser from StmtNode sequences.
type IfBlock struct {
	nodeBase
	Condition string   // if condition expression
	Body      []Node   // Nodes for the if branch
	ElseIfs   []Branch // elif branches (may be empty)
	Else      []Node   // else branch (may be nil)
}

// Branch represents an elif branch.
type Branch struct {
	Condition string
	Body      []Node
	pos       Position
}

// Template represents a complete parsed template.
type Template struct {
	Nodes []Node
	File  string // Source file path
}
