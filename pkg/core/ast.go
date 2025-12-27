package core

import "github.com/leapstack-labs/leapsql/pkg/token"

// Node is the base interface for all AST nodes.
// This provides type safety for parser extension points (spi.ClauseHandler, etc.)
// without requiring pkg/core to import pkg/spi.
type Node interface {
	// Pos returns the position of the first character of the node.
	Pos() token.Position
	// End returns the position of the character immediately after the node.
	End() token.Position
}

// Expr is a marker interface for expression nodes.
type Expr interface {
	Node
	exprNode() // Marker method to distinguish expressions
}

// Stmt is a marker interface for statement nodes.
type Stmt interface {
	Node
	stmtNode() // Marker method to distinguish statements
}

// TableRef is a marker interface for table reference nodes.
type TableRef interface {
	Node
	tableRefNode() // Marker method to distinguish table references
}

// NodeInfo provides common fields for all AST nodes.
// Embed this in node types that need position/comment tracking.
type NodeInfo struct {
	Span             token.Span
	LeadingComments  []*token.Comment
	TrailingComments []*token.Comment
}

// Pos returns the position of the first character of the node.
func (n *NodeInfo) Pos() token.Position {
	return n.Span.Start
}

// End returns the position of the character immediately after the node.
func (n *NodeInfo) End() token.Position {
	return n.Span.End
}

// GetSpan returns the node's source span.
func (n *NodeInfo) GetSpan() token.Span {
	return n.Span
}

// AddLeadingComment adds a leading comment to the node.
func (n *NodeInfo) AddLeadingComment(c *token.Comment) {
	n.LeadingComments = append(n.LeadingComments, c)
}

// AddTrailingComment adds a trailing comment to the node.
func (n *NodeInfo) AddTrailingComment(c *token.Comment) {
	n.TrailingComments = append(n.TrailingComments, c)
}
