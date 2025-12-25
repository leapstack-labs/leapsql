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
