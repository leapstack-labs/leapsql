package parser

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// =============================================================================
// TYPE ALIASES - All AST types are now defined in pkg/core
// These aliases provide backwards compatibility for existing code.
// =============================================================================

// SelectStmt and related statement types are aliased from core.
//
//nolint:revive // Type alias group - comment documents the group
type (
	SelectStmt  = core.SelectStmt
	WithClause  = core.WithClause
	CTE         = core.CTE
	SelectBody  = core.SelectBody
	SelectCore  = core.SelectCore
	FetchClause = core.FetchClause
	WindowDef   = core.WindowDef
	SelectItem  = core.SelectItem
	FromClause  = core.FromClause
	Join        = core.Join
	OrderByItem = core.OrderByItem
)

// SetOpType represents set operation types (UNION, INTERSECT, EXCEPT).
type SetOpType = core.SetOpType

// Set operation constants
const (
	SetOpNone      = core.SetOpNone
	SetOpUnion     = core.SetOpUnion
	SetOpUnionAll  = core.SetOpUnionAll
	SetOpIntersect = core.SetOpIntersect
	SetOpExcept    = core.SetOpExcept
)

// JoinType represents the type of JOIN operation.
type JoinType = core.JoinType

// JoinComma constant
const JoinComma = core.JoinComma

// ColumnRef and related expression types are aliased from core.
//
//nolint:revive // Type alias group - comment documents the group
type (
	ColumnRef     = core.ColumnRef
	Literal       = core.Literal
	BinaryExpr    = core.BinaryExpr
	UnaryExpr     = core.UnaryExpr
	FuncCall      = core.FuncCall
	WindowSpec    = core.WindowSpec
	FrameSpec     = core.FrameSpec
	FrameBound    = core.FrameBound
	CaseExpr      = core.CaseExpr
	WhenClause    = core.WhenClause
	CastExpr      = core.CastExpr
	InExpr        = core.InExpr
	BetweenExpr   = core.BetweenExpr
	IsNullExpr    = core.IsNullExpr
	IsBoolExpr    = core.IsBoolExpr
	LikeExpr      = core.LikeExpr
	ParenExpr     = core.ParenExpr
	StarExpr      = core.StarExpr
	SubqueryExpr  = core.SubqueryExpr
	ExistsExpr    = core.ExistsExpr
	MacroExpr     = core.MacroExpr
	LambdaExpr    = core.LambdaExpr
	StructLiteral = core.StructLiteral
	StructField   = core.StructField
	ListLiteral   = core.ListLiteral
	IndexExpr     = core.IndexExpr
)

// LiteralType represents the type of a literal value.
type LiteralType = core.LiteralType

// Literal type constants
const (
	LiteralNumber = core.LiteralNumber
	LiteralString = core.LiteralString
	LiteralBool   = core.LiteralBool
	LiteralNull   = core.LiteralNull
)

// FrameType represents window frame types (ROWS, RANGE, GROUPS).
type FrameType = core.FrameType

// FrameBoundType represents window frame bound types.
type FrameBoundType = core.FrameBoundType

// Frame type constants
const (
	FrameRows   = core.FrameRows
	FrameRange  = core.FrameRange
	FrameGroups = core.FrameGroups
)

// Frame bound type constants
const (
	FrameUnboundedPreceding = core.FrameUnboundedPreceding
	FrameUnboundedFollowing = core.FrameUnboundedFollowing
	FrameCurrentRow         = core.FrameCurrentRow
	FrameExprPreceding      = core.FrameExprPreceding
	FrameExprFollowing      = core.FrameExprFollowing
)

// TableName and related table reference types are aliased from core.
//
//nolint:revive // Type alias group - comment documents the group
type (
	TableName      = core.TableName
	DerivedTable   = core.DerivedTable
	LateralTable   = core.LateralTable
	MacroTable     = core.MacroTable
	PivotTable     = core.PivotTable
	PivotAggregate = core.PivotAggregate
	PivotInValue   = core.PivotInValue
	UnpivotTable   = core.UnpivotTable
	UnpivotInGroup = core.UnpivotInGroup
)

// NodeInfo and related node types/interfaces are aliased from core.
//
//nolint:revive // Type alias group - comment documents the group
type (
	NodeInfo = core.NodeInfo
	Node     = core.Node
	Expr     = core.Expr
	Stmt     = core.Stmt
	TableRef = core.TableRef
)

// StarModifier and related star modifier types are aliased from core.
//
//nolint:revive // Type alias group - comment documents the group
type (
	StarModifier    = core.StarModifier
	ExcludeModifier = core.ExcludeModifier
	ReplaceItem     = core.ReplaceItem
	ReplaceModifier = core.ReplaceModifier
	RenameItem      = core.RenameItem
	RenameModifier  = core.RenameModifier
)

// =============================================================================
// INTERFACES - These define what types implement for compatibility checks
// =============================================================================

// Statement represents a SQL statement.
//
// Deprecated: Use core.Stmt instead.
type Statement interface {
	stmtNode()
}

// =============================================================================
// HELPER FUNCTIONS - Position/span helpers for NodeInfo
// =============================================================================

// GetSpan returns the node's source span.
// Note: This is now a method on NodeInfo in core, but kept for compatibility.
func GetSpan(n *NodeInfo) token.Span {
	return n.GetSpan()
}

// AddLeadingComment adds a leading comment to the node.
// Note: This is now a method on NodeInfo in core, but kept for compatibility.
func AddLeadingComment(n *NodeInfo, c *token.Comment) {
	n.AddLeadingComment(c)
}

// AddTrailingComment adds a trailing comment to the node.
// Note: This is now a method on NodeInfo in core, but kept for compatibility.
func AddTrailingComment(n *NodeInfo, c *token.Comment) {
	n.AddTrailingComment(c)
}
