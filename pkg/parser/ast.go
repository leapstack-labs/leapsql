package parser

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// =============================================================================
// HELPER FUNCTIONS - Position/span helpers for NodeInfo
// =============================================================================

// GetSpan returns the node's source span.
// Note: This is now a method on NodeInfo in core, but kept for compatibility.
func GetSpan(n *core.NodeInfo) token.Span {
	return n.GetSpan()
}

// AddLeadingComment adds a leading comment to the node.
// Note: This is now a method on NodeInfo in core, but kept for compatibility.
func AddLeadingComment(n *core.NodeInfo, c *token.Comment) {
	n.AddLeadingComment(c)
}

// AddTrailingComment adds a trailing comment to the node.
// Note: This is now a method on NodeInfo in core, but kept for compatibility.
func AddTrailingComment(n *core.NodeInfo, c *token.Comment) {
	n.AddTrailingComment(c)
}
