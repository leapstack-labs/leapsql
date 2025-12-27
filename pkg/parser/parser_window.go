package parser

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// Window specification parsing: OVER clauses, PARTITION BY, ORDER BY, frame specs.
//
// Grammar:
//
//	window_spec   → identifier | "(" [PARTITION BY expr_list] [ORDER BY order_list] [frame_spec] ")"
//	frame_spec    → (ROWS|RANGE|GROUPS) frame_extent
//	frame_extent  → BETWEEN frame_bound AND frame_bound | frame_bound
//	frame_bound   → UNBOUNDED PRECEDING | UNBOUNDED FOLLOWING | CURRENT ROW | expr PRECEDING | expr FOLLOWING

// parseWindowSpec parses a window specification.
func (p *Parser) parseWindowSpec() *core.WindowSpec {
	spec := &core.WindowSpec{}

	// Named window reference
	if p.check(TOKEN_IDENT) {
		spec.Name = p.token.Literal
		p.nextToken()
		return spec
	}

	p.expect(TOKEN_LPAREN)

	// PARTITION BY
	if p.match(TOKEN_PARTITION) {
		p.expect(TOKEN_BY)
		spec.PartitionBy = p.parseExpressionList()
	}

	// ORDER BY
	if p.match(TOKEN_ORDER) {
		p.expect(TOKEN_BY)
		spec.OrderBy = p.parseOrderByList()
	}

	// Frame specification
	if p.check(TOKEN_ROWS) || p.check(TOKEN_RANGE) || p.check(TOKEN_GROUPS) {
		spec.Frame = p.parseFrameSpec()
	}

	p.expect(TOKEN_RPAREN)
	return spec
}

// parseFrameSpec parses a window frame specification.
func (p *Parser) parseFrameSpec() *core.FrameSpec {
	frame := &core.FrameSpec{}

	// Frame type
	switch {
	case p.match(TOKEN_ROWS):
		frame.Type = core.FrameRows
	case p.match(TOKEN_RANGE):
		frame.Type = core.FrameRange
	case p.match(TOKEN_GROUPS):
		frame.Type = core.FrameGroups
	}

	// BETWEEN ... AND ...
	if p.match(TOKEN_BETWEEN) {
		frame.Start = p.parseFrameBound()
		p.expect(TOKEN_AND)
		frame.End = p.parseFrameBound()
	} else {
		// Single bound
		frame.Start = p.parseFrameBound()
	}

	return frame
}

// parseFrameBound parses a frame bound.
func (p *Parser) parseFrameBound() *core.FrameBound {
	bound := &core.FrameBound{}

	switch {
	case p.match(TOKEN_UNBOUNDED):
		if p.match(TOKEN_PRECEDING) {
			bound.Type = core.FrameUnboundedPreceding
		} else if p.match(TOKEN_FOLLOWING) {
			bound.Type = core.FrameUnboundedFollowing
		}

	case p.match(TOKEN_CURRENT):
		p.expect(TOKEN_ROW)
		bound.Type = core.FrameCurrentRow

	default:
		// N PRECEDING or N FOLLOWING
		bound.Offset = p.parseExpression()
		if p.match(TOKEN_PRECEDING) {
			bound.Type = core.FrameExprPreceding
		} else if p.match(TOKEN_FOLLOWING) {
			bound.Type = core.FrameExprFollowing
		}
	}

	return bound
}
