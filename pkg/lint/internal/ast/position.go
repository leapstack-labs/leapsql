package ast

import (
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// GetTableRefPosition returns the position of a table reference.
func GetTableRefPosition(ref parser.TableRef) token.Position {
	if ref == nil {
		return token.Position{}
	}
	switch t := ref.(type) {
	case *parser.TableName:
		return t.Span.Start
	case *parser.DerivedTable:
		return t.Span.Start
	case *parser.LateralTable:
		return t.Span.Start
	}
	return token.Position{}
}

// GetJoinPosition returns the position of a join.
func GetJoinPosition(join *parser.Join) token.Position {
	if join == nil {
		return token.Position{}
	}
	return join.Span.Start
}

// GetSelectCorePosition returns the position of a SelectCore.
func GetSelectCorePosition(core *parser.SelectCore) token.Position {
	if core == nil {
		return token.Position{}
	}
	return core.Span.Start
}

// GetCTEPosition returns the position of a CTE.
func GetCTEPosition(cte *parser.CTE) token.Position {
	if cte == nil {
		return token.Position{}
	}
	return cte.Span.Start
}

// GetFromClausePosition returns the position of a FROM clause.
func GetFromClausePosition(from *parser.FromClause) token.Position {
	if from == nil {
		return token.Position{}
	}
	return from.Span.Start
}

// GetSelectBodyPosition returns the position of a SelectBody.
func GetSelectBodyPosition(body *parser.SelectBody) token.Position {
	if body == nil {
		return token.Position{}
	}
	return body.Span.Start
}

// GetWithClausePosition returns the position of a WITH clause.
func GetWithClausePosition(with *parser.WithClause) token.Position {
	if with == nil {
		return token.Position{}
	}
	return with.Span.Start
}
