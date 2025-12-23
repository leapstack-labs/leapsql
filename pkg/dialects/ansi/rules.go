// Package ansi provides ANSI SQL dialect lint rules.
package ansi

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// SelectStarWarning warns about using SELECT * which can cause issues
// when schema changes.
var SelectStarWarning = lint.RuleDef{
	ID:          "ansi/select-star",
	Description: "SELECT * can cause unexpected results when schema changes",
	Severity:    lint.SeverityInfo,
	Check:       checkSelectStar,
}

func checkSelectStar(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt == nil {
		return nil
	}

	if selectStmt.Body == nil || selectStmt.Body.Left == nil {
		return nil
	}

	var diagnostics []lint.Diagnostic
	core := selectStmt.Body.Left

	for _, col := range core.Columns {
		if col.Star {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "ansi/select-star",
				Severity: lint.SeverityInfo,
				Message:  "SELECT * can cause unexpected results when schema changes; consider listing columns explicitly",
				Pos:      core.Span.Start,
			})
			break // Only report once per query
		}
	}

	return diagnostics
}

// LimitWithoutOrderBy warns when a query has LIMIT without ORDER BY
// which can lead to non-deterministic results.
var LimitWithoutOrderBy = lint.RuleDef{
	ID:          "ansi/limit-without-order",
	Description: "LIMIT without ORDER BY produces non-deterministic results",
	Severity:    lint.SeverityWarning,
	Check:       checkLimitWithoutOrderBy,
}

func checkLimitWithoutOrderBy(stmt any, d lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	// Only check if dialect supports LIMIT clause
	if !d.IsClauseToken(token.LIMIT) {
		return nil
	}

	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt == nil {
		return nil
	}

	if selectStmt.Body == nil || selectStmt.Body.Left == nil {
		return nil
	}

	core := selectStmt.Body.Left

	// If there's a LIMIT but no ORDER BY, warn
	if core.Limit != nil && len(core.OrderBy) == 0 {
		return []lint.Diagnostic{{
			RuleID:   "ansi/limit-without-order",
			Severity: lint.SeverityWarning,
			Message:  "LIMIT without ORDER BY produces non-deterministic results",
			Pos:      core.Span.Start,
		}}
	}

	return nil
}

// ImplicitCrossJoin warns about comma-separated tables in FROM clause
// which creates an implicit cross join.
var ImplicitCrossJoin = lint.RuleDef{
	ID:          "ansi/implicit-cross-join",
	Description: "Comma-separated tables create an implicit cross join",
	Severity:    lint.SeverityInfo,
	Check:       checkImplicitCrossJoin,
}

func checkImplicitCrossJoin(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt == nil {
		return nil
	}

	if selectStmt.Body == nil || selectStmt.Body.Left == nil {
		return nil
	}

	core := selectStmt.Body.Left
	if core.From == nil {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, join := range core.From.Joins {
		if join.Type == parser.JoinComma {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "ansi/implicit-cross-join",
				Severity: lint.SeverityInfo,
				Message:  "Comma-separated tables create an implicit cross join; consider using explicit JOIN syntax",
				Pos:      join.Span.Start,
			})
		}
	}

	return diagnostics
}

// AllRules contains all ANSI lint rules.
var AllRules = []lint.RuleDef{
	SelectStarWarning,
	LimitWithoutOrderBy,
	ImplicitCrossJoin,
}
