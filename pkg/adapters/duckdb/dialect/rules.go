// Package dialect provides the DuckDB SQL dialect definition.
// This file contains DuckDB-specific lint rules.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

// QualifyWithoutWindow warns when QUALIFY is used but no window function
// is present in the SELECT list or QUALIFY clause.
var QualifyWithoutWindow = lint.RuleDef{
	ID:          "duckdb/qualify-without-window",
	Description: "QUALIFY requires a window function",
	Severity:    lint.SeverityError,
	Check:       checkQualifyWithoutWindow,
}

func checkQualifyWithoutWindow(stmt any, d lint.DialectInfo) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt == nil {
		return nil
	}

	if selectStmt.Body == nil || selectStmt.Body.Left == nil {
		return nil
	}

	core := selectStmt.Body.Left
	if core.Qualify == nil {
		return nil
	}

	// Check if any window function exists in SELECT or QUALIFY
	hasWindow := containsWindowFunction(core.Columns, d) ||
		containsWindowFunctionExpr(core.Qualify, d)

	if !hasWindow {
		return []lint.Diagnostic{{
			RuleID:   "duckdb/qualify-without-window",
			Severity: lint.SeverityError,
			Message:  "QUALIFY clause requires a window function in SELECT or QUALIFY expression",
			Pos:      core.Span.Start,
		}}
	}

	return nil
}

// containsWindowFunction checks if any column contains a window function.
func containsWindowFunction(columns []parser.SelectItem, d lint.DialectInfo) bool {
	for _, col := range columns {
		if col.Expr != nil && containsWindowFunctionExpr(col.Expr, d) {
			return true
		}
	}
	return false
}

// containsWindowFunctionExpr recursively checks if an expression contains a window function.
func containsWindowFunctionExpr(expr parser.Expr, d lint.DialectInfo) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *parser.FuncCall:
		// A function with Window (OVER clause) is a window function
		if e.Window != nil {
			return true
		}
		// Also check if it's a built-in window function (like row_number)
		if d.IsWindow(e.Name) {
			return true
		}
		// Check arguments
		for _, arg := range e.Args {
			if containsWindowFunctionExpr(arg, d) {
				return true
			}
		}
	case *parser.BinaryExpr:
		return containsWindowFunctionExpr(e.Left, d) || containsWindowFunctionExpr(e.Right, d)
	case *parser.UnaryExpr:
		return containsWindowFunctionExpr(e.Expr, d)
	case *parser.CaseExpr:
		if containsWindowFunctionExpr(e.Operand, d) {
			return true
		}
		for _, when := range e.Whens {
			if containsWindowFunctionExpr(when.Condition, d) || containsWindowFunctionExpr(when.Result, d) {
				return true
			}
		}
		return containsWindowFunctionExpr(e.Else, d)
	case *parser.CastExpr:
		return containsWindowFunctionExpr(e.Expr, d)
	case *parser.ParenExpr:
		return containsWindowFunctionExpr(e.Expr, d)
	case *parser.InExpr:
		if containsWindowFunctionExpr(e.Expr, d) {
			return true
		}
		for _, v := range e.Values {
			if containsWindowFunctionExpr(v, d) {
				return true
			}
		}
	case *parser.BetweenExpr:
		return containsWindowFunctionExpr(e.Expr, d) ||
			containsWindowFunctionExpr(e.Low, d) ||
			containsWindowFunctionExpr(e.High, d)
	case *parser.IsNullExpr:
		return containsWindowFunctionExpr(e.Expr, d)
	case *parser.IsBoolExpr:
		return containsWindowFunctionExpr(e.Expr, d)
	case *parser.LikeExpr:
		return containsWindowFunctionExpr(e.Expr, d) || containsWindowFunctionExpr(e.Pattern, d)
	}

	return false
}

// AllRules contains all DuckDB-specific lint rules.
var AllRules = []lint.RuleDef{
	QualifyWithoutWindow,
}
