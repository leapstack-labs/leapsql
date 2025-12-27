package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(SimpleCaseConversion)
}

// SimpleCaseConversion suggests converting searched CASE to simple CASE when possible.
var SimpleCaseConversion = sql.RuleDef{
	ID:          "ST02",
	Name:        "structure.simple_case",
	Group:       "structure",
	Description: "Searched CASE can be simplified to simple CASE expression.",
	Severity:    lint.SeverityHint,
	Check:       checkSimpleCaseConversion,

	Rationale: `When all WHEN conditions compare the same column to different literal values using equality, 
a searched CASE can be rewritten as a simple CASE. Simple CASE expressions are more concise and clearly 
communicate the intent of mapping values from a single column.`,

	BadExample: `SELECT
  CASE
    WHEN status = 'A' THEN 'Active'
    WHEN status = 'I' THEN 'Inactive'
    WHEN status = 'P' THEN 'Pending'
  END AS status_label
FROM orders`,

	GoodExample: `SELECT
  CASE status
    WHEN 'A' THEN 'Active'
    WHEN 'I' THEN 'Inactive'
    WHEN 'P' THEN 'Pending'
  END AS status_label
FROM orders`,

	Fix: "Convert to simple CASE by moving the common column after CASE and removing it from WHEN clauses.",
}

func checkSimpleCaseConversion(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, caseExpr := range ast.CollectCaseExprs(selectStmt) {
		// Skip if already a simple CASE (has operand)
		if caseExpr.Operand != nil {
			continue
		}

		// Check if all WHEN conditions compare the same column to a literal
		if canConvertToSimpleCaseST02(caseExpr) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST02",
				Severity:         lint.SeverityHint,
				Message:          "Searched CASE expression can be converted to simple CASE for better readability",
				DocumentationURL: lint.BuildDocURL("ST02"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}

func canConvertToSimpleCaseST02(caseExpr *parser.CaseExpr) bool {
	if len(caseExpr.Whens) < 2 {
		return false
	}

	var commonColumn string

	for _, when := range caseExpr.Whens {
		// Check if condition is an equality comparison
		binExpr, ok := when.Condition.(*parser.BinaryExpr)
		if !ok {
			return false
		}

		// Must be equality comparison (=)
		if binExpr.Op.String() != "=" {
			return false
		}

		// One side should be a column ref, the other a literal
		var colRef *parser.ColumnRef
		var hasLiteral bool

		if cr, ok := binExpr.Left.(*parser.ColumnRef); ok {
			colRef = cr
			_, hasLiteral = binExpr.Right.(*parser.Literal)
		} else if cr, ok := binExpr.Right.(*parser.ColumnRef); ok {
			colRef = cr
			_, hasLiteral = binExpr.Left.(*parser.Literal)
		}

		if colRef == nil || !hasLiteral {
			return false
		}

		colName := colRef.Column
		if colRef.Table != "" {
			colName = colRef.Table + "." + colName
		}

		if commonColumn == "" {
			commonColumn = colName
		} else if commonColumn != colName {
			return false // Different columns in different WHENs
		}
	}

	return commonColumn != ""
}
