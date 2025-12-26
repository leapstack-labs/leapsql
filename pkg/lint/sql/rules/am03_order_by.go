package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(OrderByAmbiguous)
}

// OrderByAmbiguous warns about ORDER BY in set operations with ambiguous columns.
var OrderByAmbiguous = sql.RuleDef{
	ID:          "AM03",
	Name:        "ambiguous.order_by",
	Group:       "ambiguous",
	Description: "ORDER BY column may be ambiguous in set operation.",
	Severity:    lint.SeverityWarning,
	Check:       checkOrderByAmbiguous,
}

func checkOrderByAmbiguous(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt == nil {
		return nil
	}

	// Only check if this is a set operation
	bodies := ast.CollectSelectBodies(selectStmt)
	hasSetOp := false
	for _, body := range bodies {
		if body != nil && body.Op != parser.SetOpNone {
			hasSetOp = true
			break
		}
	}
	if !hasSetOp {
		return nil
	}

	// Check for ORDER BY with unqualified column names
	core := ast.GetSelectCore(selectStmt)
	if core == nil || len(core.OrderBy) == 0 {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, item := range core.OrderBy {
		// If ORDER BY uses a column reference without table qualifier, it's ambiguous
		if colRef, ok := item.Expr.(*parser.ColumnRef); ok && colRef.Table == "" {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AM03",
				Severity:         lint.SeverityWarning,
				Message:          "ORDER BY column '" + colRef.Column + "' may be ambiguous in set operation; consider using column position",
				DocumentationURL: lint.BuildDocURL("AM03"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
