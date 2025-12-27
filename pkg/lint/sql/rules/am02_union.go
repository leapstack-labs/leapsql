package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(UnionDistinct)
}

// UnionDistinct warns about using UNION without ALL (implicit DISTINCT).
var UnionDistinct = sql.RuleDef{
	ID:          "AM02",
	Name:        "ambiguous.union",
	Group:       "ambiguous",
	Description: "UNION without ALL performs implicit DISTINCT which may be unintended.",
	Severity:    lint.SeverityInfo,
	Check:       checkUnionDistinct,

	Rationale: `UNION (without ALL) automatically removes duplicate rows, which has 
performance implications and may not be the intended behavior. Explicitly using 
UNION ALL or UNION DISTINCT makes the intent clear and avoids accidental deduplication.`,

	BadExample: `SELECT name FROM customers
UNION
SELECT name FROM suppliers`,

	GoodExample: `-- If duplicates should be removed:
SELECT name FROM customers
UNION DISTINCT
SELECT name FROM suppliers

-- If duplicates should be kept:
SELECT name FROM customers
UNION ALL
SELECT name FROM suppliers`,

	Fix: "Use UNION ALL if duplicates are acceptable, or UNION DISTINCT to make deduplication explicit.",
}

func checkUnionDistinct(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt == nil {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, body := range ast.CollectSelectBodies(selectStmt) {
		if body == nil {
			continue
		}
		// Check for UNION without ALL
		if body.Op == parser.SetOpUnion && !body.All {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AM02",
				Severity:         lint.SeverityInfo,
				Message:          "UNION without ALL performs implicit DISTINCT; use UNION ALL if duplicates are acceptable",
				Pos:              body.Span.Start,
				DocumentationURL: lint.BuildDocURL("AM02"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
