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
