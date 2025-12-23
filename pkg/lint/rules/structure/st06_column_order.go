package structure

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(SelectColumnOrder)
}

// SelectColumnOrder recommends putting wildcards last in SELECT.
var SelectColumnOrder = lint.RuleDef{
	ID:          "ST06",
	Name:        "structure.column_order",
	Group:       "structure",
	Description: "Wildcards should appear last in SELECT clause.",
	Severity:    lint.SeverityHint,
	Check:       checkSelectColumnOrder,
}

func checkSelectColumnOrder(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	core := ast.GetSelectCore(selectStmt)
	if core == nil || len(core.Columns) < 2 {
		return nil
	}

	// Find first wildcard and first non-wildcard after it
	wildcardIdx := -1
	for i, col := range core.Columns {
		if col.Star || col.TableStar != "" {
			wildcardIdx = i
			break
		}
	}

	if wildcardIdx == -1 || wildcardIdx == len(core.Columns)-1 {
		return nil // No wildcard or it's already last
	}

	// Check if there are non-wildcard columns after the wildcard
	for i := wildcardIdx + 1; i < len(core.Columns); i++ {
		col := core.Columns[i]
		if !col.Star && col.TableStar == "" {
			return []lint.Diagnostic{{
				RuleID:           "ST06",
				Severity:         lint.SeverityHint,
				Message:          "Wildcards should appear last in SELECT clause for better readability",
				Pos:              core.Span.Start,
				DocumentationURL: lint.BuildDocURL("ST06"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			}}
		}
	}

	return nil
}
