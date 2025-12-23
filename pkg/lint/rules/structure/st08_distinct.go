package structure

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(DistinctVsGroupBy)
}

// DistinctVsGroupBy suggests using GROUP BY instead of DISTINCT for aggregates.
var DistinctVsGroupBy = lint.RuleDef{
	ID:          "ST08",
	Name:        "structure.distinct",
	Group:       "structure",
	Description: "Consider GROUP BY instead of DISTINCT when selecting columns for aggregation.",
	Severity:    lint.SeverityInfo,
	Check:       checkDistinctVsGroupBy,
}

func checkDistinctVsGroupBy(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	core := ast.GetSelectCore(selectStmt)
	if core == nil || !core.Distinct {
		return nil
	}

	// If DISTINCT is used without GROUP BY and all selected columns
	// are simple column references, suggest GROUP BY
	if len(core.GroupBy) > 0 {
		return nil
	}

	allSimpleColumns := true
	for _, col := range core.Columns {
		if col.Star || col.TableStar != "" {
			allSimpleColumns = false
			break
		}
		// Check if expression is a column ref or function call
		switch col.Expr.(type) {
		case *parser.ColumnRef:
			// Simple column ref - OK
		case *parser.FuncCall:
			// If there are aggregate functions, this is a hint
			return nil
		default:
			allSimpleColumns = false
		}
	}

	if allSimpleColumns && len(core.Columns) > 0 {
		return []lint.Diagnostic{{
			RuleID:           "ST08",
			Severity:         lint.SeverityInfo,
			Message:          "DISTINCT on simple columns could be expressed as GROUP BY for clarity",
			Pos:              core.Span.Start,
			DocumentationURL: lint.BuildDocURL("ST08"),
			ImpactScore:      lint.ImpactLow.Int(),
			AutoFixable:      false,
		}}
	}

	return nil
}
