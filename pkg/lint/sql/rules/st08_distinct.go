package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(DistinctVsGroupBy)
}

// DistinctVsGroupBy suggests using GROUP BY instead of DISTINCT for aggregates.
var DistinctVsGroupBy = sql.RuleDef{
	ID:          "ST08",
	Name:        "structure.distinct",
	Group:       "structure",
	Description: "Consider GROUP BY instead of DISTINCT when selecting columns for aggregation.",
	Severity:    lint.SeverityInfo,
	Check:       checkDistinctVsGroupBy,

	Rationale: `Using GROUP BY instead of DISTINCT on simple column selections makes the query's intent 
clearer and positions the code better for future aggregation needs. GROUP BY explicitly shows which 
columns define the unique rows, while DISTINCT can be ambiguous in complex queries.`,

	BadExample: `SELECT DISTINCT department, location
FROM employees`,

	GoodExample: `SELECT department, location
FROM employees
GROUP BY department, location`,

	Fix: "Replace SELECT DISTINCT with GROUP BY on the same columns.",
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
