package ambiguous

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(DistinctWithGroupBy)
}

// DistinctWithGroupBy detects redundant DISTINCT with GROUP BY.
var DistinctWithGroupBy = lint.RuleDef{
	ID:          "AM01",
	Name:        "ambiguous.distinct",
	Group:       "ambiguous",
	Description: "Using DISTINCT with GROUP BY is redundant.",
	Severity:    lint.SeverityWarning,
	Check:       checkDistinctWithGroupBy,
}

func checkDistinctWithGroupBy(stmt any, _ lint.DialectInfo) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	core := ast.GetSelectCore(selectStmt)
	if core == nil {
		return nil
	}

	if core.Distinct && len(core.GroupBy) > 0 {
		return []lint.Diagnostic{{
			RuleID:   "AM01",
			Severity: lint.SeverityWarning,
			Message:  "Using DISTINCT with GROUP BY is redundant; GROUP BY already produces unique rows",
			Pos:      core.Span.Start,
		}}
	}
	return nil
}
