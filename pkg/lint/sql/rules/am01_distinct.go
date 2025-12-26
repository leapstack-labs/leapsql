package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(DistinctWithGroupBy)
}

// DistinctWithGroupBy detects redundant DISTINCT with GROUP BY.
var DistinctWithGroupBy = sql.RuleDef{
	ID:          "AM01",
	Name:        "ambiguous.distinct",
	Group:       "ambiguous",
	Description: "Using DISTINCT with GROUP BY is redundant.",
	Severity:    lint.SeverityWarning,
	Check:       checkDistinctWithGroupBy,
}

func checkDistinctWithGroupBy(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
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
			RuleID:           "AM01",
			Severity:         lint.SeverityWarning,
			Message:          "Using DISTINCT with GROUP BY is redundant; GROUP BY already produces unique rows",
			Pos:              core.Span.Start,
			DocumentationURL: lint.BuildDocURL("AM01"),
			ImpactScore:      lint.ImpactMedium.Int(),
			AutoFixable:      false,
		}}
	}
	return nil
}
