package structure

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(UnusedCTE)
}

// UnusedCTE warns about CTEs that are defined but never used.
var UnusedCTE = lint.RuleDef{
	ID:          "ST03",
	Name:        "structure.unused_cte",
	Group:       "structure",
	Description: "CTE is defined but never referenced.",
	Severity:    lint.SeverityWarning,
	Check:       checkUnusedCTE,
}

func checkUnusedCTE(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt.With == nil {
		return nil
	}

	// Collect all CTE names
	cteNames := make(map[string]*parser.CTE)
	for _, cte := range selectStmt.With.CTEs {
		cteNames[strings.ToLower(cte.Name)] = cte
	}

	// Collect all referenced table names
	usedTables := make(map[string]bool)
	collectTableNames(selectStmt.Body, usedTables)

	// Also check for references within CTEs to other CTEs
	for _, cte := range selectStmt.With.CTEs {
		collectTableNames(cte.Select.Body, usedTables)
	}

	// Find unused CTEs
	var diagnostics []lint.Diagnostic
	for _, cte := range selectStmt.With.CTEs {
		if !usedTables[strings.ToLower(cte.Name)] {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "ST03",
				Severity: lint.SeverityWarning,
				Message:  "CTE '" + cte.Name + "' is defined but never referenced",
				Pos:      cte.Span.Start,
			})
		}
	}
	return diagnostics
}

func collectTableNames(body *parser.SelectBody, used map[string]bool) {
	if body == nil {
		return
	}

	ast.Walk(body, func(node any) bool {
		if tn, ok := node.(*parser.TableName); ok {
			used[strings.ToLower(tn.Name)] = true
		}
		return true
	})
}
