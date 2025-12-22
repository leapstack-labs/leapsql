package references

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(QualifyColumns)
}

// QualifyColumns recommends qualifying column references in multi-table queries.
var QualifyColumns = lint.RuleDef{
	ID:          "RF02",
	Name:        "references.qualification",
	Group:       "references",
	Description: "Qualify column references in queries with multiple tables.",
	Severity:    lint.SeverityWarning,
	Check:       checkQualifyColumns,
}

func checkQualifyColumns(stmt any, _ lint.DialectInfo) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	core := ast.GetSelectCore(selectStmt)
	if core == nil || core.From == nil {
		return nil
	}

	// Count table sources
	tableCount := 1
	tableCount += len(core.From.Joins)

	// If single table, don't require qualification
	if tableCount < 2 {
		return nil
	}

	// Find unqualified column references
	var diagnostics []lint.Diagnostic
	for _, colRef := range ast.CollectColumnRefs(selectStmt) {
		if colRef.Table == "" {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "RF02",
				Severity: lint.SeverityWarning,
				Message:  "Column '" + colRef.Column + "' should be qualified with table name in multi-table query",
			})
		}
	}
	return diagnostics
}
