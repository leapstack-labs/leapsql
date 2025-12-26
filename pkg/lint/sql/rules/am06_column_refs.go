package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(AmbiguousColumnRef)
}

// AmbiguousColumnRef warns about unqualified column references with multiple tables.
var AmbiguousColumnRef = sql.RuleDef{
	ID:          "AM06",
	Name:        "ambiguous.column_refs",
	Group:       "ambiguous",
	Description: "Unqualified column reference may be ambiguous with multiple tables.",
	Severity:    lint.SeverityWarning,
	Check:       checkAmbiguousColumnRef,
}

func checkAmbiguousColumnRef(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	core := ast.GetSelectCore(selectStmt)
	if core == nil || core.From == nil {
		return nil
	}

	// Count table sources (including joins)
	tableCount := 1 // Start with the main source
	tableCount += len(core.From.Joins)

	// If only one table, no ambiguity possible
	if tableCount < 2 {
		return nil
	}

	// Find unqualified column references
	var diagnostics []lint.Diagnostic
	for _, colRef := range ast.CollectColumnRefs(selectStmt) {
		if colRef.Table == "" {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AM06",
				Severity:         lint.SeverityWarning,
				Message:          "Column '" + colRef.Column + "' is unqualified and may be ambiguous with multiple tables; consider adding table qualifier",
				DocumentationURL: lint.BuildDocURL("AM06"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
