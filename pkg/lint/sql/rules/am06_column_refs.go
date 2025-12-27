package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
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
	Severity:    core.SeverityWarning,
	Check:       checkAmbiguousColumnRef,

	Rationale: `When multiple tables are joined, unqualified column names may exist in 
more than one table. The database may pick an unexpected source, or error out. 
Qualifying columns prevents ambiguity and makes the query self-documenting.`,

	BadExample: `SELECT name, email, created_at
FROM customers c
JOIN orders o ON o.customer_id = c.id`,

	GoodExample: `SELECT c.name, c.email, o.created_at
FROM customers c
JOIN orders o ON o.customer_id = c.id`,

	Fix: "Prefix column references with the table alias (e.g., c.name instead of name).",
}

func checkAmbiguousColumnRef(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	selectCore := ast.GetSelectCore(selectStmt)
	if selectCore == nil || selectCore.From == nil {
		return nil
	}

	// Count table sources (including joins)
	tableCount := 1 // Start with the main source
	tableCount += len(selectCore.From.Joins)

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
				Severity:         core.SeverityWarning,
				Message:          "Column '" + colRef.Column + "' is unqualified and may be ambiguous with multiple tables; consider adding table qualifier",
				DocumentationURL: lint.BuildDocURL("AM06"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
