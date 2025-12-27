package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(QualifyColumns)
}

// QualifyColumns recommends qualifying column references in multi-table queries.
var QualifyColumns = sql.RuleDef{
	ID:          "RF02",
	Name:        "references.qualification",
	Group:       "references",
	Description: "Qualify column references in queries with multiple tables.",
	Severity:    lint.SeverityWarning,
	Check:       checkQualifyColumns,

	Rationale: `In queries involving multiple tables, unqualified column names can be ambiguous. 
If two tables have a column with the same name, the query may fail or return unexpected results. 
Qualifying columns with table names or aliases makes the query explicit and prevents errors when schemas change.`,

	BadExample: `SELECT name, amount
FROM customers
JOIN orders ON customers.id = orders.customer_id`,

	GoodExample: `SELECT customers.name, orders.amount
FROM customers
JOIN orders ON customers.id = orders.customer_id`,

	Fix: "Prefix each column reference with its table name or alias.",
}

func checkQualifyColumns(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
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
				RuleID:           "RF02",
				Severity:         lint.SeverityWarning,
				Message:          "Column '" + colRef.Column + "' should be qualified with table name in multi-table query",
				DocumentationURL: lint.BuildDocURL("RF02"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
