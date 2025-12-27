package rules

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
)

func init() {
	sql.Register(ColumnCountMismatch)
}

// ColumnCountMismatch warns about mismatched column counts in set operations.
var ColumnCountMismatch = sql.RuleDef{
	ID:          "AM04",
	Name:        "ambiguous.column_count",
	Group:       "ambiguous",
	Description: "Mismatched column counts in set operation.",
	Severity:    core.SeverityError,
	Check:       checkColumnCountMismatch,

	Rationale: `Set operations (UNION, INTERSECT, EXCEPT) require all queries to have the 
same number of columns. A mismatch will cause a runtime error. This rule catches 
the issue at development time.`,

	BadExample: `SELECT id, name, email FROM customers
UNION ALL
SELECT id, name FROM suppliers`,

	GoodExample: `SELECT id, name, email FROM customers
UNION ALL
SELECT id, name, contact_email FROM suppliers`,

	Fix: "Ensure all queries in the set operation have the same number of columns.",
}

func checkColumnCountMismatch(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	// Collect all SelectCores in the statement
	cores := ast.CollectSelectCores(selectStmt)
	if len(cores) < 2 {
		return nil // No set operation
	}

	// Count columns in each core
	var diagnostics []lint.Diagnostic
	firstCount := countColumnsAM04(cores[0])

	for i := 1; i < len(cores); i++ {
		count := countColumnsAM04(cores[i])
		if count != firstCount {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AM04",
				Severity:         core.SeverityError,
				Message:          fmt.Sprintf("Column count mismatch in set operation: first query has %d columns, query %d has %d columns", firstCount, i+1, count),
				Pos:              cores[i].Span.Start,
				DocumentationURL: lint.BuildDocURL("AM04"),
				ImpactScore:      lint.ImpactCritical.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}

func countColumnsAM04(core *core.SelectCore) int {
	if core == nil {
		return 0
	}
	count := 0
	for _, col := range core.Columns {
		if col.Star || col.TableStar != "" {
			// Can't statically determine column count with *
			// Return -1 to indicate unknown
			return -1
		}
		count++
	}
	return count
}
