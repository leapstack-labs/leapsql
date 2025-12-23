package ambiguous

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(ColumnCountMismatch)
}

// ColumnCountMismatch warns about mismatched column counts in set operations.
var ColumnCountMismatch = lint.RuleDef{
	ID:          "AM04",
	Name:        "ambiguous.column_count",
	Group:       "ambiguous",
	Description: "Mismatched column counts in set operation.",
	Severity:    lint.SeverityError,
	Check:       checkColumnCountMismatch,
}

func checkColumnCountMismatch(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
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
	firstCount := countColumns(cores[0])

	for i := 1; i < len(cores); i++ {
		count := countColumns(cores[i])
		if count != firstCount {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AM04",
				Severity:         lint.SeverityError,
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

func countColumns(core *parser.SelectCore) int {
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
