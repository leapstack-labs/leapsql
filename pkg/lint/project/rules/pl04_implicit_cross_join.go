package projectrules

import (
	"fmt"
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PL04",
		Name:        "implicit-cross-join",
		Group:       "lineage",
		Description: "JOINs with no visible join keys in column lineage",
		Severity:    lint.SeverityWarning,
		Check:       checkImplicitCrossJoin,

		Rationale: `When a model references multiple tables but no column expression bridges them, it may indicate 
a missing JOIN condition (Cartesian product). Cross joins are rarely intentional and can cause 
massive data explosion. This rule uses column lineage to detect potential cross-join scenarios.`,

		BadExample: `SELECT 
  o.id,
  o.amount,
  c.name  -- No column references both 'o' and 'c' tables
FROM orders o, customers c  -- Implicit cross join`,

		GoodExample: `SELECT 
  o.id,
  o.amount,
  c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id  -- Explicit join condition`,

		Fix: "Add explicit JOIN conditions between all referenced tables, or confirm that a cross join is intentional.",
	})
}

// checkImplicitCrossJoin detects potential Cartesian products (cross joins)
// by analyzing column lineage. If a model references multiple sources but
// no single column references columns from multiple sources, it may indicate
// a missing or implicit join condition.
//
// This is a LeapSQL-exclusive rule that leverages column-level lineage.
//
// Detection logic:
//  1. Identify models with 2+ distinct source tables
//  2. For each pair of sources, check if any column expression references both
//  3. If no column bridges two sources, flag as potential cross-join
//
// Limitations:
//   - May produce false positives if join logic is complex or in subqueries
//   - Requires complete column lineage information
//
// Best practice: Ensure all JOINs have explicit ON conditions.
func checkImplicitCrossJoin(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	for _, model := range ctx.Models() {
		// Need at least 2 sources and some column info
		if len(model.Sources) < 2 {
			continue
		}
		if len(model.Columns) == 0 {
			continue
		}

		// Get unique source tables from column lineage
		sourceTables := getSourceTablesFromColumns(model.Columns)
		if len(sourceTables) < 2 {
			continue
		}

		// Find pairs of sources that have no bridging columns
		unbridgedPairs := findUnbridgedSourcePairs(model.Columns, sourceTables)

		if len(unbridgedPairs) > 0 {
			// Format the pairs for display
			pairStrs := make([]string, len(unbridgedPairs))
			for i, pair := range unbridgedPairs {
				pairStrs[i] = fmt.Sprintf("(%s, %s)", pair[0], pair[1])
			}

			diagnostics = append(diagnostics, project.Diagnostic{
				RuleID:   "PL04",
				Severity: lint.SeverityWarning,
				Message: fmt.Sprintf(
					"Model '%s' may have implicit cross-join: no columns bridge sources %s; verify JOIN conditions",
					model.Name, strings.Join(pairStrs, ", ")),
				Model:            model.Path,
				FilePath:         model.FilePath,
				DocumentationURL: lint.BuildDocURL("PL04"),
				ImpactScore:      lint.ImpactHigh.Int(),
				AutoFixable:      false,
			})
		}
	}

	return diagnostics
}

// getSourceTablesFromColumns extracts unique source table names from column lineage.
func getSourceTablesFromColumns(columns []core.ColumnInfo) []string {
	tableSet := make(map[string]bool)
	for _, col := range columns {
		for _, src := range col.Sources {
			if src.Table != "" {
				tableSet[src.Table] = true
			}
		}
	}

	tables := make([]string, 0, len(tableSet))
	for t := range tableSet {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	return tables
}

// findUnbridgedSourcePairs finds pairs of source tables where no column
// references both tables (indicating no visible join key).
func findUnbridgedSourcePairs(columns []core.ColumnInfo, sources []string) [][2]string {
	// Build a map of which source pairs are bridged by at least one column
	bridged := make(map[string]bool)

	for _, col := range columns {
		// Get unique tables this column references
		colTables := make(map[string]bool)
		for _, src := range col.Sources {
			if src.Table != "" {
				colTables[src.Table] = true
			}
		}

		// If column references multiple tables, mark those pairs as bridged
		tableList := make([]string, 0, len(colTables))
		for t := range colTables {
			tableList = append(tableList, t)
		}

		// Mark all pairs from this column as bridged
		for i := 0; i < len(tableList); i++ {
			for j := i + 1; j < len(tableList); j++ {
				// Create canonical key (sorted order)
				t1, t2 := tableList[i], tableList[j]
				if t1 > t2 {
					t1, t2 = t2, t1
				}
				bridged[t1+"|"+t2] = true
			}
		}
	}

	// Find pairs that are not bridged
	var unbridged [][2]string
	for i := 0; i < len(sources); i++ {
		for j := i + 1; j < len(sources); j++ {
			t1, t2 := sources[i], sources[j]
			if t1 > t2 {
				t1, t2 = t2, t1
			}
			key := t1 + "|" + t2
			if !bridged[key] {
				unbridged = append(unbridged, [2]string{sources[i], sources[j]})
			}
		}
	}

	return unbridged
}
