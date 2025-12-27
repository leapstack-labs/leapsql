package rules

import (
	"regexp"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	sql.Register(ForbidAlias)
}

// ForbidAlias warns about forbidden alias patterns.
var ForbidAlias = sql.RuleDef{
	ID:          "AL07",
	Name:        "aliasing.forbid",
	Group:       "aliasing",
	Description: "Forbidden alias patterns (e.g., single letters, t1/t2).",
	Severity:    core.SeverityWarning,
	ConfigKeys:  []string{"forbidden_patterns", "forbidden_names"},
	Check:       checkForbidAlias,

	Rationale: `Generic aliases like single letters (a, b, c) or numbered tables (t1, t2) 
provide no semantic meaning. They make queries harder to understand and maintain, 
especially in complex queries with multiple joins. Use descriptive aliases instead.`,

	BadExample: `SELECT a.name, b.total, c.date
FROM customers a
JOIN orders b ON b.customer_id = a.id
JOIN shipments c ON c.order_id = b.id`,

	GoodExample: `SELECT cust.name, ord.total, ship.date
FROM customers cust
JOIN orders ord ON ord.customer_id = cust.id
JOIN shipments ship ON ship.order_id = ord.id`,

	Fix: "Replace forbidden aliases with meaningful names that describe what the table represents in this query context.",
}

// Default forbidden patterns:
// - Single lowercase letter: ^[a-z]$
// - Numbered tables: ^t\d+$
var defaultForbiddenPatterns = []string{
	`^[a-z]$`,  // single lowercase letters like a, b, c
	`^t\d+$`,   // t1, t2, t3, etc.
	`^tbl\d*$`, // tbl, tbl1, tbl2, etc.
}

func checkForbidAlias(stmt any, _ lint.DialectInfo, opts map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	// Get forbidden patterns from config or use defaults
	patterns := lint.GetStringSliceOption(opts, "forbidden_patterns", defaultForbiddenPatterns)
	forbiddenNames := lint.GetStringSliceOption(opts, "forbidden_names", nil)

	// Compile patterns
	var compiledPatterns []*regexp.Regexp
	for _, p := range patterns {
		if re, err := regexp.Compile(p); err == nil {
			compiledPatterns = append(compiledPatterns, re)
		}
	}

	// Build forbidden names set (case-insensitive)
	forbiddenSet := make(map[string]bool)
	for _, name := range forbiddenNames {
		forbiddenSet[strings.ToLower(name)] = true
	}

	var diagnostics []lint.Diagnostic

	// Check table aliases
	for _, ref := range ast.CollectTableRefs(selectStmt) {
		var alias string
		pos := ast.GetTableRefPosition(ref)
		switch t := ref.(type) {
		case *core.TableName:
			alias = t.Alias
		case *core.DerivedTable:
			alias = t.Alias
		case *core.LateralTable:
			alias = t.Alias
		}
		if alias != "" {
			if diag := checkForbiddenAlias(alias, "Table", compiledPatterns, forbiddenSet, pos); diag != nil {
				diagnostics = append(diagnostics, *diag)
			}
		}
	}

	// Check column aliases
	selectCore := ast.GetSelectCore(selectStmt)
	if selectCore != nil {
		corePos := ast.GetSelectCorePosition(selectCore)
		for _, col := range selectCore.Columns {
			if col.Alias != "" {
				alias := strings.TrimSpace(col.Alias)
				if diag := checkForbiddenAlias(alias, "Column", compiledPatterns, forbiddenSet, corePos); diag != nil {
					diagnostics = append(diagnostics, *diag)
				}
			}
		}
	}

	return diagnostics
}

func checkForbiddenAlias(alias, aliasType string, patterns []*regexp.Regexp, forbiddenSet map[string]bool, pos token.Position) *lint.Diagnostic {
	lowerAlias := strings.ToLower(alias)

	// Check against forbidden names
	if forbiddenSet[lowerAlias] {
		return &lint.Diagnostic{
			RuleID:           "AL07",
			Severity:         core.SeverityWarning,
			Message:          aliasType + " alias '" + alias + "' is forbidden; use a more descriptive name",
			Pos:              pos,
			DocumentationURL: lint.BuildDocURL("AL07"),
			ImpactScore:      lint.ImpactMedium.Int(),
			AutoFixable:      false,
		}
	}

	// Check against patterns
	for _, re := range patterns {
		if re.MatchString(lowerAlias) {
			return &lint.Diagnostic{
				RuleID:           "AL07",
				Severity:         core.SeverityWarning,
				Message:          aliasType + " alias '" + alias + "' matches forbidden pattern; use a more descriptive name",
				Pos:              pos,
				DocumentationURL: lint.BuildDocURL("AL07"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			}
		}
	}

	return nil
}
