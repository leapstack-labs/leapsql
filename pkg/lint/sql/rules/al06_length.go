package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(AliasLength)
}

// AliasLength enforces alias length constraints.
var AliasLength = sql.RuleDef{
	ID:          "AL06",
	Name:        "aliasing.length",
	Group:       "aliasing",
	Description: "Alias length should be between min and max characters.",
	Severity:    lint.SeverityInfo,
	ConfigKeys:  []string{"min_length", "max_length"},
	Check:       checkAliasLength,

	Rationale: `Overly short aliases (single letters) lack meaning and make queries 
harder to understand. Overly long aliases add verbosity without improving clarity 
and may exceed database identifier limits. Balance brevity with descriptiveness.`,

	BadExample: `SELECT a.customer_name, b.order_total
FROM customers_with_active_subscriptions_table a
JOIN order_history_last_30_days b ON b.customer_id = a.id`,

	GoodExample: `SELECT cust.customer_name, orders.order_total
FROM customers_with_active_subscriptions_table cust
JOIN order_history_last_30_days orders ON orders.customer_id = cust.id`,

	Fix: "Choose aliases that are descriptive but concise, typically 2-10 characters. Use meaningful abbreviations.",
}

const (
	defaultMinLength = 1
	defaultMaxLength = 30
)

func checkAliasLength(stmt any, _ lint.DialectInfo, opts map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	minLen := lint.GetIntOption(opts, "min_length", defaultMinLength)
	maxLen := lint.GetIntOption(opts, "max_length", defaultMaxLength)

	var diagnostics []lint.Diagnostic

	// Check table aliases
	for _, ref := range ast.CollectTableRefs(selectStmt) {
		var alias string
		pos := ast.GetTableRefPosition(ref)
		switch t := ref.(type) {
		case *parser.TableName:
			alias = t.Alias
		case *parser.DerivedTable:
			alias = t.Alias
		case *parser.LateralTable:
			alias = t.Alias
		}
		if alias != "" {
			if len(alias) < minLen {
				diagnostics = append(diagnostics, lint.Diagnostic{
					RuleID:           "AL06",
					Severity:         lint.SeverityInfo,
					Message:          "Table alias '" + alias + "' is too short; minimum length is " + string(rune('0'+minLen)),
					Pos:              pos,
					DocumentationURL: lint.BuildDocURL("AL06"),
					ImpactScore:      lint.ImpactLow.Int(),
					AutoFixable:      false,
				})
			}
			if len(alias) > maxLen {
				diagnostics = append(diagnostics, lint.Diagnostic{
					RuleID:           "AL06",
					Severity:         lint.SeverityInfo,
					Message:          "Table alias '" + alias + "' is too long; maximum length is " + string(rune('0'+maxLen)),
					Pos:              pos,
					DocumentationURL: lint.BuildDocURL("AL06"),
					ImpactScore:      lint.ImpactLow.Int(),
					AutoFixable:      false,
				})
			}
		}
	}

	// Check column aliases
	core := ast.GetSelectCore(selectStmt)
	if core != nil {
		corePos := ast.GetSelectCorePosition(core)
		for _, col := range core.Columns {
			if col.Alias != "" {
				alias := strings.TrimSpace(col.Alias)
				if len(alias) < minLen {
					diagnostics = append(diagnostics, lint.Diagnostic{
						RuleID:           "AL06",
						Severity:         lint.SeverityInfo,
						Message:          "Column alias '" + alias + "' is too short",
						Pos:              corePos,
						DocumentationURL: lint.BuildDocURL("AL06"),
						ImpactScore:      lint.ImpactLow.Int(),
						AutoFixable:      false,
					})
				}
				if len(alias) > maxLen {
					diagnostics = append(diagnostics, lint.Diagnostic{
						RuleID:           "AL06",
						Severity:         lint.SeverityInfo,
						Message:          "Column alias '" + alias + "' is too long",
						Pos:              corePos,
						DocumentationURL: lint.BuildDocURL("AL06"),
						ImpactScore:      lint.ImpactLow.Int(),
						AutoFixable:      false,
					})
				}
			}
		}
	}

	return diagnostics
}
