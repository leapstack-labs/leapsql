package references

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(ConsistentQualification)
}

// ConsistentQualification enforces consistent column qualification style.
var ConsistentQualification = lint.RuleDef{
	ID:          "RF03",
	Name:        "references.consistent",
	Group:       "references",
	Description: "Column qualification style should be consistent.",
	Severity:    lint.SeverityInfo,
	Check:       checkConsistentQualification,
}

func checkConsistentQualification(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	refs := ast.CollectColumnRefs(selectStmt)
	if len(refs) < 2 {
		return nil
	}

	// Count qualified vs unqualified
	qualified := 0
	unqualified := 0
	for _, ref := range refs {
		if ref.Table != "" {
			qualified++
		} else {
			unqualified++
		}
	}

	// Check for mixed qualification
	if qualified > 0 && unqualified > 0 {
		return []lint.Diagnostic{{
			RuleID:           "RF03",
			Severity:         lint.SeverityInfo,
			Message:          "Mixed column qualification style; some columns are qualified, others are not",
			DocumentationURL: lint.BuildDocURL("RF03"),
			ImpactScore:      lint.ImpactLow.Int(),
			AutoFixable:      false,
		}}
	}

	return nil
}
