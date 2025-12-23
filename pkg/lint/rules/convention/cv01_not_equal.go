package convention

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	lint.Register(NotEqualOperator)
}

// NotEqualOperator recommends consistent not-equal operator usage.
// Note: This rule is limited because the AST normalizes both <> and != to NE token.
// We recommend != over <> but cannot detect the original source syntax.
var NotEqualOperator = lint.RuleDef{
	ID:          "CV01",
	Name:        "convention.not_equal",
	Group:       "convention",
	Description: "Prefer != over <> for not equal operator.",
	Severity:    lint.SeverityHint,
	Check:       checkNotEqualOperator,
}

func checkNotEqualOperator(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	// Note: Both <> and != are tokenized as NE, so we cannot distinguish them
	// at the AST level. This rule is a placeholder for when we add source tracking.
	var diagnostics []lint.Diagnostic
	for _, binExpr := range ast.CollectBinaryExprs(selectStmt) {
		// We can only detect that NE is used, not which form
		_ = binExpr.Op == token.NE
	}
	return diagnostics
}
