package convention

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
)

func init() {
	lint.Register(NotEqualOperator)
}

// NotEqualOperator recommends consistent not-equal operator usage.
//
// NOT IMPLEMENTED: This rule cannot be implemented because the lexer normalizes
// both <> and != to the same NE token. The original source syntax is not preserved
// in the AST, making it impossible to detect which operator was used.
//
// To implement this rule would require:
// - Adding an OriginalOp field to BinaryExpr to track the source syntax
// - Modifying the lexer to preserve the original operator form
//
// See: plans/done/sqlfluff-linting-gaps.md for more details.
var NotEqualOperator = lint.RuleDef{
	ID:          "CV01",
	Name:        "convention.not_equal",
	Group:       "convention",
	Description: "Prefer != over <> for not equal operator (NOT IMPLEMENTED: AST normalizes both operators).",
	Severity:    lint.SeverityHint,
	Check:       checkNotEqualOperator,
}

// checkNotEqualOperator is a stub that returns no diagnostics.
// See the comment on NotEqualOperator for why this cannot be implemented.
func checkNotEqualOperator(_ any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	// NOT IMPLEMENTED: The lexer normalizes both <> and != to NE token.
	// We cannot detect which operator form was used in the source.
	return nil
}
