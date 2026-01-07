package format

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Format formats a parsed SQL statement according to the dialect.
func Format(stmt *core.SelectStmt, d *core.Dialect) string {
	p := newPrinter(d)
	p.formatSelectStmt(stmt)
	return p.String()
}

// WithComments formats a statement with comment preservation.
func WithComments(stmt *core.SelectStmt, comments []*token.Comment, d *core.Dialect) string {
	decorated := Decorate(stmt, comments)
	p := newPrinter(d)
	p.formatSelectStmt(decorated)
	return p.String()
}

// NOTE: SQL() function has been moved to internal/engine/sql.go
// Use engine.FormatSQL() for parse-and-format in one call.
