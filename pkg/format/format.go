package format

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Format formats a parsed SQL statement according to the dialect.
func Format(stmt *core.SelectStmt, d *dialect.Dialect) string {
	p := newPrinter(d)
	p.formatSelectStmt(stmt)
	return p.String()
}

// WithComments formats a statement with comment preservation.
func WithComments(stmt *core.SelectStmt, comments []*token.Comment, d *dialect.Dialect) string {
	decorated := Decorate(stmt, comments)
	p := newPrinter(d)
	p.formatSelectStmt(decorated)
	return p.String()
}

// SQL parses and formats SQL in one call.
func SQL(sql string, d *dialect.Dialect) (string, error) {
	stmt, comments, err := parser.ParseWithDialectAndComments(sql, d)
	if err != nil {
		return "", err
	}
	return WithComments(stmt, comments, d), nil
}
