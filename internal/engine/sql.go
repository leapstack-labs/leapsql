// Package engine provides the SQL orchestration layer.
// This file contains the FormatSQL function which combines parsing and formatting.
package engine

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/format"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

// FormatSQL parses and formats SQL in one step.
// This is an orchestration function that combines parser and formatter.
func FormatSQL(sql string, d *core.Dialect) (string, error) {
	stmt, comments, err := parser.ParseWithDialectAndComments(sql, d)
	if err != nil {
		return "", err
	}
	return format.WithComments(stmt, comments, d), nil
}
