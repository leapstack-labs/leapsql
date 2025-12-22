// Package ast provides AST traversal utilities for lint rules.
package ast

import (
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

// Walk traverses an AST depth-first and calls fn for each node.
// If fn returns false, traversal stops.
func Walk(node any, fn func(node any) bool) {
	if node == nil {
		return
	}
	if !fn(node) {
		return
	}
	walkNode(node, fn)
}

func walkNode(node any, fn func(node any) bool) {
	switch n := node.(type) {
	case *parser.SelectStmt:
		if n == nil {
			return
		}
		Walk(n.With, fn)
		Walk(n.Body, fn)

	case *parser.WithClause:
		if n == nil {
			return
		}
		for _, cte := range n.CTEs {
			Walk(cte, fn)
		}

	case *parser.CTE:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *parser.SelectBody:
		if n == nil {
			return
		}
		Walk(n.Left, fn)
		Walk(n.Right, fn)

	case *parser.SelectCore:
		if n == nil {
			return
		}
		for _, col := range n.Columns {
			Walk(col.Expr, fn)
		}
		Walk(n.From, fn)
		Walk(n.Where, fn)
		for _, expr := range n.GroupBy {
			Walk(expr, fn)
		}
		Walk(n.Having, fn)
		Walk(n.Qualify, fn)
		for _, item := range n.OrderBy {
			Walk(item.Expr, fn)
		}
		Walk(n.Limit, fn)
		Walk(n.Offset, fn)

	case *parser.FromClause:
		if n == nil {
			return
		}
		Walk(n.Source, fn)
		for _, join := range n.Joins {
			Walk(join, fn)
		}

	case *parser.Join:
		if n == nil {
			return
		}
		Walk(n.Right, fn)
		Walk(n.Condition, fn)

	case *parser.TableName:
		// Leaf node

	case *parser.DerivedTable:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *parser.LateralTable:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *parser.BinaryExpr:
		if n == nil {
			return
		}
		Walk(n.Left, fn)
		Walk(n.Right, fn)

	case *parser.UnaryExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *parser.FuncCall:
		if n == nil {
			return
		}
		for _, arg := range n.Args {
			Walk(arg, fn)
		}
		Walk(n.Filter, fn)

	case *parser.CaseExpr:
		if n == nil {
			return
		}
		Walk(n.Operand, fn)
		for _, when := range n.Whens {
			Walk(when.Condition, fn)
			Walk(when.Result, fn)
		}
		Walk(n.Else, fn)

	case *parser.CastExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *parser.InExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)
		for _, v := range n.Values {
			Walk(v, fn)
		}
		Walk(n.Query, fn)

	case *parser.BetweenExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)
		Walk(n.Low, fn)
		Walk(n.High, fn)

	case *parser.IsNullExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *parser.IsBoolExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *parser.LikeExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)
		Walk(n.Pattern, fn)

	case *parser.ParenExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *parser.SubqueryExpr:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *parser.ExistsExpr:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *parser.IndexExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)
		Walk(n.Index, fn)
		Walk(n.Start, fn)
		Walk(n.End, fn)

	case *parser.ListLiteral:
		if n == nil {
			return
		}
		for _, elem := range n.Elements {
			Walk(elem, fn)
		}

	case *parser.StructLiteral:
		if n == nil {
			return
		}
		for _, field := range n.Fields {
			Walk(field.Value, fn)
		}

	case *parser.LambdaExpr:
		if n == nil {
			return
		}
		Walk(n.Body, fn)

	// Leaf nodes - no children to walk
	case *parser.ColumnRef, *parser.Literal, *parser.StarExpr, *parser.MacroExpr, *parser.MacroTable:
		// Nothing to traverse
	}
}

// WalkExprs walks all expressions in a statement and calls fn for each.
// Returns early if fn returns false.
func WalkExprs(stmt *parser.SelectStmt, fn func(expr parser.Expr) bool) {
	Walk(stmt, func(node any) bool {
		if expr, ok := node.(parser.Expr); ok {
			return fn(expr)
		}
		return true
	})
}

// CollectFuncCalls returns all function calls in a statement.
func CollectFuncCalls(stmt *parser.SelectStmt) []*parser.FuncCall {
	var funcs []*parser.FuncCall
	Walk(stmt, func(node any) bool {
		if fc, ok := node.(*parser.FuncCall); ok {
			funcs = append(funcs, fc)
		}
		return true
	})
	return funcs
}

// CollectColumnRefs returns all column references in a statement.
func CollectColumnRefs(stmt *parser.SelectStmt) []*parser.ColumnRef {
	var refs []*parser.ColumnRef
	Walk(stmt, func(node any) bool {
		if cr, ok := node.(*parser.ColumnRef); ok {
			refs = append(refs, cr)
		}
		return true
	})
	return refs
}

// CollectTableRefs returns all table references in a statement.
func CollectTableRefs(stmt *parser.SelectStmt) []parser.TableRef {
	var refs []parser.TableRef
	Walk(stmt, func(node any) bool {
		if tr, ok := node.(parser.TableRef); ok {
			refs = append(refs, tr)
		}
		return true
	})
	return refs
}

// CollectCaseExprs returns all CASE expressions in a statement.
func CollectCaseExprs(stmt *parser.SelectStmt) []*parser.CaseExpr {
	var cases []*parser.CaseExpr
	Walk(stmt, func(node any) bool {
		if ce, ok := node.(*parser.CaseExpr); ok {
			cases = append(cases, ce)
		}
		return true
	})
	return cases
}

// CollectBinaryExprs returns all binary expressions in a statement.
func CollectBinaryExprs(stmt *parser.SelectStmt) []*parser.BinaryExpr {
	var exprs []*parser.BinaryExpr
	Walk(stmt, func(node any) bool {
		if be, ok := node.(*parser.BinaryExpr); ok {
			exprs = append(exprs, be)
		}
		return true
	})
	return exprs
}

// HasWindowFunction checks if any function call in the statement has a window spec.
func HasWindowFunction(stmt *parser.SelectStmt) bool {
	found := false
	Walk(stmt, func(node any) bool {
		if fc, ok := node.(*parser.FuncCall); ok && fc.Window != nil {
			found = true
			return false // Stop walking
		}
		return true
	})
	return found
}

// GetSelectCore extracts the SelectCore from a SelectStmt, handling nil checks.
func GetSelectCore(stmt *parser.SelectStmt) *parser.SelectCore {
	if stmt == nil || stmt.Body == nil || stmt.Body.Left == nil {
		return nil
	}
	return stmt.Body.Left
}

// CollectCTENames returns the names of all CTEs defined in a statement.
func CollectCTENames(stmt *parser.SelectStmt) []string {
	if stmt == nil || stmt.With == nil {
		return nil
	}
	names := make([]string, 0, len(stmt.With.CTEs))
	for _, cte := range stmt.With.CTEs {
		names = append(names, cte.Name)
	}
	return names
}

// CollectReferencedTables returns names of tables referenced in FROM clauses.
// Returns a map of table name/alias -> true.
func CollectReferencedTables(stmt *parser.SelectStmt) map[string]bool {
	refs := make(map[string]bool)
	Walk(stmt, func(node any) bool {
		switch n := node.(type) {
		case *parser.TableName:
			if n.Alias != "" {
				refs[n.Alias] = true
			} else {
				refs[n.Name] = true
			}
		case *parser.DerivedTable:
			if n.Alias != "" {
				refs[n.Alias] = true
			}
		case *parser.LateralTable:
			if n.Alias != "" {
				refs[n.Alias] = true
			}
		}
		return true
	})
	return refs
}

// CollectJoins returns all joins in a statement.
func CollectJoins(stmt *parser.SelectStmt) []*parser.Join {
	var joins []*parser.Join
	Walk(stmt, func(node any) bool {
		if j, ok := node.(*parser.Join); ok {
			joins = append(joins, j)
		}
		return true
	})
	return joins
}

// CollectSelectCores returns all SelectCore nodes in a statement (for unions).
func CollectSelectCores(stmt *parser.SelectStmt) []*parser.SelectCore {
	var cores []*parser.SelectCore
	Walk(stmt, func(node any) bool {
		if sc, ok := node.(*parser.SelectCore); ok {
			cores = append(cores, sc)
		}
		return true
	})
	return cores
}

// CollectSelectBodies returns all SelectBody nodes (for finding set operations).
func CollectSelectBodies(stmt *parser.SelectStmt) []*parser.SelectBody {
	var bodies []*parser.SelectBody
	Walk(stmt, func(node any) bool {
		if sb, ok := node.(*parser.SelectBody); ok {
			bodies = append(bodies, sb)
		}
		return true
	})
	return bodies
}
