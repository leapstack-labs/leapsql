// Package ast provides AST traversal utilities for SQL lint rules.
package ast

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
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
	case *core.SelectStmt:
		if n == nil {
			return
		}
		Walk(n.With, fn)
		Walk(n.Body, fn)

	case *core.WithClause:
		if n == nil {
			return
		}
		for _, cte := range n.CTEs {
			Walk(cte, fn)
		}

	case *core.CTE:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *core.SelectBody:
		if n == nil {
			return
		}
		Walk(n.Left, fn)
		Walk(n.Right, fn)

	case *core.SelectCore:
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

	case *core.FromClause:
		if n == nil {
			return
		}
		Walk(n.Source, fn)
		for _, join := range n.Joins {
			Walk(join, fn)
		}

	case *core.Join:
		if n == nil {
			return
		}
		Walk(n.Right, fn)
		Walk(n.Condition, fn)

	case *core.TableName:
		// Leaf node

	case *core.DerivedTable:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *core.LateralTable:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *core.BinaryExpr:
		if n == nil {
			return
		}
		Walk(n.Left, fn)
		Walk(n.Right, fn)

	case *core.UnaryExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *core.FuncCall:
		if n == nil {
			return
		}
		for _, arg := range n.Args {
			Walk(arg, fn)
		}
		Walk(n.Filter, fn)

	case *core.CaseExpr:
		if n == nil {
			return
		}
		Walk(n.Operand, fn)
		for _, when := range n.Whens {
			Walk(when.Condition, fn)
			Walk(when.Result, fn)
		}
		Walk(n.Else, fn)

	case *core.CastExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *core.InExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)
		for _, v := range n.Values {
			Walk(v, fn)
		}
		Walk(n.Query, fn)

	case *core.BetweenExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)
		Walk(n.Low, fn)
		Walk(n.High, fn)

	case *core.IsNullExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *core.IsBoolExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *core.LikeExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)
		Walk(n.Pattern, fn)

	case *core.ParenExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)

	case *core.SubqueryExpr:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *core.ExistsExpr:
		if n == nil {
			return
		}
		Walk(n.Select, fn)

	case *core.IndexExpr:
		if n == nil {
			return
		}
		Walk(n.Expr, fn)
		Walk(n.Index, fn)
		Walk(n.Start, fn)
		Walk(n.Stop, fn)

	case *core.ListLiteral:
		if n == nil {
			return
		}
		for _, elem := range n.Elements {
			Walk(elem, fn)
		}

	case *core.StructLiteral:
		if n == nil {
			return
		}
		for _, field := range n.Fields {
			Walk(field.Value, fn)
		}

	case *core.LambdaExpr:
		if n == nil {
			return
		}
		Walk(n.Body, fn)

	// Leaf nodes - no children to walk
	case *core.ColumnRef, *core.Literal, *core.StarExpr, *core.MacroExpr, *core.MacroTable:
		// Nothing to traverse
	}
}

// WalkExprs walks all expressions in a statement and calls fn for each.
// Returns early if fn returns false.
func WalkExprs(stmt *core.SelectStmt, fn func(expr core.Expr) bool) {
	Walk(stmt, func(node any) bool {
		if expr, ok := node.(core.Expr); ok {
			return fn(expr)
		}
		return true
	})
}

// CollectFuncCalls returns all function calls in a statement.
func CollectFuncCalls(stmt *core.SelectStmt) []*core.FuncCall {
	var funcs []*core.FuncCall
	Walk(stmt, func(node any) bool {
		if fc, ok := node.(*core.FuncCall); ok {
			funcs = append(funcs, fc)
		}
		return true
	})
	return funcs
}

// CollectColumnRefs returns all column references in a statement.
func CollectColumnRefs(stmt *core.SelectStmt) []*core.ColumnRef {
	var refs []*core.ColumnRef
	Walk(stmt, func(node any) bool {
		if cr, ok := node.(*core.ColumnRef); ok {
			refs = append(refs, cr)
		}
		return true
	})
	return refs
}

// CollectTableRefs returns all table references in a statement.
func CollectTableRefs(stmt *core.SelectStmt) []core.TableRef {
	var refs []core.TableRef
	Walk(stmt, func(node any) bool {
		if tr, ok := node.(core.TableRef); ok {
			refs = append(refs, tr)
		}
		return true
	})
	return refs
}

// CollectCaseExprs returns all CASE expressions in a statement.
func CollectCaseExprs(stmt *core.SelectStmt) []*core.CaseExpr {
	var cases []*core.CaseExpr
	Walk(stmt, func(node any) bool {
		if ce, ok := node.(*core.CaseExpr); ok {
			cases = append(cases, ce)
		}
		return true
	})
	return cases
}

// CollectBinaryExprs returns all binary expressions in a statement.
func CollectBinaryExprs(stmt *core.SelectStmt) []*core.BinaryExpr {
	var exprs []*core.BinaryExpr
	Walk(stmt, func(node any) bool {
		if be, ok := node.(*core.BinaryExpr); ok {
			exprs = append(exprs, be)
		}
		return true
	})
	return exprs
}

// HasWindowFunction checks if any function call in the statement has a window spec.
func HasWindowFunction(stmt *core.SelectStmt) bool {
	found := false
	Walk(stmt, func(node any) bool {
		if fc, ok := node.(*core.FuncCall); ok && fc.Window != nil {
			found = true
			return false // Stop walking
		}
		return true
	})
	return found
}

// GetSelectCore extracts the SelectCore from a SelectStmt, handling nil checks.
func GetSelectCore(stmt *core.SelectStmt) *core.SelectCore {
	if stmt == nil || stmt.Body == nil || stmt.Body.Left == nil {
		return nil
	}
	return stmt.Body.Left
}

// CollectCTENames returns the names of all CTEs defined in a statement.
func CollectCTENames(stmt *core.SelectStmt) []string {
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
func CollectReferencedTables(stmt *core.SelectStmt) map[string]bool {
	refs := make(map[string]bool)
	Walk(stmt, func(node any) bool {
		switch n := node.(type) {
		case *core.TableName:
			if n.Alias != "" {
				refs[n.Alias] = true
			} else {
				refs[n.Name] = true
			}
		case *core.DerivedTable:
			if n.Alias != "" {
				refs[n.Alias] = true
			}
		case *core.LateralTable:
			if n.Alias != "" {
				refs[n.Alias] = true
			}
		}
		return true
	})
	return refs
}

// CollectJoins returns all joins in a statement.
func CollectJoins(stmt *core.SelectStmt) []*core.Join {
	var joins []*core.Join
	Walk(stmt, func(node any) bool {
		if j, ok := node.(*core.Join); ok {
			joins = append(joins, j)
		}
		return true
	})
	return joins
}

// CollectSelectCores returns all SelectCore nodes in a statement (for unions).
func CollectSelectCores(stmt *core.SelectStmt) []*core.SelectCore {
	var cores []*core.SelectCore
	Walk(stmt, func(node any) bool {
		if sc, ok := node.(*core.SelectCore); ok {
			cores = append(cores, sc)
		}
		return true
	})
	return cores
}

// CollectSelectBodies returns all SelectBody nodes (for finding set operations).
func CollectSelectBodies(stmt *core.SelectStmt) []*core.SelectBody {
	var bodies []*core.SelectBody
	Walk(stmt, func(node any) bool {
		if sb, ok := node.(*core.SelectBody); ok {
			bodies = append(bodies, sb)
		}
		return true
	})
	return bodies
}
