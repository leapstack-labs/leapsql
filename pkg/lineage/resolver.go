package lineage

// Resolver walks the AST and resolves:
// - CTE definitions (names and columns)
// - Table references in FROM clauses
// - Column references to their source tables
// - Star expansion (SELECT * and t.*)
type Resolver struct {
	dialect *Dialect
	schema  Schema
	errors  []error
}

// NewResolver creates a new resolver with the given dialect and schema.
func NewResolver(dialect *Dialect, schema Schema) *Resolver {
	if dialect == nil {
		dialect = DefaultDialect()
	}
	return &Resolver{
		dialect: dialect,
		schema:  schema,
	}
}

// Resolve builds scopes for a SELECT statement and returns the root scope.
func (r *Resolver) Resolve(stmt *SelectStmt) (*Scope, error) {
	if stmt == nil {
		return nil, &ResolveError{Message: "nil statement"}
	}

	scope := NewScope(r.dialect, r.schema)

	// First, resolve CTEs
	if stmt.With != nil {
		if err := r.resolveCTEs(scope, stmt.With); err != nil {
			return nil, err
		}
	}

	// Resolve the main query body
	if err := r.resolveSelectBody(scope, stmt.Body); err != nil {
		return nil, err
	}

	return scope, nil
}

// resolveCTEs resolves all CTEs in a WITH clause.
// CTEs can reference previously defined CTEs (forward references not allowed).
func (r *Resolver) resolveCTEs(scope *Scope, with *WithClause) error {
	for _, cte := range with.CTEs {
		// Create a child scope for the CTE that can see previously defined CTEs
		cteScope := scope.Child()

		// Resolve the CTE's SELECT statement
		if cte.Select != nil {
			// Handle nested WITH clauses within the CTE
			if cte.Select.With != nil {
				if err := r.resolveCTEs(cteScope, cte.Select.With); err != nil {
					return err
				}
			}

			if cte.Select.Body != nil {
				if err := r.resolveSelectBody(cteScope, cte.Select.Body); err != nil {
					return err
				}

				// Extract columns from the CTE's SELECT list
				columns := r.extractSelectColumns(cteScope, cte.Select.Body)

				// Collect underlying sources from the CTE scope
				underlyingSources := r.collectUnderlyingSources(cteScope)

				scope.RegisterCTEWithSources(cte.Name, columns, underlyingSources)
			}
		}
	}
	return nil
}

// collectUnderlyingSources collects all physical table sources from a scope.
// It traces through CTEs and derived tables to find the underlying physical tables.
func (r *Resolver) collectUnderlyingSources(scope *Scope) []string {
	seen := make(map[string]struct{})
	var sources []string

	for _, entry := range scope.AllEntries() {
		switch entry.Type {
		case ScopeTable:
			// Physical table - use SourceTable (fully qualified name)
			tableName := entry.SourceTable
			if tableName == "" {
				tableName = entry.Name
			}
			if _, ok := seen[tableName]; !ok {
				seen[tableName] = struct{}{}
				sources = append(sources, tableName)
			}
		case ScopeCTE, ScopeDerived:
			// CTE or derived table - trace through to underlying sources
			for _, underlying := range entry.UnderlyingSources {
				if _, ok := seen[underlying]; !ok {
					seen[underlying] = struct{}{}
					sources = append(sources, underlying)
				}
			}
		}
	}

	return sources
}

// resolveSelectBody resolves a SELECT body (may include set operations).
func (r *Resolver) resolveSelectBody(scope *Scope, body *SelectBody) error {
	if body == nil {
		return nil
	}

	// Resolve the left (main) select
	if body.Left != nil {
		if err := r.resolveSelectCore(scope, body.Left); err != nil {
			return err
		}
	}

	// Resolve the right side of set operations (UNION, INTERSECT, EXCEPT)
	if body.Right != nil {
		// Set operations create their own scope
		rightScope := scope.Child()
		if err := r.resolveSelectBody(rightScope, body.Right); err != nil {
			return err
		}
	}

	return nil
}

// resolveSelectCore resolves a single SELECT clause.
func (r *Resolver) resolveSelectCore(scope *Scope, core *SelectCore) error {
	if core == nil {
		return nil
	}

	// Resolve FROM clause first (defines available tables)
	if core.From != nil {
		if err := r.resolveFromClause(scope, core.From); err != nil {
			return err
		}
	}

	// Note: We don't need to resolve expressions here for scope building.
	// Expression resolution happens during lineage extraction.

	return nil
}

// resolveFromClause resolves tables and joins in a FROM clause.
func (r *Resolver) resolveFromClause(scope *Scope, from *FromClause) error {
	// Resolve the main table reference
	if err := r.resolveTableRef(scope, from.Source); err != nil {
		return err
	}

	// Resolve joined tables
	for _, join := range from.Joins {
		if err := r.resolveTableRef(scope, join.Right); err != nil {
			return err
		}
	}

	return nil
}

// resolveTableRef resolves a table reference and registers it in scope.
func (r *Resolver) resolveTableRef(scope *Scope, ref TableRef) error {
	if ref == nil {
		return nil
	}

	switch t := ref.(type) {
	case *TableName:
		// Check if this references a CTE
		if cte, ok := scope.LookupCTE(t.Name); ok {
			// Register as alias to CTE, propagating underlying sources
			entry := &ScopeEntry{
				Type:              ScopeCTE,
				Name:              cte.Name,
				Alias:             t.Alias,
				Columns:           cte.Columns,
				UnderlyingSources: cte.UnderlyingSources,
			}
			normalized := scope.normalize(entry.EffectiveName())
			scope.entries[normalized] = entry
		} else {
			// Physical table
			scope.RegisterTable(t)
		}

	case *DerivedTable:
		// Derived table (subquery in FROM)
		subScope := scope.Child()

		if t.Select != nil {
			// Handle WITH clauses in subquery
			if t.Select.With != nil {
				if err := r.resolveCTEs(subScope, t.Select.With); err != nil {
					return err
				}
			}

			if t.Select.Body != nil {
				if err := r.resolveSelectBody(subScope, t.Select.Body); err != nil {
					return err
				}

				// Extract columns from subquery
				columns := r.extractSelectColumns(subScope, t.Select.Body)

				// Collect underlying sources from the subquery scope
				underlyingSources := r.collectUnderlyingSources(subScope)

				scope.RegisterDerivedWithSources(t.Alias, columns, underlyingSources)
			}
		}

	case *LateralTable:
		// LATERAL subquery - can reference tables from outer scope
		// We use the current scope (not a child) so it can see outer tables
		if t.Select != nil {
			if t.Select.With != nil {
				if err := r.resolveCTEs(scope, t.Select.With); err != nil {
					return err
				}
			}

			if t.Select.Body != nil {
				if err := r.resolveSelectBody(scope, t.Select.Body); err != nil {
					return err
				}

				columns := r.extractSelectColumns(scope, t.Select.Body)
				// For LATERAL, we can't easily separate sources, so we just register without underlying sources
				scope.RegisterDerived(t.Alias, columns)
			}
		}
	}

	return nil
}

// extractSelectColumns extracts column names from a SELECT list.
// Returns the list of output column names.
func (r *Resolver) extractSelectColumns(scope *Scope, body *SelectBody) []string {
	if body == nil || body.Left == nil {
		return nil
	}

	core := body.Left
	var columns []string

	for i, item := range core.Columns {
		name := r.extractColumnName(scope, item, i)
		columns = append(columns, name)
	}

	return columns
}

// extractColumnName extracts the output name for a SELECT item.
func (r *Resolver) extractColumnName(scope *Scope, item SelectItem, index int) string {
	// Explicit alias takes precedence
	if item.Alias != "" {
		return item.Alias
	}

	// SELECT *
	if item.Star {
		// Star expands to multiple columns - we can't name them individually here
		// This is handled during star expansion in lineage extraction
		return "*"
	}

	// SELECT table.*
	if item.TableStar != "" {
		return item.TableStar + ".*"
	}

	// Try to infer name from expression
	if item.Expr != nil {
		return r.inferColumnName(item.Expr, index)
	}

	// Fallback to generated name
	return r.generateColumnName(index)
}

// inferColumnName tries to infer a column name from an expression.
func (r *Resolver) inferColumnName(expr Expr, index int) string {
	switch e := expr.(type) {
	case *ColumnRef:
		return e.Column

	case *FuncCall:
		// Use function name as column name
		return r.dialect.NormalizeName(e.Name)

	case *CastExpr:
		// Use the inner expression's name
		return r.inferColumnName(e.Expr, index)

	case *ParenExpr:
		// Use the inner expression's name
		return r.inferColumnName(e.Expr, index)

	case *CaseExpr:
		// CASE expressions typically need explicit aliases
		return r.generateColumnName(index)

	case *Literal:
		// Literals need explicit aliases
		return r.generateColumnName(index)

	case *BinaryExpr, *UnaryExpr:
		// Complex expressions need explicit aliases
		return r.generateColumnName(index)
	}

	return r.generateColumnName(index)
}

// generateColumnName generates a default column name for unnamed expressions.
func (r *Resolver) generateColumnName(index int) string {
	// DuckDB uses col0, col1, etc. for unnamed columns
	return "column" + string(rune('0'+index))
}

// ResolveError represents an error during resolution.
type ResolveError struct {
	Message string
}

func (e *ResolveError) Error() string {
	return "resolve error: " + e.Message
}

// ColumnResolver resolves column references within an expression.
// Used during lineage extraction to find all source columns.
type ColumnResolver struct {
	scope   *Scope
	dialect *Dialect
}

// NewColumnResolver creates a column resolver for the given scope.
func NewColumnResolver(scope *Scope, dialect *Dialect) *ColumnResolver {
	if dialect == nil {
		dialect = DefaultDialect()
	}
	return &ColumnResolver{
		scope:   scope,
		dialect: dialect,
	}
}

// CollectColumns collects all column references from an expression.
func (cr *ColumnResolver) CollectColumns(expr Expr) []*ColumnRef {
	var refs []*ColumnRef
	cr.collectColumnsRecursive(expr, &refs)
	return refs
}

// collectColumnsRecursive recursively collects column references.
func (cr *ColumnResolver) collectColumnsRecursive(expr Expr, refs *[]*ColumnRef) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *ColumnRef:
		*refs = append(*refs, e)

	case *BinaryExpr:
		cr.collectColumnsRecursive(e.Left, refs)
		cr.collectColumnsRecursive(e.Right, refs)

	case *UnaryExpr:
		cr.collectColumnsRecursive(e.Expr, refs)

	case *FuncCall:
		for _, arg := range e.Args {
			cr.collectColumnsRecursive(arg, refs)
		}
		if e.Filter != nil {
			cr.collectColumnsRecursive(e.Filter, refs)
		}
		if e.Window != nil {
			for _, p := range e.Window.PartitionBy {
				cr.collectColumnsRecursive(p, refs)
			}
			for _, o := range e.Window.OrderBy {
				cr.collectColumnsRecursive(o.Expr, refs)
			}
		}

	case *CaseExpr:
		if e.Operand != nil {
			cr.collectColumnsRecursive(e.Operand, refs)
		}
		for _, w := range e.Whens {
			cr.collectColumnsRecursive(w.Condition, refs)
			cr.collectColumnsRecursive(w.Result, refs)
		}
		if e.Else != nil {
			cr.collectColumnsRecursive(e.Else, refs)
		}

	case *CastExpr:
		cr.collectColumnsRecursive(e.Expr, refs)

	case *InExpr:
		cr.collectColumnsRecursive(e.Expr, refs)
		for _, v := range e.Values {
			cr.collectColumnsRecursive(v, refs)
		}
		// Note: We don't recurse into IN subqueries - they're separate lineage

	case *BetweenExpr:
		cr.collectColumnsRecursive(e.Expr, refs)
		cr.collectColumnsRecursive(e.Low, refs)
		cr.collectColumnsRecursive(e.High, refs)

	case *IsNullExpr:
		cr.collectColumnsRecursive(e.Expr, refs)

	case *LikeExpr:
		cr.collectColumnsRecursive(e.Expr, refs)
		cr.collectColumnsRecursive(e.Pattern, refs)

	case *ParenExpr:
		cr.collectColumnsRecursive(e.Expr, refs)

	case *StarExpr:
		// Star expressions are handled specially during lineage extraction
		// We don't collect them as regular column refs

	case *Literal:
		// Literals have no column references

	case *SubqueryExpr, *ExistsExpr:
		// Subqueries have their own scope and lineage
	}
}

// ResolveColumnRef resolves a column reference to its source.
func (cr *ColumnResolver) ResolveColumnRef(ref *ColumnRef) (*ColumnSource, bool) {
	return cr.scope.ResolveColumnFull(ref)
}

// ExpandStar expands a star expression to individual column references.
func (cr *ColumnResolver) ExpandStar(tableName string) []*ColumnRef {
	return cr.scope.ExpandStar(tableName)
}
