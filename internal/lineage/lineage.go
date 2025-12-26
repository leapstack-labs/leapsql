package lineage

import (
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

// ColumnLineage describes the lineage of a single output column.
type ColumnLineage struct {
	Name      string             // Output column name
	Sources   []core.SourceRef   // Source columns this output derives from
	Transform core.TransformType // Type of transformation applied
	Function  string             // Function name (for aggregates/window functions)
}

// ModelLineage describes the complete lineage of a SQL model.
type ModelLineage struct {
	Sources        []string         // All source tables (deduplicated, sorted)
	Columns        []*ColumnLineage // Lineage for each output column
	UsesSelectStar bool             // true if SELECT * or t.* detected
}

// ExtractLineageOptions configures the lineage extraction.
type ExtractLineageOptions struct {
	Dialect *dialect.Dialect // SQL dialect (required)
	Schema  parser.Schema    // Schema information for star expansion
}

// ExtractLineageWithOptions extracts lineage with full configuration options.
// Returns dialect.ErrDialectRequired if opts.Dialect is nil.
func ExtractLineageWithOptions(sqlStr string, opts ExtractLineageOptions) (*ModelLineage, error) {
	d := opts.Dialect
	if d == nil {
		return nil, dialect.ErrDialectRequired
	}

	// Parse the SQL with the specified dialect
	stmt, err := parser.ParseWithDialect(sqlStr, d)
	if err != nil {
		return nil, err
	}

	// Create extractor
	extractor := &lineageExtractor{
		dialect: d,
		schema:  opts.Schema,
		sources: make(map[string]struct{}),
	}

	// Extract lineage
	return extractor.extract(stmt)
}

// lineageExtractor walks the AST to extract lineage information.
type lineageExtractor struct {
	dialect        *dialect.Dialect
	schema         parser.Schema
	sources        map[string]struct{} // Collected source tables
	usesSelectStar bool                // Track star usage during extraction
}

// extract extracts lineage from a parsed statement.
func (e *lineageExtractor) extract(stmt *parser.SelectStmt) (*ModelLineage, error) {
	if stmt == nil || stmt.Body == nil {
		return nil, &parser.ParseError{Message: "empty statement"}
	}

	// Resolve scopes
	resolver, err := parser.NewResolver(e.dialect, e.schema)
	if err != nil {
		return nil, err
	}
	scope, err := resolver.Resolve(stmt)
	if err != nil {
		return nil, err
	}

	// Extract column lineage from the main SELECT body
	columns, err := e.extractBodyLineage(scope, stmt.Body)
	if err != nil {
		return nil, err
	}

	// Collect all source tables
	e.collectSources(scope)

	// Build result
	result := &ModelLineage{
		Sources:        e.getSortedSources(),
		Columns:        columns,
		UsesSelectStar: e.usesSelectStar,
	}

	return result, nil
}

// extractBodyLineage extracts lineage from a SELECT body.
func (e *lineageExtractor) extractBodyLineage(scope *parser.Scope, body *parser.SelectBody) ([]*ColumnLineage, error) {
	if body == nil || body.Left == nil {
		return nil, nil
	}

	// Extract from the left (main) SELECT
	columns, err := e.extractCoreLineage(scope, body.Left)
	if err != nil {
		return nil, err
	}

	// Handle set operations (UNION, INTERSECT, EXCEPT)
	if body.Right != nil {
		// For set operations, we merge the lineage from both sides
		// The output columns come from the left side, but sources come from both
		rightColumns, err := e.extractBodyLineage(scope, body.Right)
		if err != nil {
			return nil, err
		}

		// Merge sources from right side into left columns
		for i, col := range columns {
			if i < len(rightColumns) {
				col.Sources = e.mergeSources(col.Sources, rightColumns[i].Sources)
				// Mark as expression since it's a union of values
				if col.Transform == core.TransformDirect {
					col.Transform = core.TransformExpression
				}
			}
		}
	}

	return columns, nil
}

// extractCoreLineage extracts lineage from a SELECT core.
func (e *lineageExtractor) extractCoreLineage(scope *parser.Scope, core *parser.SelectCore) ([]*ColumnLineage, error) {
	if core == nil {
		return nil, nil
	}

	// Register tables from FROM clause
	if core.From != nil {
		e.registerFromClause(scope, core.From)
	}

	colResolver, err := parser.NewColumnResolver(scope, e.dialect)
	if err != nil {
		return nil, err
	}

	var columns []*ColumnLineage
	for i, item := range core.Columns {
		lineages := e.extractSelectItemLineage(scope, colResolver, item, i)
		columns = append(columns, lineages...)
	}

	return columns, nil
}

// extractSelectItemLineage extracts lineage from a single SELECT item.
func (e *lineageExtractor) extractSelectItemLineage(scope *parser.Scope, colResolver *parser.ColumnResolver, item parser.SelectItem, index int) []*ColumnLineage {
	// Handle SELECT *
	if item.Star {
		e.usesSelectStar = true
		lineages := e.expandStar(scope, "", index)
		return e.applyStarModifiers(scope, colResolver, lineages, item.Modifiers)
	}

	// Handle SELECT table.*
	if item.TableStar != "" {
		e.usesSelectStar = true
		lineages := e.expandStar(scope, item.TableStar, index)
		return e.applyStarModifiers(scope, colResolver, lineages, item.Modifiers)
	}

	// Regular expression
	lineage := e.extractExprLineage(scope, colResolver, item.Expr)

	// Determine output column name
	name := item.Alias
	if name == "" {
		name = e.inferColumnName(item.Expr, index)
	}
	lineage.Name = name

	return []*ColumnLineage{lineage}
}

// applyStarModifiers applies EXCLUDE, REPLACE, and RENAME modifiers to star-expanded columns.
func (e *lineageExtractor) applyStarModifiers(scope *parser.Scope, colResolver *parser.ColumnResolver, lineages []*ColumnLineage, modifiers []parser.StarModifier) []*ColumnLineage {
	if len(modifiers) == 0 {
		return lineages
	}

	for _, mod := range modifiers {
		switch m := mod.(type) {
		case *parser.ExcludeModifier:
			// Remove excluded columns
			excludeSet := make(map[string]bool)
			for _, col := range m.Columns {
				excludeSet[e.dialect.NormalizeName(col)] = true
			}
			var filtered []*ColumnLineage
			for _, l := range lineages {
				if !excludeSet[e.dialect.NormalizeName(l.Name)] {
					filtered = append(filtered, l)
				}
			}
			lineages = filtered

		case *parser.ReplaceModifier:
			// Replace columns with expressions
			replaceMap := make(map[string]*parser.ReplaceItem)
			for i := range m.Items {
				replaceMap[e.dialect.NormalizeName(m.Items[i].Alias)] = &m.Items[i]
			}
			for _, l := range lineages {
				if repl, ok := replaceMap[e.dialect.NormalizeName(l.Name)]; ok {
					// Extract lineage from the replacement expression
					exprLineage := e.extractExprLineage(scope, colResolver, repl.Expr)
					l.Sources = exprLineage.Sources
					l.Transform = core.TransformExpression
					l.Function = exprLineage.Function
				}
			}

		case *parser.RenameModifier:
			// Rename columns
			renameMap := make(map[string]string)
			for _, item := range m.Items {
				renameMap[e.dialect.NormalizeName(item.OldName)] = item.NewName
			}
			for _, l := range lineages {
				if newName, ok := renameMap[e.dialect.NormalizeName(l.Name)]; ok {
					l.Name = newName
				}
			}
		}
	}

	return lineages
}

// expandStar expands a SELECT * or table.* into individual column lineages.
func (e *lineageExtractor) expandStar(scope *parser.Scope, tableName string, _ int) []*ColumnLineage {
	refs := scope.ExpandStar(tableName)
	if len(refs) == 0 {
		// No schema info available - return a single "unknown" lineage
		name := "*"
		if tableName != "" {
			name = tableName + ".*"
		}
		return []*ColumnLineage{{
			Name:      name,
			Transform: core.TransformDirect,
		}}
	}

	var lineages []*ColumnLineage
	for _, ref := range refs {
		source := core.SourceRef{
			Table:  ref.Table,
			Column: ref.Column,
		}

		// Record the source table (avoiding CTE/derived names)
		if entry, ok := scope.Lookup(ref.Table); ok {
			switch entry.Type {
			case parser.ScopeTable:
				if entry.SourceTable != "" {
					e.sources[entry.SourceTable] = struct{}{}
				} else {
					e.sources[entry.Name] = struct{}{}
				}
			case parser.ScopeCTE, parser.ScopeDerived:
				// For CTEs and derived tables, use underlying sources
				for _, underlying := range entry.UnderlyingSources {
					e.sources[underlying] = struct{}{}
				}
			}
		}

		lineages = append(lineages, &ColumnLineage{
			Name:      ref.Column,
			Sources:   []core.SourceRef{source},
			Transform: core.TransformDirect,
		})
	}

	return lineages
}

// extractExprLineage extracts lineage from an expression.
func (e *lineageExtractor) extractExprLineage(scope *parser.Scope, colResolver *parser.ColumnResolver, expr parser.Expr) *ColumnLineage {
	lineage := &ColumnLineage{}

	if expr == nil {
		return lineage
	}

	switch ex := expr.(type) {
	case *parser.ColumnRef:
		// Direct column reference
		source := e.resolveColumnRef(scope, ex)
		if source != nil {
			lineage.Sources = []core.SourceRef{*source}
		}
		lineage.Transform = core.TransformDirect

	case *parser.Literal:
		// Literals have no source columns
		lineage.Transform = core.TransformExpression

	case *parser.FuncCall:
		// Collect all column refs from the function arguments
		sources := e.collectExprSources(scope, colResolver, expr)
		lineage.Sources = sources

		// Determine transform type based on function classification
		funcType := e.dialect.FunctionLineageType(ex.Name)
		funcName := e.dialect.NormalizeName(ex.Name)
		switch funcType {
		case dialect.LineageTable:
			// Table function acts as a data source, not a transformation
			// SELECT * FROM read_csv('file.csv') - the CSV is the source, not upstream columns
			lineage.Sources = nil
			lineage.Transform = core.TransformExpression
			lineage.Function = funcName
		case dialect.LineageAggregate:
			lineage.Transform = core.TransformExpression
			lineage.Function = funcName
		case dialect.LineageWindow:
			lineage.Transform = core.TransformExpression
			lineage.Function = funcName
		case dialect.LineageGenerator:
			// Generator functions have no source columns
			lineage.Sources = nil
			lineage.Transform = core.TransformExpression
			lineage.Function = funcName
		default:
			// Passthrough - keep all source columns
			if len(sources) == 1 {
				lineage.Transform = core.TransformDirect
			} else {
				lineage.Transform = core.TransformExpression
			}
		}

	case *parser.CaseExpr:
		// Collect all column refs from CASE expression
		sources := e.collectExprSources(scope, colResolver, expr)
		lineage.Sources = sources
		lineage.Transform = core.TransformExpression

	case *parser.CastExpr:
		// CAST preserves lineage but is a transformation
		innerLineage := e.extractExprLineage(scope, colResolver, ex.Expr)
		lineage.Sources = innerLineage.Sources
		lineage.Transform = core.TransformExpression

	case *parser.BinaryExpr:
		// Recursively extract lineage from both sides (handles subqueries)
		leftLineage := e.extractExprLineage(scope, colResolver, ex.Left)
		rightLineage := e.extractExprLineage(scope, colResolver, ex.Right)
		lineage.Sources = e.mergeSources(leftLineage.Sources, rightLineage.Sources)
		lineage.Transform = core.TransformExpression

	case *parser.UnaryExpr:
		innerLineage := e.extractExprLineage(scope, colResolver, ex.Expr)
		lineage.Sources = innerLineage.Sources
		lineage.Transform = core.TransformExpression

	case *parser.ParenExpr:
		return e.extractExprLineage(scope, colResolver, ex.Expr)

	case *parser.SubqueryExpr:
		// Scalar subquery: extract lineage from the subquery's SELECT list only
		// We use "tunnel vision" - ignore WHERE/HAVING to avoid correlation issues
		if ex.Select != nil && ex.Select.Body != nil && ex.Select.Body.Left != nil {
			core := ex.Select.Body.Left

			// Create a child scope for the subquery
			subScope := scope.Child()

			// Resolve ONLY the FROM clause (not WHERE - avoids correlation issues)
			if core.From != nil {
				e.resolveSubqueryFrom(subScope, ex.Select, core.From)
			}

			// Create column resolver for the subquery scope
			subColResolver, err := parser.NewColumnResolver(subScope, e.dialect)
			if err == nil {
				// Extract lineage from the subquery's SELECT columns only
				for _, item := range core.Columns {
					if item.Expr != nil {
						itemLineage := e.extractExprLineage(subScope, subColResolver, item.Expr)
						lineage.Sources = append(lineage.Sources, itemLineage.Sources...)
					}
				}
			}
		}
		lineage.Transform = core.TransformExpression

	case *parser.LambdaExpr:
		// Lambda expressions: extract lineage from body only
		// Lambda parameters are local bindings, not column references
		// e.g., list_transform([1,2,3], x -> x * 2) - 'x' is a parameter, not a column
		if ex.Body != nil {
			bodyLineage := e.extractExprLineage(scope, colResolver, ex.Body)
			lineage.Sources = bodyLineage.Sources
		}
		lineage.Transform = core.TransformExpression

	case *parser.StructLiteral:
		// Struct literals: collect lineage from all field values
		// e.g., {'name': first_name, 'full': first_name || last_name}
		for _, field := range ex.Fields {
			if field.Value != nil {
				fieldLineage := e.extractExprLineage(scope, colResolver, field.Value)
				lineage.Sources = e.mergeSources(lineage.Sources, fieldLineage.Sources)
			}
		}
		lineage.Transform = core.TransformExpression

	case *parser.ListLiteral:
		// List literals: collect lineage from all elements
		// e.g., [a, b, c] or [col1, col2 * 2]
		for _, elem := range ex.Elements {
			elemLineage := e.extractExprLineage(scope, colResolver, elem)
			lineage.Sources = e.mergeSources(lineage.Sources, elemLineage.Sources)
		}
		lineage.Transform = core.TransformExpression

	case *parser.IndexExpr:
		// Index expressions: collect lineage from the indexed expression and indices
		// e.g., arr[1], arr[i], arr[start:end]
		if ex.Expr != nil {
			exprLineage := e.extractExprLineage(scope, colResolver, ex.Expr)
			lineage.Sources = e.mergeSources(lineage.Sources, exprLineage.Sources)
		}
		if ex.Index != nil {
			indexLineage := e.extractExprLineage(scope, colResolver, ex.Index)
			lineage.Sources = e.mergeSources(lineage.Sources, indexLineage.Sources)
		}
		// For slice expressions, also check Start and End
		if ex.Start != nil {
			startLineage := e.extractExprLineage(scope, colResolver, ex.Start)
			lineage.Sources = e.mergeSources(lineage.Sources, startLineage.Sources)
		}
		if ex.End != nil {
			endLineage := e.extractExprLineage(scope, colResolver, ex.End)
			lineage.Sources = e.mergeSources(lineage.Sources, endLineage.Sources)
		}
		lineage.Transform = core.TransformExpression

	default:
		// For other expression types, collect all column references
		sources := e.collectExprSources(scope, colResolver, expr)
		lineage.Sources = sources
		if len(sources) > 1 {
			lineage.Transform = core.TransformExpression
		}
	}

	return lineage
}

// collectExprSources collects all source columns from an expression.
func (e *lineageExtractor) collectExprSources(scope *parser.Scope, colResolver *parser.ColumnResolver, expr parser.Expr) []core.SourceRef {
	refs := colResolver.CollectColumns(expr)
	var sources []core.SourceRef
	seen := make(map[string]struct{})

	for _, ref := range refs {
		source := e.resolveColumnRef(scope, ref)
		if source != nil {
			key := source.Table + "." + source.Column
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				sources = append(sources, *source)
			}
		}
	}

	return sources
}

// resolveColumnRef resolves a column reference to its source.
func (e *lineageExtractor) resolveColumnRef(scope *parser.Scope, ref *parser.ColumnRef) *core.SourceRef {
	if ref == nil {
		return nil
	}

	// Try to resolve through scope
	resolved, ok := scope.ResolveColumnFull(ref)
	if ok {
		// For CTEs and derived tables, record underlying sources instead of the CTE/derived name
		if resolved.FromCTE || resolved.FromDerived {
			// Lookup the entry to get underlying sources
			if entry, entryOk := scope.Lookup(resolved.Table); entryOk {
				for _, underlying := range entry.UnderlyingSources {
					e.sources[underlying] = struct{}{}
				}
			}
			// Return source with the underlying table if there's only one, otherwise use the alias
			if entry, entryOk := scope.Lookup(resolved.Table); entryOk && len(entry.UnderlyingSources) == 1 {
				return &core.SourceRef{
					Table:  entry.UnderlyingSources[0],
					Column: resolved.Column,
				}
			}
			return &core.SourceRef{
				Table:  resolved.Table, // Keep CTE/derived alias for column lineage tracing
				Column: resolved.Column,
			}
		}

		// Physical table - record the source table
		if resolved.SourceTable != "" {
			e.sources[resolved.SourceTable] = struct{}{}
		} else {
			e.sources[resolved.Table] = struct{}{}
		}

		return &core.SourceRef{
			Table:  resolved.SourceTable,
			Column: resolved.Column,
		}
	}

	// Fallback: use the reference as-is, but check if it's a CTE/derived first
	if ref.Table != "" {
		if entry, entryOk := scope.Lookup(ref.Table); entryOk {
			if entry.Type == parser.ScopeCTE || entry.Type == parser.ScopeDerived {
				// Don't add CTE/derived names to sources
				for _, underlying := range entry.UnderlyingSources {
					e.sources[underlying] = struct{}{}
				}
				return &core.SourceRef{
					Table:  ref.Table,
					Column: ref.Column,
				}
			}
		}
		e.sources[ref.Table] = struct{}{}
		return &core.SourceRef{
			Table:  ref.Table,
			Column: ref.Column,
		}
	}

	// Unqualified column with no resolution - still include it
	return &core.SourceRef{
		Column: ref.Column,
	}
}

// registerFromClause registers tables from a FROM clause.
func (e *lineageExtractor) registerFromClause(scope *parser.Scope, from *parser.FromClause) {
	if from == nil {
		return
	}

	e.registerTableRef(scope, from.Source)

	for _, join := range from.Joins {
		e.registerTableRef(scope, join.Right)

		// Note: NATURAL JOIN column lineage requires schema introspection.
		// Table lineage still works, but we can't determine which columns are joined
		// without knowing the schema. This is handled gracefully - column lineage
		// will work for explicit column references.
		//
		// For USING clause, the join columns are explicit in the AST (join.Using).
		// Column lineage will work through normal column resolution.
	}
}

// registerTableRef registers a table reference as a source.
func (e *lineageExtractor) registerTableRef(scope *parser.Scope, ref parser.TableRef) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *parser.TableName:
		// Check if it's a CTE reference
		if _, ok := scope.LookupCTE(t.Name); ok {
			// CTE references are not external sources
			return
		}

		// Build fully qualified name
		var parts []string
		if t.Catalog != "" {
			parts = append(parts, t.Catalog)
		}
		if t.Schema != "" {
			parts = append(parts, t.Schema)
		}
		parts = append(parts, t.Name)
		e.sources[strings.Join(parts, ".")] = struct{}{}

	case *parser.DerivedTable:
		// Derived tables don't add sources directly
		// Their inner queries' sources are collected when we process them

	case *parser.LateralTable:
		// Same as derived tables

	case *parser.PivotTable:
		// PIVOT: register the source table and collect lineage from aggregates
		e.registerTableRef(scope, t.Source)
		// ForColumn is used for pivoting - its lineage is from the source table

	case *parser.UnpivotTable:
		// UNPIVOT: register the source table
		e.registerTableRef(scope, t.Source)
		// InColumns are being unpivoted - their lineage is from the source table
	}
}

// collectSources collects sources from scope entries.
func (e *lineageExtractor) collectSources(scope *parser.Scope) {
	for _, entry := range scope.AllEntries() {
		switch entry.Type {
		case parser.ScopeTable:
			if entry.SourceTable != "" {
				e.sources[entry.SourceTable] = struct{}{}
			} else {
				e.sources[entry.Name] = struct{}{}
			}
		case parser.ScopeCTE, parser.ScopeDerived:
			// For CTEs and derived tables, use ONLY underlying sources
			// Do NOT add the CTE/derived name itself
			for _, underlying := range entry.UnderlyingSources {
				e.sources[underlying] = struct{}{}
			}
		}
	}
}

// getSortedSources returns the collected sources as a sorted slice.
func (e *lineageExtractor) getSortedSources() []string {
	sources := make([]string, 0, len(e.sources))
	for s := range e.sources {
		if s != "" {
			sources = append(sources, s)
		}
	}
	sort.Strings(sources)
	return sources
}

// mergeSources merges two source lists, removing duplicates.
func (e *lineageExtractor) mergeSources(a, b []core.SourceRef) []core.SourceRef {
	seen := make(map[string]struct{})
	var result []core.SourceRef

	for _, s := range a {
		key := s.Table + "." + s.Column
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, s)
		}
	}

	for _, s := range b {
		key := s.Table + "." + s.Column
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, s)
		}
	}

	return result
}

// inferColumnName infers a column name from an expression.
func (e *lineageExtractor) inferColumnName(expr parser.Expr, index int) string {
	if expr == nil {
		return e.generateColumnName(index)
	}

	switch ex := expr.(type) {
	case *parser.ColumnRef:
		return ex.Column

	case *parser.FuncCall:
		return strings.ToLower(ex.Name)

	case *parser.CastExpr:
		return e.inferColumnName(ex.Expr, index)

	case *parser.ParenExpr:
		return e.inferColumnName(ex.Expr, index)

	default:
		return e.generateColumnName(index)
	}
}

// generateColumnName generates a default column name.
func (e *lineageExtractor) generateColumnName(index int) string {
	return "column" + string(rune('0'+index))
}

// resolveSubqueryFrom resolves the FROM clause for a subquery into the given scope.
// This is a lightweight resolution that handles:
// - Physical tables
// - CTEs (from the subquery's WITH clause)
// - Derived tables (recursively)
// It does NOT resolve WHERE/HAVING to avoid issues with correlated subqueries.
func (e *lineageExtractor) resolveSubqueryFrom(scope *parser.Scope, stmt *parser.SelectStmt, from *parser.FromClause) {
	// First, handle any CTEs in the subquery
	if stmt.With != nil {
		for _, cte := range stmt.With.CTEs {
			if cte.Select != nil && cte.Select.Body != nil && cte.Select.Body.Left != nil {
				cteCore := cte.Select.Body.Left

				// Create a child scope for the CTE
				cteScope := scope.Child()

				// Recursively resolve the CTE's FROM clause
				if cteCore.From != nil {
					e.resolveSubqueryFrom(cteScope, cte.Select, cteCore.From)
				}

				// Extract column names from CTE
				var columns []string
				for _, item := range cteCore.Columns {
					if item.Alias != "" {
						columns = append(columns, item.Alias)
					} else if item.Expr != nil {
						if col, ok := item.Expr.(*parser.ColumnRef); ok {
							columns = append(columns, col.Column)
						}
					}
				}
				scope.RegisterCTE(cte.Name, columns)
			}
		}
	}

	// Register tables from FROM clause
	e.resolveTableRefForSubquery(scope, from.Source)
	for _, join := range from.Joins {
		e.resolveTableRefForSubquery(scope, join.Right)
	}
}

// resolveTableRefForSubquery registers a table reference in the subquery scope.
func (e *lineageExtractor) resolveTableRefForSubquery(scope *parser.Scope, ref parser.TableRef) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *parser.TableName:
		// Check if it references a CTE first
		if _, ok := scope.LookupCTE(t.Name); ok {
			// CTE reference - already registered, no external source
			return
		}
		// Physical table - register in scope and record as source
		scope.RegisterTable(t)

		// Build fully qualified name and record as source
		var parts []string
		if t.Catalog != "" {
			parts = append(parts, t.Catalog)
		}
		if t.Schema != "" {
			parts = append(parts, t.Schema)
		}
		parts = append(parts, t.Name)
		e.sources[strings.Join(parts, ".")] = struct{}{}

	case *parser.DerivedTable:
		// Nested derived table - extract columns and register
		if t.Select != nil && t.Select.Body != nil && t.Select.Body.Left != nil {
			derivedCore := t.Select.Body.Left

			// Create a child scope for the derived table
			derivedScope := scope.Child()

			// Recursively resolve the derived table's FROM clause
			if derivedCore.From != nil {
				e.resolveSubqueryFrom(derivedScope, t.Select, derivedCore.From)
			}

			var columns []string
			for _, item := range derivedCore.Columns {
				if item.Alias != "" {
					columns = append(columns, item.Alias)
				} else if item.Expr != nil {
					if col, ok := item.Expr.(*parser.ColumnRef); ok {
						columns = append(columns, col.Column)
					}
				}
			}
			scope.RegisterDerived(t.Alias, columns)
		}

	case *parser.LateralTable:
		// Similar to DerivedTable
		if t.Select != nil && t.Select.Body != nil && t.Select.Body.Left != nil {
			lateralCore := t.Select.Body.Left

			// Recursively resolve the lateral table's FROM clause
			// Note: LATERAL can see outer scope, so we use the same scope
			if lateralCore.From != nil {
				e.resolveSubqueryFrom(scope, t.Select, lateralCore.From)
			}

			var columns []string
			for _, item := range lateralCore.Columns {
				if item.Alias != "" {
					columns = append(columns, item.Alias)
				}
			}
			scope.RegisterDerived(t.Alias, columns)
		}

	case *parser.PivotTable:
		// PIVOT: resolve the source table recursively
		e.resolveTableRefForSubquery(scope, t.Source)

	case *parser.UnpivotTable:
		// UNPIVOT: resolve the source table recursively
		e.resolveTableRefForSubquery(scope, t.Source)
	}
}
