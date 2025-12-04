package lineage

import (
	"sort"
	"strings"
)

// TransformType describes how source columns are transformed.
type TransformType string

const (
	// TransformDirect means the column is a direct copy (no transformation).
	TransformDirect TransformType = ""
	// TransformExpression means the column is derived from an expression.
	TransformExpression TransformType = "EXPR"
)

// SourceColumn represents a source column in the lineage.
type SourceColumn struct {
	Table  string // Source table name (may be qualified: schema.table)
	Column string // Column name
}

// ColumnLineage describes the lineage of a single output column.
type ColumnLineage struct {
	Name      string         // Output column name
	Sources   []SourceColumn // Source columns this output derives from
	Transform TransformType  // Type of transformation applied
	Function  string         // Function name (for aggregates/window functions)
}

// ModelLineage describes the complete lineage of a SQL model.
type ModelLineage struct {
	Sources []string         // All source tables (deduplicated, sorted)
	Columns []*ColumnLineage // Lineage for each output column
}

// ExtractLineageOptions configures the lineage extraction.
type ExtractLineageOptions struct {
	Dialect *Dialect // SQL dialect (defaults to DuckDB)
	Schema  Schema   // Schema information for star expansion
}

// ExtractLineage extracts column-level lineage from a SQL statement.
// The schema parameter is optional but required for SELECT * expansion.
func ExtractLineage(sql string, schema Schema) (*ModelLineage, error) {
	return ExtractLineageWithOptions(sql, ExtractLineageOptions{Schema: schema})
}

// ExtractLineageWithOptions extracts lineage with full configuration options.
func ExtractLineageWithOptions(sql string, opts ExtractLineageOptions) (*ModelLineage, error) {
	dialect := opts.Dialect
	if dialect == nil {
		dialect = DefaultDialect()
	}

	// Parse the SQL
	stmt, err := Parse(sql)
	if err != nil {
		return nil, err
	}

	// Create extractor
	extractor := &lineageExtractor{
		dialect: dialect,
		schema:  opts.Schema,
		sources: make(map[string]struct{}),
	}

	// Extract lineage
	return extractor.extract(stmt)
}

// lineageExtractor walks the AST to extract lineage information.
type lineageExtractor struct {
	dialect *Dialect
	schema  Schema
	sources map[string]struct{} // Collected source tables
}

// extract extracts lineage from a parsed statement.
func (e *lineageExtractor) extract(stmt *SelectStmt) (*ModelLineage, error) {
	if stmt == nil || stmt.Body == nil {
		return nil, &ParseError{Message: "empty statement"}
	}

	// Resolve scopes
	resolver := NewResolver(e.dialect, e.schema)
	scope, err := resolver.Resolve(stmt)
	if err != nil {
		return nil, err
	}

	// Extract column lineage from the main SELECT body
	columns := e.extractBodyLineage(scope, stmt.Body)

	// Collect all source tables
	e.collectSources(scope)

	// Build result
	result := &ModelLineage{
		Sources: e.getSortedSources(),
		Columns: columns,
	}

	return result, nil
}

// extractBodyLineage extracts lineage from a SELECT body.
func (e *lineageExtractor) extractBodyLineage(scope *Scope, body *SelectBody) []*ColumnLineage {
	if body == nil || body.Left == nil {
		return nil
	}

	// Extract from the left (main) SELECT
	columns := e.extractCoreLineage(scope, body.Left)

	// Handle set operations (UNION, INTERSECT, EXCEPT)
	if body.Right != nil {
		// For set operations, we merge the lineage from both sides
		// The output columns come from the left side, but sources come from both
		rightColumns := e.extractBodyLineage(scope, body.Right)

		// Merge sources from right side into left columns
		for i, col := range columns {
			if i < len(rightColumns) {
				col.Sources = e.mergeSources(col.Sources, rightColumns[i].Sources)
				// Mark as expression since it's a union of values
				if col.Transform == TransformDirect {
					col.Transform = TransformExpression
				}
			}
		}
	}

	return columns
}

// extractCoreLineage extracts lineage from a SELECT core.
func (e *lineageExtractor) extractCoreLineage(scope *Scope, core *SelectCore) []*ColumnLineage {
	if core == nil {
		return nil
	}

	// Register tables from FROM clause
	if core.From != nil {
		e.registerFromClause(scope, core.From)
	}

	var columns []*ColumnLineage
	colResolver := NewColumnResolver(scope, e.dialect)

	for i, item := range core.Columns {
		lineages := e.extractSelectItemLineage(scope, colResolver, item, i)
		columns = append(columns, lineages...)
	}

	return columns
}

// extractSelectItemLineage extracts lineage from a single SELECT item.
func (e *lineageExtractor) extractSelectItemLineage(scope *Scope, colResolver *ColumnResolver, item SelectItem, index int) []*ColumnLineage {
	// Handle SELECT *
	if item.Star {
		return e.expandStar(scope, "", index)
	}

	// Handle SELECT table.*
	if item.TableStar != "" {
		return e.expandStar(scope, item.TableStar, index)
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

// expandStar expands a SELECT * or table.* into individual column lineages.
func (e *lineageExtractor) expandStar(scope *Scope, tableName string, startIndex int) []*ColumnLineage {
	refs := scope.ExpandStar(tableName)
	if len(refs) == 0 {
		// No schema info available - return a single "unknown" lineage
		name := "*"
		if tableName != "" {
			name = tableName + ".*"
		}
		return []*ColumnLineage{{
			Name:      name,
			Transform: TransformDirect,
		}}
	}

	var lineages []*ColumnLineage
	for _, ref := range refs {
		source := SourceColumn{
			Table:  ref.Table,
			Column: ref.Column,
		}

		// Record the source table
		if entry, ok := scope.Lookup(ref.Table); ok {
			if entry.SourceTable != "" {
				e.sources[entry.SourceTable] = struct{}{}
			} else {
				e.sources[entry.Name] = struct{}{}
			}
		}

		lineages = append(lineages, &ColumnLineage{
			Name:      ref.Column,
			Sources:   []SourceColumn{source},
			Transform: TransformDirect,
		})
	}

	return lineages
}

// extractExprLineage extracts lineage from an expression.
func (e *lineageExtractor) extractExprLineage(scope *Scope, colResolver *ColumnResolver, expr Expr) *ColumnLineage {
	lineage := &ColumnLineage{}

	if expr == nil {
		return lineage
	}

	switch ex := expr.(type) {
	case *ColumnRef:
		// Direct column reference
		source := e.resolveColumnRef(scope, ex)
		if source != nil {
			lineage.Sources = []SourceColumn{*source}
		}
		lineage.Transform = TransformDirect

	case *Literal:
		// Literals have no source columns
		lineage.Transform = TransformExpression

	case *FuncCall:
		// Collect all column refs from the function arguments
		sources := e.collectExprSources(scope, colResolver, expr)
		lineage.Sources = sources

		// Determine transform type based on function classification
		funcType := e.dialect.FunctionLineageType(ex.Name)
		switch funcType {
		case LineageAggregate:
			lineage.Transform = TransformExpression
			lineage.Function = e.dialect.CanonicalFunctionName(ex.Name)
		case LineageWindow:
			lineage.Transform = TransformExpression
			lineage.Function = e.dialect.CanonicalFunctionName(ex.Name)
		case LineageGenerator:
			// Generator functions have no source columns
			lineage.Sources = nil
			lineage.Transform = TransformExpression
			lineage.Function = e.dialect.CanonicalFunctionName(ex.Name)
		default:
			// Passthrough - keep all source columns
			if len(sources) == 1 {
				lineage.Transform = TransformDirect
			} else {
				lineage.Transform = TransformExpression
			}
		}

	case *CaseExpr:
		// Collect all column refs from CASE expression
		sources := e.collectExprSources(scope, colResolver, expr)
		lineage.Sources = sources
		lineage.Transform = TransformExpression

	case *CastExpr:
		// CAST preserves lineage but is a transformation
		innerLineage := e.extractExprLineage(scope, colResolver, ex.Expr)
		lineage.Sources = innerLineage.Sources
		lineage.Transform = TransformExpression

	case *BinaryExpr:
		// Collect sources from both sides
		sources := e.collectExprSources(scope, colResolver, expr)
		lineage.Sources = sources
		lineage.Transform = TransformExpression

	case *UnaryExpr:
		sources := e.collectExprSources(scope, colResolver, expr)
		lineage.Sources = sources
		lineage.Transform = TransformExpression

	case *ParenExpr:
		return e.extractExprLineage(scope, colResolver, ex.Expr)

	default:
		// For other expression types, collect all column references
		sources := e.collectExprSources(scope, colResolver, expr)
		lineage.Sources = sources
		if len(sources) > 1 {
			lineage.Transform = TransformExpression
		}
	}

	return lineage
}

// collectExprSources collects all source columns from an expression.
func (e *lineageExtractor) collectExprSources(scope *Scope, colResolver *ColumnResolver, expr Expr) []SourceColumn {
	refs := colResolver.CollectColumns(expr)
	var sources []SourceColumn
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
func (e *lineageExtractor) resolveColumnRef(scope *Scope, ref *ColumnRef) *SourceColumn {
	if ref == nil {
		return nil
	}

	// Try to resolve through scope
	resolved, ok := scope.ResolveColumnFull(ref)
	if ok {
		// Record the source table
		if resolved.SourceTable != "" {
			e.sources[resolved.SourceTable] = struct{}{}
		} else {
			e.sources[resolved.Table] = struct{}{}
		}

		return &SourceColumn{
			Table:  resolved.SourceTable,
			Column: resolved.Column,
		}
	}

	// Fallback: use the reference as-is
	if ref.Table != "" {
		e.sources[ref.Table] = struct{}{}
		return &SourceColumn{
			Table:  ref.Table,
			Column: ref.Column,
		}
	}

	// Unqualified column with no resolution - still include it
	return &SourceColumn{
		Column: ref.Column,
	}
}

// registerFromClause registers tables from a FROM clause.
func (e *lineageExtractor) registerFromClause(scope *Scope, from *FromClause) {
	if from == nil {
		return
	}

	e.registerTableRef(scope, from.Source)

	for _, join := range from.Joins {
		e.registerTableRef(scope, join.Right)
	}
}

// registerTableRef registers a table reference as a source.
func (e *lineageExtractor) registerTableRef(scope *Scope, ref TableRef) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *TableName:
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

	case *DerivedTable:
		// Derived tables don't add sources directly
		// Their inner queries' sources are collected when we process them

	case *LateralTable:
		// Same as derived tables
	}
}

// collectSources collects sources from scope entries.
func (e *lineageExtractor) collectSources(scope *Scope) {
	for _, entry := range scope.AllEntries() {
		switch entry.Type {
		case ScopeTable:
			if entry.SourceTable != "" {
				e.sources[entry.SourceTable] = struct{}{}
			} else {
				e.sources[entry.Name] = struct{}{}
			}
		case ScopeCTE, ScopeDerived:
			// For CTEs and derived tables, use underlying sources
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
func (e *lineageExtractor) mergeSources(a, b []SourceColumn) []SourceColumn {
	seen := make(map[string]struct{})
	var result []SourceColumn

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
func (e *lineageExtractor) inferColumnName(expr Expr, index int) string {
	if expr == nil {
		return e.generateColumnName(index)
	}

	switch ex := expr.(type) {
	case *ColumnRef:
		return ex.Column

	case *FuncCall:
		return strings.ToLower(ex.Name)

	case *CastExpr:
		return e.inferColumnName(ex.Expr, index)

	case *ParenExpr:
		return e.inferColumnName(ex.Expr, index)

	default:
		return e.generateColumnName(index)
	}
}

// generateColumnName generates a default column name.
func (e *lineageExtractor) generateColumnName(index int) string {
	return "column" + string(rune('0'+index))
}
