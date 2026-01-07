package parser

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"strings"
)

// Schema maps table names to their columns.
// Used for SELECT * expansion when schema information is available.
type Schema map[string][]string

// ScopeType indicates the type of scope entry.
type ScopeType int

const (
	// ScopeTable represents a physical table.
	ScopeTable ScopeType = iota
	// ScopeCTE represents a Common Table Expression.
	ScopeCTE
	// ScopeDerived represents a derived table (subquery in FROM).
	ScopeDerived
)

// ScopeEntry represents a table/CTE/derived table in scope.
type ScopeEntry struct {
	Type              ScopeType
	Name              string   // Original table/CTE name
	Alias             string   // Alias (if any)
	Columns           []string // Known columns (from schema or derived query)
	SourceTable       string   // For physical tables: fully qualified name (schema.table)
	UnderlyingSources []string // For CTEs/derived tables: underlying physical tables
}

// EffectiveName returns the name used to reference this entry (alias if present, else name).
func (e *ScopeEntry) EffectiveName() string {
	if e.Alias != "" {
		return e.Alias
	}
	return e.Name
}

// Scope tracks all available tables, CTEs, and their columns within a query context.
type Scope struct {
	parent  *Scope                 // Parent scope (for nested queries)
	entries map[string]*ScopeEntry // Name/alias -> entry (normalized to lowercase)
	dialect *core.Dialect          // For name normalization
	schema  Schema                 // External schema information
}

// NewScope creates a new root scope.
// Returns an error if dialect is nil.
func NewScope(d *core.Dialect, schema Schema) (*Scope, error) {
	if d == nil {
		return nil, core.ErrDialectRequired
	}
	return &Scope{
		entries: make(map[string]*ScopeEntry),
		dialect: d,
		schema:  schema,
	}, nil
}

// Child creates a child scope for nested queries (subqueries, derived tables).
func (s *Scope) Child() *Scope {
	return &Scope{
		parent:  s,
		entries: make(map[string]*ScopeEntry),
		dialect: s.dialect,
		schema:  s.schema,
	}
}

// normalize normalizes an identifier according to dialect rules.
func (s *Scope) normalize(name string) string {
	return s.dialect.NormalizeName(name)
}

// RegisterCTE registers a CTE with its resolved columns.
func (s *Scope) RegisterCTE(name string, columns []string) {
	normalized := s.normalize(name)
	s.entries[normalized] = &ScopeEntry{
		Type:    ScopeCTE,
		Name:    name,
		Columns: columns,
	}
}

// RegisterCTEWithSources registers a CTE with its resolved columns and underlying sources.
func (s *Scope) RegisterCTEWithSources(name string, columns []string, underlyingSources []string) {
	normalized := s.normalize(name)
	s.entries[normalized] = &ScopeEntry{
		Type:              ScopeCTE,
		Name:              name,
		Columns:           columns,
		UnderlyingSources: underlyingSources,
	}
}

// RegisterTable registers a physical table from a FROM clause.
func (s *Scope) RegisterTable(table *core.TableName) {
	entry := &ScopeEntry{
		Type: ScopeTable,
		Name: table.Name,
	}

	// Build fully qualified source name
	var parts []string
	if table.Catalog != "" {
		parts = append(parts, table.Catalog)
	}
	if table.Schema != "" {
		parts = append(parts, table.Schema)
	}
	parts = append(parts, table.Name)
	entry.SourceTable = strings.Join(parts, ".")

	if table.Alias != "" {
		entry.Alias = table.Alias
	}

	// Try to get columns from schema
	if s.schema != nil {
		// Try various forms of the table name
		for _, key := range []string{
			entry.SourceTable,
			table.Name,
			s.normalize(entry.SourceTable),
			s.normalize(table.Name),
		} {
			if cols, ok := s.schema[key]; ok {
				entry.Columns = cols
				break
			}
		}
	}

	// Register by effective name (alias or table name)
	normalized := s.normalize(entry.EffectiveName())
	s.entries[normalized] = entry
}

// RegisterDerived registers a derived table (subquery in FROM).
func (s *Scope) RegisterDerived(alias string, columns []string) {
	normalized := s.normalize(alias)
	s.entries[normalized] = &ScopeEntry{
		Type:    ScopeDerived,
		Name:    alias,
		Alias:   alias,
		Columns: columns,
	}
}

// RegisterDerivedWithSources registers a derived table with its underlying sources.
func (s *Scope) RegisterDerivedWithSources(alias string, columns []string, underlyingSources []string) {
	normalized := s.normalize(alias)
	s.entries[normalized] = &ScopeEntry{
		Type:              ScopeDerived,
		Name:              alias,
		Alias:             alias,
		Columns:           columns,
		UnderlyingSources: underlyingSources,
	}
}

// Lookup finds a scope entry by name (table name or alias).
// Searches current scope first, then parent scopes.
func (s *Scope) Lookup(name string) (*ScopeEntry, bool) {
	normalized := s.normalize(name)

	if entry, ok := s.entries[normalized]; ok {
		return entry, true
	}

	// Check parent scope (for correlated subqueries, LATERAL, etc.)
	if s.parent != nil {
		return s.parent.Lookup(name)
	}

	return nil, false
}

// LookupCTE looks up a CTE by name.
// CTEs are only looked up in parent scopes (they're defined before the main query).
func (s *Scope) LookupCTE(name string) (*ScopeEntry, bool) {
	normalized := s.normalize(name)

	if entry, ok := s.entries[normalized]; ok && entry.Type == ScopeCTE {
		return entry, true
	}

	if s.parent != nil {
		return s.parent.LookupCTE(name)
	}

	return nil, false
}

// AllEntries returns all scope entries in the current scope (not including parent).
func (s *Scope) AllEntries() []*ScopeEntry {
	entries := make([]*ScopeEntry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	return entries
}

// ResolveColumn attempts to resolve a column reference to its source table.
// Returns the scope entry and true if found, nil and false otherwise.
//
// For unqualified columns, it searches all entries in scope.
// For qualified columns (table.column), it looks up by qualifier.
func (s *Scope) ResolveColumn(ref *core.ColumnRef) (*ScopeEntry, bool) {
	if ref.Table != "" {
		// Qualified reference - lookup by table name/alias
		return s.Lookup(ref.Table)
	}

	// Unqualified - search all entries by column name
	// Return first match (ambiguous references would need more complex handling)
	for _, entry := range s.entries {
		for _, col := range entry.Columns {
			if s.normalize(col) == s.normalize(ref.Column) {
				return entry, true
			}
		}
	}

	// No column match found - try single-table inference
	// If there's exactly one physical table in scope with no schema info,
	// assume unqualified columns belong to it (common for raw/seed tables)
	var singleTable *ScopeEntry
	tableCount := 0
	for _, entry := range s.entries {
		if entry.Type == ScopeTable {
			tableCount++
			singleTable = entry
		}
	}
	if tableCount == 1 && singleTable != nil {
		return singleTable, true
	}

	// Check parent scope
	if s.parent != nil {
		return s.parent.ResolveColumn(ref)
	}

	// Column not found in any known schema - this is OK for tables without schema info
	return nil, false
}

// ExpandStar expands a SELECT * to column references.
// If tableName is empty, expands * for all tables in scope.
// If tableName is provided, expands only for that table.
//
// Returns nil if the table is not found or has no known columns.
func (s *Scope) ExpandStar(tableName string) []*core.ColumnRef {
	if tableName != "" {
		// Expand table.*
		entry, ok := s.Lookup(tableName)
		if !ok || len(entry.Columns) == 0 {
			return nil
		}

		refs := make([]*core.ColumnRef, len(entry.Columns))
		for i, col := range entry.Columns {
			refs[i] = &core.ColumnRef{
				Table:  entry.EffectiveName(),
				Column: col,
			}
		}
		return refs
	}

	// Expand * for all tables
	var refs []*core.ColumnRef
	for _, entry := range s.entries {
		for _, col := range entry.Columns {
			refs = append(refs, &core.ColumnRef{
				Table:  entry.EffectiveName(),
				Column: col,
			})
		}
	}
	return refs
}

// HasSchemaInfo returns true if the scope has column information for any table.
func (s *Scope) HasSchemaInfo() bool {
	for _, entry := range s.entries {
		if len(entry.Columns) > 0 {
			return true
		}
	}
	return false
}

// ColumnSource represents a resolved source for a column reference.
type ColumnSource struct {
	Table       string // Source table name
	SourceTable string // Fully qualified source (e.g., schema.table)
	Column      string // Column name
	FromCTE     bool   // True if from a CTE
	FromDerived bool   // True if from a derived table
}

// ResolveColumnFull resolves a column reference and returns full source information.
func (s *Scope) ResolveColumnFull(ref *core.ColumnRef) (*ColumnSource, bool) {
	entry, ok := s.ResolveColumn(ref)
	if !ok {
		// No schema info - return a best-effort resolution
		if ref.Table != "" {
			return &ColumnSource{
				Table:  ref.Table,
				Column: ref.Column,
			}, true
		}
		// Unqualified column with no schema - can't resolve
		return nil, false
	}

	source := &ColumnSource{
		Table:       entry.EffectiveName(),
		Column:      ref.Column,
		FromCTE:     entry.Type == ScopeCTE,
		FromDerived: entry.Type == ScopeDerived,
	}

	if entry.SourceTable != "" {
		source.SourceTable = entry.SourceTable
	} else {
		source.SourceTable = entry.Name
	}

	return source, true
}
