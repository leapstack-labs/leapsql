package core

import "github.com/leapstack-labs/leapsql/pkg/token"

// ---------- Table Reference Types ----------

// TableName represents a table name reference.
type TableName struct {
	NodeInfo
	Catalog string
	Schema  string
	Name    string
	Alias   string
}

func (*TableName) tableRefNode() {}

// Pos implements Node.
func (t *TableName) Pos() token.Position { return t.NodeInfo.Pos() }

// End implements Node.
func (t *TableName) End() token.Position { return t.NodeInfo.End() }

// DerivedTable represents a subquery in FROM clause.
type DerivedTable struct {
	NodeInfo
	Select *SelectStmt
	Alias  string
}

func (*DerivedTable) tableRefNode() {}

// Pos implements Node.
func (d *DerivedTable) Pos() token.Position { return d.NodeInfo.Pos() }

// End implements Node.
func (d *DerivedTable) End() token.Position { return d.NodeInfo.End() }

// LateralTable represents a LATERAL subquery.
type LateralTable struct {
	NodeInfo
	Select *SelectStmt
	Alias  string
}

func (*LateralTable) tableRefNode() {}

// Pos implements Node.
func (l *LateralTable) Pos() token.Position { return l.NodeInfo.Pos() }

// End implements Node.
func (l *LateralTable) End() token.Position { return l.NodeInfo.End() }

// MacroTable represents a macro used as a table reference (e.g., {{ ref('table') }}).
type MacroTable struct {
	NodeInfo
	Content string // raw {{ ... }} content including delimiters
	Alias   string
}

func (*MacroTable) tableRefNode() {}

// Pos implements Node.
func (m *MacroTable) Pos() token.Position { return m.NodeInfo.Pos() }

// End implements Node.
func (m *MacroTable) End() token.Position { return m.NodeInfo.End() }

// ---------- DuckDB PIVOT/UNPIVOT Table References ----------

// PivotTable represents a PIVOT operation in FROM clause.
// SELECT * FROM table PIVOT (agg FOR col IN (values))
type PivotTable struct {
	NodeInfo
	Source     TableRef         // The source table/subquery
	Aggregates []PivotAggregate // Aggregate functions to compute
	ForColumn  string           // Column to pivot on
	InValues   []PivotInValue   // Values to pivot (or InStar=true for *)
	InStar     bool             // true if IN *
	Alias      string           // Optional alias
}

func (*PivotTable) tableRefNode() {}

// Pos implements Node.
func (p *PivotTable) Pos() token.Position { return p.NodeInfo.Pos() }

// End implements Node.
func (p *PivotTable) End() token.Position { return p.NodeInfo.End() }

// PivotAggregate represents an aggregate in PIVOT.
type PivotAggregate struct {
	Func  *FuncCall // The aggregate function
	Alias string    // Optional alias for the result columns
}

// PivotInValue represents a value in PIVOT ... IN (...).
type PivotInValue struct {
	Value Expr   // The value (literal, identifier, or expression)
	Alias string // Optional column name alias
}

// UnpivotTable represents an UNPIVOT operation in FROM clause.
// SELECT * FROM table UNPIVOT (value FOR name IN (columns))
type UnpivotTable struct {
	NodeInfo
	Source       TableRef         // The source table/subquery
	ValueColumns []string         // Output column(s) for values
	NameColumn   string           // Output column for names
	InColumns    []UnpivotInGroup // Source columns to unpivot
	Alias        string           // Optional alias
}

func (*UnpivotTable) tableRefNode() {}

// Pos implements Node.
func (u *UnpivotTable) Pos() token.Position { return u.NodeInfo.Pos() }

// End implements Node.
func (u *UnpivotTable) End() token.Position { return u.NodeInfo.End() }

// UnpivotInGroup represents a group of columns in UNPIVOT ... IN (...).
// For simple UNPIVOT: single column per group.
// For multi-column UNPIVOT: multiple columns per group.
type UnpivotInGroup struct {
	Columns []string // Column name(s) in this group
	Alias   string   // Optional row value alias
}
