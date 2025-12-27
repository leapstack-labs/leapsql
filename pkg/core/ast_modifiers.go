package core

// StarModifier is the interface for star expression modifiers (DuckDB).
// Implemented by ExcludeModifier, ReplaceModifier, and RenameModifier.
type StarModifier interface {
	starModifier()
}

// ExcludeModifier represents * EXCLUDE (col1, col2, ...).
type ExcludeModifier struct {
	Columns []string // Column names to exclude
}

func (*ExcludeModifier) starModifier() {}

// ReplaceItem represents a single replacement in REPLACE modifier.
type ReplaceItem struct {
	Expr  Expr   // Expression to use
	Alias string // Column name to replace
}

// ReplaceModifier represents * REPLACE (expr AS col, ...).
type ReplaceModifier struct {
	Items []ReplaceItem
}

func (*ReplaceModifier) starModifier() {}

// RenameItem represents a single rename in RENAME modifier.
type RenameItem struct {
	OldName string
	NewName string
}

// RenameModifier represents * RENAME (old AS new, ...).
type RenameModifier struct {
	Items []RenameItem
}

func (*RenameModifier) starModifier() {}
