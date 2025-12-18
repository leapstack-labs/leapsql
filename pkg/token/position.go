package token

// Position represents a location in the source code.
type Position struct {
	Line   int // 1-based line number
	Column int // 1-based column number
	Offset int // 0-based byte offset
}

// IsValid returns true if the position is valid (line > 0).
func (p Position) IsValid() bool {
	return p.Line > 0
}
