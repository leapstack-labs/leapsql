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

// Span represents a range in source code.
type Span struct {
	Start Position
	End   Position
}

// Contains returns true if the span contains the given offset.
func (s Span) Contains(offset int) bool {
	return offset >= s.Start.Offset && offset < s.End.Offset
}

// IsValid returns true if both start and end positions are valid.
func (s Span) IsValid() bool {
	return s.Start.IsValid() && s.End.IsValid()
}
