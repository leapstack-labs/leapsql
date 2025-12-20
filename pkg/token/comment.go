package token

// CommentKind distinguishes line vs block comments.
type CommentKind int

// Comment kinds.
const (
	LineComment  CommentKind = iota // -- comment
	BlockComment                    // /* comment */
)

// Comment represents a SQL comment with position.
type Comment struct {
	Kind CommentKind
	Text string // includes delimiters (-- or /* */)
	Span Span
}

// IsLineComment returns true if this is a line comment.
func (c *Comment) IsLineComment() bool {
	return c.Kind == LineComment
}

// IsBlockComment returns true if this is a block comment.
func (c *Comment) IsBlockComment() bool {
	return c.Kind == BlockComment
}
