package ansi

// Standard ANSI SQL join type values.
// These are defined as untyped string constants to avoid import cycles.
// The parser uses these values with parser.JoinType(value).
const (
	JoinInner = "INNER"
	JoinLeft  = "LEFT"
	JoinRight = "RIGHT"
	JoinFull  = "FULL"
	JoinCross = "CROSS"
)
