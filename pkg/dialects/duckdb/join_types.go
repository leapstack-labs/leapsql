package duckdb

// DuckDB-specific join type values.
// These are defined as untyped string constants to avoid import cycles.
// The parser uses these values with parser.JoinType(value).
const (
	JoinSemi       = "SEMI"       // Returns rows from left that have matches in right
	JoinAnti       = "ANTI"       // Returns rows from left that have NO matches in right
	JoinAsof       = "ASOF"       // Temporal join matching closest value
	JoinPositional = "POSITIONAL" // Joins by row position (no condition needed)
)
