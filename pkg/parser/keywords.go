package parser

// Soft keywords are identifiers that have special meaning in specific contexts.
// They are not reserved words and can be used as identifiers elsewhere.
// Example: "NAME" is a soft keyword in "UNION BY NAME" but can still be used
// as a column name in "SELECT name FROM users".
const (
	SoftKeywordName  = "NAME"
	SoftKeywordValue = "VALUE" // For future PIVOT/UNPIVOT support
)
