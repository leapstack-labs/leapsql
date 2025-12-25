package core

// DialectConfig holds the static configuration for a SQL dialect.
// This is pure data — no handler functions.
//
// The runtime behavior (clause handlers, infix handlers, etc.) lives in
// pkg/dialect.Dialect, which embeds this config.
type DialectConfig struct {
	// Name is the dialect identifier (e.g., "duckdb", "postgres")
	Name string

	// Identifiers defines quoting and normalization rules
	Identifiers IdentifierConfig

	// DefaultSchema is the default schema name ("main" for DuckDB, "public" for Postgres)
	DefaultSchema string

	// Placeholder defines how query parameters are formatted
	Placeholder PlaceholderStyle

	// Function classifications (normalized names)
	Aggregates     []string // SUM, COUNT, AVG, etc.
	Generators     []string // NOW, UUID, RANDOM, etc.
	Windows        []string // ROW_NUMBER, LAG, LEAD, etc.
	TableFunctions []string // read_csv, generate_series, etc.

	// Keywords for autocomplete/highlighting
	Keywords  []string
	DataTypes []string
}

// NormalizationStrategy defines how unquoted identifiers are normalized.
type NormalizationStrategy int

const (
	// NormLowercase normalizes unquoted identifiers to lowercase (default SQL behavior).
	NormLowercase NormalizationStrategy = iota
	// NormUppercase normalizes unquoted identifiers to uppercase (Snowflake, Oracle).
	NormUppercase
	// NormCaseSensitive preserves identifier case exactly (MySQL, ClickHouse).
	NormCaseSensitive
	// NormCaseInsensitive normalizes to lowercase for comparison (BigQuery, Hive, DuckDB).
	NormCaseInsensitive
)

// PlaceholderStyle defines how query parameters are formatted.
type PlaceholderStyle int

const (
	// PlaceholderQuestion uses ? for all parameters (DuckDB, MySQL, SQLite).
	PlaceholderQuestion PlaceholderStyle = iota
	// PlaceholderDollar uses $1, $2, etc. for parameters (PostgreSQL).
	PlaceholderDollar
)

// IdentifierConfig defines how identifiers are quoted and normalized.
type IdentifierConfig struct {
	Quote         string                // Quote character: ", `, [
	QuoteEnd      string                // End quote character (usually same as Quote, ] for [)
	Escape        string                // Escape sequence: "", ``, ]]
	Normalization NormalizationStrategy // How to normalize unquoted identifiers
}

// LineageType classifies how a function affects column lineage.
type LineageType int

const (
	// LineagePassthrough means all input columns pass through (default).
	LineagePassthrough LineageType = iota
	// LineageAggregate means many rows aggregate to one value.
	LineageAggregate
	// LineageGenerator means function generates values with no upstream.
	LineageGenerator
	// LineageWindow means function requires OVER clause.
	LineageWindow
	// LineageTable means function returns rows as a table source.
	LineageTable
)

// String returns the string representation of LineageType.
func (t LineageType) String() string {
	switch t {
	case LineagePassthrough:
		return "passthrough"
	case LineageAggregate:
		return "aggregate"
	case LineageGenerator:
		return "generator"
	case LineageWindow:
		return "window"
	case LineageTable:
		return "table"
	default:
		return "unknown"
	}
}
