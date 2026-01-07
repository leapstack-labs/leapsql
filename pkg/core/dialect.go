package core

import (
	"errors"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/token"
)

// ErrDialectRequired is returned when a dialect is required but not provided.
var ErrDialectRequired = errors.New("dialect is required")

// FunctionLineageType classifies how a function affects lineage.
type FunctionLineageType int

// FunctionLineageType constants classify function behavior for lineage analysis.
const (
	// LineagePassthrough indicates the function passes through columns unchanged.
	LineagePassthrough FunctionLineageType = iota
	// LineageAggregate indicates an aggregate function.
	LineageAggregate
	// LineageGenerator indicates a function that generates values without input columns.
	LineageGenerator
	// LineageWindow indicates a window function.
	LineageWindow
	// LineageTable indicates a table-valued function.
	LineageTable
)

// String returns the string representation of FunctionLineageType.
func (t FunctionLineageType) String() string {
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

// FunctionDoc contains documentation metadata for LSP features.
type FunctionDoc struct {
	Description string
	Signatures  []string
	ReturnType  string
	Example     string
}

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

	// Feature Flags - Builder auto-wires based on these
	//
	// Clause extensions
	SupportsQualify    bool // QUALIFY clause for window filtering
	SupportsReturning  bool // RETURNING clause for DML
	SupportsGroupByAll bool // GROUP BY ALL
	SupportsOrderByAll bool // ORDER BY ALL

	// Operator extensions
	SupportsIlike        bool // ILIKE case-insensitive LIKE
	SupportsCastOperator bool // :: cast operator

	// Join extensions
	SupportsSemiAntiJoins bool // SEMI/ANTI join types
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

// FormatPlaceholder returns a placeholder for the given parameter index (1-based).
// Returns "?" for PlaceholderQuestion style, "$1", "$2" etc. for PlaceholderDollar style.
func (p PlaceholderStyle) FormatPlaceholder(index int) string {
	switch p {
	case PlaceholderDollar:
		return "$" + formatInt(index)
	default: // PlaceholderQuestion
		return "?"
	}
}

// formatInt converts an integer to a string without importing strconv.
func formatInt(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		return "-" + formatInt(-n)
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// IdentifierConfig defines how identifiers are quoted and normalized.
type IdentifierConfig struct {
	Quote         string                // Quote character: ", `, [
	QuoteEnd      string                // End quote character (usually same as Quote, ] for [)
	Escape        string                // Escape sequence: "", ``, ]]
	Normalization NormalizationStrategy // How to normalize unquoted identifiers
}

// Dialect represents a SQL dialect configuration with runtime behavior.
// Handler fields use 'any' to keep core free of spi imports.
// The pkg/dialect Builder provides type-safe construction.
type Dialect struct {
	Name        string
	Identifiers IdentifierConfig

	// Database-specific settings
	DefaultSchema string
	Placeholder   PlaceholderStyle

	// Function classifications (normalized names)
	Aggregates     map[string]struct{}
	Generators     map[string]struct{}
	Windows        map[string]struct{}
	TableFunctions map[string]struct{}

	// Documentation for LSP
	Docs map[string]FunctionDoc

	// Keywords and types for autocomplete
	Keywords      map[string]struct{}
	ReservedWords map[string]struct{}
	DataTypes     []string

	// Parsing behavior - handlers stored as 'any'
	ClauseSeq      []token.TokenType
	ClauseDefs     map[token.TokenType]ClauseDef
	Symbols        map[string]token.TokenType
	DynamicKw      map[string]token.TokenType
	Precedences    map[token.TokenType]int
	InfixHandlers  map[token.TokenType]any // spi.InfixHandler
	PrefixHandlers map[token.TokenType]any // spi.PrefixHandler
	JoinTypes      map[token.TokenType]JoinTypeDef
	StarModifiers  map[token.TokenType]any // spi.StarModifierHandler
	FromItems      map[token.TokenType]any // spi.FromItemHandler
}

// --- Accessor Methods ---

// ClauseSequence returns the ordered list of clause token types.
func (d *Dialect) ClauseSequence() []token.TokenType {
	return d.ClauseSeq
}

// ClauseDefFor returns the definition for a clause token type.
func (d *Dialect) ClauseDefFor(t token.TokenType) (ClauseDef, bool) {
	def, ok := d.ClauseDefs[t]
	return def, ok
}

// IsClauseToken returns true if this dialect supports the given clause.
func (d *Dialect) IsClauseToken(t token.TokenType) bool {
	_, ok := d.ClauseDefs[t]
	return ok
}

// Precedence returns the precedence level for an operator token.
func (d *Dialect) Precedence(t token.TokenType) int {
	if p, ok := d.Precedences[t]; ok {
		return p
	}
	return PrecedenceNone
}

// InfixHandler returns the custom infix handler (as any).
// Caller casts to spi.InfixHandler.
func (d *Dialect) InfixHandler(t token.TokenType) any {
	return d.InfixHandlers[t]
}

// PrefixHandler returns the custom prefix handler (as any).
func (d *Dialect) PrefixHandler(t token.TokenType) any {
	return d.PrefixHandlers[t]
}

// StarModifierHandler returns the handler for a star modifier (as any).
func (d *Dialect) StarModifierHandler(t token.TokenType) any {
	return d.StarModifiers[t]
}

// IsStarModifierToken returns true if the token is a star modifier.
func (d *Dialect) IsStarModifierToken(t token.TokenType) bool {
	return d.StarModifiers[t] != nil
}

// FromItemHandler returns the handler for a FROM item extension (as any).
func (d *Dialect) FromItemHandler(t token.TokenType) any {
	return d.FromItems[t]
}

// IsFromItemToken returns true if the token is a FROM item extension.
func (d *Dialect) IsFromItemToken(t token.TokenType) bool {
	return d.FromItems[t] != nil
}

// JoinTypeDefFor returns the definition for a join type token.
func (d *Dialect) JoinTypeDefFor(t token.TokenType) (JoinTypeDef, bool) {
	def, ok := d.JoinTypes[t]
	return def, ok
}

// IsJoinTypeToken returns true if the token is a dialect-specific join type.
func (d *Dialect) IsJoinTypeToken(t token.TokenType) bool {
	_, ok := d.JoinTypes[t]
	return ok
}

// SymbolsMap returns the custom operators map for lexer symbol matching.
func (d *Dialect) SymbolsMap() map[string]token.TokenType {
	return d.Symbols
}

// LookupKeyword returns the token type for a dynamic keyword.
func (d *Dialect) LookupKeyword(name string) (token.TokenType, bool) {
	lowerName := strings.ToLower(name)
	if t, ok := d.DynamicKw[lowerName]; ok {
		return t, true
	}
	return token.IDENT, false
}

// NormalizeName normalizes an identifier according to dialect rules.
func (d *Dialect) NormalizeName(name string) string {
	switch d.Identifiers.Normalization {
	case NormUppercase:
		return strings.ToUpper(name)
	case NormLowercase, NormCaseInsensitive:
		return strings.ToLower(name)
	default:
		return name
	}
}

// FunctionLineageTypeOf returns the lineage classification for a function.
func (d *Dialect) FunctionLineageTypeOf(name string) FunctionLineageType {
	normalized := d.NormalizeName(name)
	if _, ok := d.TableFunctions[normalized]; ok {
		return LineageTable
	}
	if _, ok := d.Aggregates[normalized]; ok {
		return LineageAggregate
	}
	if _, ok := d.Generators[normalized]; ok {
		return LineageGenerator
	}
	if _, ok := d.Windows[normalized]; ok {
		return LineageWindow
	}
	return LineagePassthrough
}

// IsAggregate returns true if the function is an aggregate function.
func (d *Dialect) IsAggregate(name string) bool {
	return d.FunctionLineageTypeOf(name) == LineageAggregate
}

// IsGenerator returns true if the function generates values.
func (d *Dialect) IsGenerator(name string) bool {
	return d.FunctionLineageTypeOf(name) == LineageGenerator
}

// IsWindow returns true if the function is a window function.
func (d *Dialect) IsWindow(name string) bool {
	return d.FunctionLineageTypeOf(name) == LineageWindow
}

// IsTableFunction returns true if the function acts as a table source.
func (d *Dialect) IsTableFunction(name string) bool {
	return d.FunctionLineageTypeOf(name) == LineageTable
}

// GetDoc returns documentation for a function.
func (d *Dialect) GetDoc(name string) (FunctionDoc, bool) {
	normalized := d.NormalizeName(name)
	doc, ok := d.Docs[normalized]
	return doc, ok
}

// IsReservedWord returns true if the word needs quoting.
func (d *Dialect) IsReservedWord(word string) bool {
	normalized := d.NormalizeName(word)
	_, ok := d.ReservedWords[normalized]
	return ok
}

// QuoteIdentifier quotes an identifier using the dialect's quote characters.
func (d *Dialect) QuoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, d.Identifiers.QuoteEnd, d.Identifiers.Escape)
	return d.Identifiers.Quote + escaped + d.Identifiers.QuoteEnd
}

// QuoteIdentifierIfNeeded quotes an identifier only if it's a reserved word.
func (d *Dialect) QuoteIdentifierIfNeeded(name string) string {
	if d.IsReservedWord(name) {
		return d.QuoteIdentifier(name)
	}
	return name
}

// GetName returns the dialect name.
func (d *Dialect) GetName() string {
	return d.Name
}

// Config returns the pure data configuration for this dialect.
func (d *Dialect) Config() *DialectConfig {
	aggregates := make([]string, 0, len(d.Aggregates))
	for f := range d.Aggregates {
		aggregates = append(aggregates, f)
	}
	generators := make([]string, 0, len(d.Generators))
	for f := range d.Generators {
		generators = append(generators, f)
	}
	windows := make([]string, 0, len(d.Windows))
	for f := range d.Windows {
		windows = append(windows, f)
	}
	tableFunctions := make([]string, 0, len(d.TableFunctions))
	for f := range d.TableFunctions {
		tableFunctions = append(tableFunctions, f)
	}
	keywords := make([]string, 0, len(d.Keywords))
	for kw := range d.Keywords {
		keywords = append(keywords, kw)
	}
	return &DialectConfig{
		Name:           d.Name,
		Identifiers:    d.Identifiers,
		DefaultSchema:  d.DefaultSchema,
		Placeholder:    d.Placeholder,
		Aggregates:     aggregates,
		Generators:     generators,
		Windows:        windows,
		TableFunctions: tableFunctions,
		Keywords:       keywords,
		DataTypes:      d.DataTypes,
	}
}
