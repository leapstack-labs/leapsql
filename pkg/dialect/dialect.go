// Package dialect provides SQL dialect configuration and function classification.
//
// This package contains the public contract for dialect definitions used by the parser,
// lineage analyzer, and other SQL-aware components. Concrete dialect implementations
// are registered from pkg/adapters/*/dialect packages.
package dialect

import (
	"strconv"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

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

// Type classifies how a function affects lineage.
type Type int

const (
	// LineagePassthrough means all input columns pass through (default for unknown functions).
	LineagePassthrough Type = iota
	// LineageAggregate means many rows aggregate to one value (SUM, COUNT, etc.).
	LineageAggregate
	// LineageGenerator means function generates values with no upstream columns (NOW, UUID, etc.).
	LineageGenerator
	// LineageWindow means function requires OVER clause (ROW_NUMBER, LAG, etc.).
	LineageWindow
	// LineageTable means function returns rows and acts as a table source (read_csv, generate_series, etc.).
	LineageTable
)

// ClauseDef bundles clause parsing logic with storage destination.
type ClauseDef struct {
	Handler  spi.ClauseHandler
	Slot     spi.ClauseSlot
	Keywords []string // Keywords to print for this clause (e.g. "GROUP", "BY")
	Inline   bool     // true for same-line clauses (LIMIT, OFFSET)
}

// ClauseOption configures a ClauseDef.
type ClauseOption func(*ClauseDef)

// WithInline marks a clause as inline (keyword and value on same line).
func WithInline() ClauseOption {
	return func(c *ClauseDef) {
		c.Inline = true
	}
}

// WithKeywords sets the display keywords for a clause.
func WithKeywords(keywords ...string) ClauseOption {
	return func(c *ClauseDef) {
		c.Keywords = keywords
	}
}

// String returns the string representation of Type.
func (t Type) String() string {
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
	Description string   // Brief description
	Signatures  []string // Overloaded signatures, e.g. ["datediff(part, start, end) -> BIGINT"]
	ReturnType  string   // e.g. "INTEGER", "TABLE", "VARCHAR"
	Example     string   // Optional usage example
}

// Dialect represents a SQL dialect configuration.
type Dialect struct {
	Name        string
	Identifiers IdentifierConfig

	// Database-specific settings
	DefaultSchema string           // Default schema name ("main" for DuckDB, "public" for Postgres)
	Placeholder   PlaceholderStyle // How to format query parameters

	// Function classifications (normalized to dialect's normalization strategy)
	aggregates     map[string]struct{}
	generators     map[string]struct{}
	windows        map[string]struct{}
	tableFunctions map[string]struct{} // Table-valued functions (read_csv, generate_series, etc.)

	// Documentation for LSP
	docs map[string]FunctionDoc

	// Keywords and types for autocomplete/highlighting
	keywords      map[string]struct{} // Reserved keywords for LSP completions
	reservedWords map[string]struct{} // All keywords that need quoting as identifiers
	dataTypes     []string

	// Parsing behavior (NEW - for dialect-aware parsing)
	parent         *Dialect                             // Parent dialect for inheritance
	clauseSequence []token.TokenType                    // Order of clauses in SELECT statement
	clauseDefs     map[token.TokenType]ClauseDef        // Handler + Slot per clause
	symbols        map[string]token.TokenType           // Custom operators: "::" -> DCOLON
	dynamicKw      map[string]token.TokenType           // Custom keywords: "QUALIFY" -> QUALIFY
	precedence     map[token.TokenType]int              // Operator precedence for expressions
	infixHandlers  map[token.TokenType]spi.InfixHandler // Optional custom infix parsing
}

// FunctionLineageType returns the lineage classification for a function.
func (d *Dialect) FunctionLineageType(name string) Type {
	normalized := d.NormalizeName(name)

	// Check table functions first (highest priority)
	if _, ok := d.tableFunctions[normalized]; ok {
		return LineageTable
	}

	if _, ok := d.aggregates[normalized]; ok {
		return LineageAggregate
	}
	if _, ok := d.generators[normalized]; ok {
		return LineageGenerator
	}
	if _, ok := d.windows[normalized]; ok {
		return LineageWindow
	}
	return LineagePassthrough
}

// NormalizeName normalizes an identifier according to dialect rules.
func (d *Dialect) NormalizeName(name string) string {
	switch d.Identifiers.Normalization {
	case NormUppercase:
		return strings.ToUpper(name)
	case NormLowercase, NormCaseInsensitive:
		return strings.ToLower(name)
	default: // NormCaseSensitive
		return name
	}
}

// IsAggregate returns true if the function is an aggregate function.
func (d *Dialect) IsAggregate(name string) bool {
	return d.FunctionLineageType(name) == LineageAggregate
}

// IsGenerator returns true if the function generates values without input columns.
func (d *Dialect) IsGenerator(name string) bool {
	return d.FunctionLineageType(name) == LineageGenerator
}

// IsWindow returns true if the function is a window-only function.
func (d *Dialect) IsWindow(name string) bool {
	return d.FunctionLineageType(name) == LineageWindow
}

// IsTableFunction returns true if the function acts as a table source.
func (d *Dialect) IsTableFunction(name string) bool {
	return d.FunctionLineageType(name) == LineageTable
}

// GetDoc returns documentation for a function.
func (d *Dialect) GetDoc(name string) (FunctionDoc, bool) {
	normalized := d.NormalizeName(name)
	doc, ok := d.docs[normalized]
	return doc, ok
}

// AllFunctions returns all known function names.
func (d *Dialect) AllFunctions() []string {
	seen := make(map[string]struct{})
	var funcs []string

	// Collect from all function categories
	for f := range d.aggregates {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}
	for f := range d.generators {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}
	for f := range d.windows {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}
	for f := range d.tableFunctions {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}

	// Also include any functions that have documentation but aren't classified
	for f := range d.docs {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}

	return funcs
}

// Keywords returns all reserved keywords.
func (d *Dialect) Keywords() []string {
	kws := make([]string, 0, len(d.keywords))
	for kw := range d.keywords {
		kws = append(kws, kw)
	}
	return kws
}

// DataTypes returns all supported data types.
func (d *Dialect) DataTypes() []string {
	return d.dataTypes
}

// FormatPlaceholder returns a placeholder for the given parameter index (1-based).
// Returns "?" for PlaceholderQuestion style, "$1", "$2" etc. for PlaceholderDollar style.
func (d *Dialect) FormatPlaceholder(index int) string {
	switch d.Placeholder {
	case PlaceholderDollar:
		return "$" + strconv.Itoa(index)
	default: // PlaceholderQuestion
		return "?"
	}
}

// IsReservedWord returns true if the word needs quoting when used as an identifier.
func (d *Dialect) IsReservedWord(word string) bool {
	normalized := d.NormalizeName(word)
	_, ok := d.reservedWords[normalized]
	return ok
}

// QuoteIdentifier quotes an identifier using the dialect's quote characters.
func (d *Dialect) QuoteIdentifier(name string) string {
	// Escape any existing quote end characters in the name (e.g., ] -> ]])
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

// ---------- Parsing Behavior Methods ----------

// ClauseSequence returns the ordered list of clause token types for this dialect.
func (d *Dialect) ClauseSequence() []token.TokenType {
	if d.clauseSequence != nil {
		return d.clauseSequence
	}
	if d.parent != nil {
		return d.parent.ClauseSequence()
	}
	return nil
}

// ClauseHandler returns the handler for a clause token type.
func (d *Dialect) ClauseHandler(t token.TokenType) spi.ClauseHandler {
	if d.clauseDefs != nil {
		if def, ok := d.clauseDefs[t]; ok {
			return def.Handler
		}
	}
	if d.parent != nil {
		return d.parent.ClauseHandler(t)
	}
	return nil
}

// ClauseDef returns the definition (handler + slot) for a clause token type.
func (d *Dialect) ClauseDef(t token.TokenType) (ClauseDef, bool) {
	if d.clauseDefs != nil {
		if def, ok := d.clauseDefs[t]; ok {
			return def, true
		}
	}
	if d.parent != nil {
		return d.parent.ClauseDef(t)
	}
	return ClauseDef{}, false
}

// IsClauseToken returns true if this dialect supports the given clause token.
func (d *Dialect) IsClauseToken(t token.TokenType) bool {
	_, ok := d.ClauseDef(t)
	return ok
}

// AllClauseTokens returns all clause tokens registered in this dialect.
func (d *Dialect) AllClauseTokens() []token.TokenType {
	seen := make(map[token.TokenType]bool)
	var tokens []token.TokenType

	for t := range d.clauseDefs {
		if !seen[t] {
			seen[t] = true
			tokens = append(tokens, t)
		}
	}
	if d.parent != nil {
		for _, t := range d.parent.AllClauseTokens() {
			if !seen[t] {
				seen[t] = true
				tokens = append(tokens, t)
			}
		}
	}
	return tokens
}

// Symbols returns the custom operators map for lexer symbol matching.
func (d *Dialect) Symbols() map[string]token.TokenType {
	if d.symbols != nil {
		return d.symbols
	}
	if d.parent != nil {
		return d.parent.Symbols()
	}
	return nil
}

// LookupKeyword returns the token type for a dynamic keyword.
// Returns the token type and true if found, or IDENT and false if not.
func (d *Dialect) LookupKeyword(name string) (token.TokenType, bool) {
	lowerName := strings.ToLower(name)
	if d.dynamicKw != nil {
		if t, ok := d.dynamicKw[lowerName]; ok {
			return t, true
		}
	}
	if d.parent != nil {
		return d.parent.LookupKeyword(name)
	}
	return token.IDENT, false
}

// Precedence returns the precedence level for an operator token.
// Returns 0 (PrecedenceNone) if the operator is not recognized.
func (d *Dialect) Precedence(t token.TokenType) int {
	if d.precedence != nil {
		if p, ok := d.precedence[t]; ok {
			return p
		}
	}
	if d.parent != nil {
		return d.parent.Precedence(t)
	}
	return spi.PrecedenceNone
}

// InfixHandler returns the custom infix handler for an operator token.
func (d *Dialect) InfixHandler(t token.TokenType) spi.InfixHandler {
	if d.infixHandlers != nil {
		if h, ok := d.infixHandlers[t]; ok {
			return h
		}
	}
	if d.parent != nil {
		return d.parent.InfixHandler(t)
	}
	return nil
}

// Parent returns the parent dialect, if any.
func (d *Dialect) Parent() *Dialect {
	return d.parent
}

// Builder provides a fluent API for constructing dialects.
type Builder struct {
	dialect *Dialect
}

// NewDialect creates a new dialect builder with the given name.
func NewDialect(name string) *Builder {
	return &Builder{
		dialect: &Dialect{
			Name: name,
			Identifiers: IdentifierConfig{
				Quote:         `"`,
				QuoteEnd:      `"`,
				Escape:        `""`,
				Normalization: NormLowercase,
			},
			aggregates:     make(map[string]struct{}),
			generators:     make(map[string]struct{}),
			windows:        make(map[string]struct{}),
			tableFunctions: make(map[string]struct{}),
			docs:           make(map[string]FunctionDoc),
			keywords:       make(map[string]struct{}),
			reservedWords:  make(map[string]struct{}),
			dataTypes:      nil,
			// Parsing behavior
			clauseSequence: nil,
			clauseDefs:     make(map[token.TokenType]ClauseDef),
			symbols:        make(map[string]token.TokenType),
			dynamicKw:      make(map[string]token.TokenType),
			precedence:     make(map[token.TokenType]int),
			infixHandlers:  make(map[token.TokenType]spi.InfixHandler),
		},
	}
}

// Identifiers configures identifier quoting and normalization.
func (b *Builder) Identifiers(quote, quoteEnd, escape string, norm NormalizationStrategy) *Builder {
	b.dialect.Identifiers = IdentifierConfig{
		Quote:         quote,
		QuoteEnd:      quoteEnd,
		Escape:        escape,
		Normalization: norm,
	}
	return b
}

// Aggregates adds aggregate functions to the dialect.
func (b *Builder) Aggregates(funcs ...string) *Builder {
	for _, f := range funcs {
		b.dialect.aggregates[b.dialect.NormalizeName(f)] = struct{}{}
	}
	return b
}

// Generators adds generator functions (no input columns) to the dialect.
func (b *Builder) Generators(funcs ...string) *Builder {
	for _, f := range funcs {
		b.dialect.generators[b.dialect.NormalizeName(f)] = struct{}{}
	}
	return b
}

// Windows adds window-only functions to the dialect.
func (b *Builder) Windows(funcs ...string) *Builder {
	for _, f := range funcs {
		b.dialect.windows[b.dialect.NormalizeName(f)] = struct{}{}
	}
	return b
}

// TableFunctions adds table-valued functions to the dialect.
func (b *Builder) TableFunctions(funcs ...string) *Builder {
	for _, f := range funcs {
		b.dialect.tableFunctions[b.dialect.NormalizeName(f)] = struct{}{}
	}
	return b
}

// WithDocs registers documentation for functions.
func (b *Builder) WithDocs(docs map[string]FunctionDoc) *Builder {
	for name, doc := range docs {
		b.dialect.docs[b.dialect.NormalizeName(name)] = doc
	}
	return b
}

// WithKeywords registers reserved keywords.
func (b *Builder) WithKeywords(kws ...string) *Builder {
	for _, kw := range kws {
		b.dialect.keywords[b.dialect.NormalizeName(kw)] = struct{}{}
	}
	return b
}

// WithDataTypes registers supported data types.
func (b *Builder) WithDataTypes(types ...string) *Builder {
	b.dialect.dataTypes = append(b.dialect.dataTypes, types...)
	return b
}

// DefaultSchema sets the default schema name.
func (b *Builder) DefaultSchema(schema string) *Builder {
	b.dialect.DefaultSchema = schema
	return b
}

// PlaceholderStyle sets how query parameters are formatted.
func (b *Builder) PlaceholderStyle(style PlaceholderStyle) *Builder {
	b.dialect.Placeholder = style
	return b
}

// WithReservedWords registers words that need quoting when used as identifiers.
func (b *Builder) WithReservedWords(words ...string) *Builder {
	if b.dialect.reservedWords == nil {
		b.dialect.reservedWords = make(map[string]struct{})
	}
	for _, w := range words {
		b.dialect.reservedWords[b.dialect.NormalizeName(w)] = struct{}{}
	}
	return b
}

// Build returns the constructed dialect.
func (b *Builder) Build() *Dialect {
	return b.dialect
}

// ---------- Parsing Behavior Builder Methods ----------

// Extends inherits from a parent dialect (deep copy of parsing behavior).
func (b *Builder) Extends(parent *Dialect) *Builder {
	b.dialect.parent = parent
	// Deep copy parent's parsing behavior
	if parent.clauseSequence != nil {
		b.dialect.clauseSequence = make([]token.TokenType, len(parent.clauseSequence))
		copy(b.dialect.clauseSequence, parent.clauseSequence)
	}
	if parent.clauseDefs != nil {
		for k, v := range parent.clauseDefs {
			b.dialect.clauseDefs[k] = v
		}
	}
	if parent.symbols != nil {
		for k, v := range parent.symbols {
			b.dialect.symbols[k] = v
		}
	}
	if parent.dynamicKw != nil {
		for k, v := range parent.dynamicKw {
			b.dialect.dynamicKw[k] = v
		}
	}
	if parent.precedence != nil {
		for k, v := range parent.precedence {
			b.dialect.precedence[k] = v
		}
	}
	if parent.infixHandlers != nil {
		for k, v := range parent.infixHandlers {
			b.dialect.infixHandlers[k] = v
		}
	}
	return b
}

// AddOperator registers a custom operator symbol for the lexer.
func (b *Builder) AddOperator(symbol string, t token.TokenType) *Builder {
	b.dialect.symbols[symbol] = t
	return b
}

// AddKeyword registers a dynamic keyword for the lexer.
func (b *Builder) AddKeyword(name string, t token.TokenType) *Builder {
	b.dialect.dynamicKw[strings.ToLower(name)] = t
	return b
}

// ClauseSequence sets the full clause sequence (for base dialects).
func (b *Builder) ClauseSequence(tokens ...token.TokenType) *Builder {
	b.dialect.clauseSequence = tokens
	return b
}

// ClauseHandler registers a handler for a clause token with storage slot.
func (b *Builder) ClauseHandler(t token.TokenType, handler spi.ClauseHandler, slot spi.ClauseSlot, opts ...ClauseOption) *Builder {
	def := ClauseDef{Handler: handler, Slot: slot}
	for _, opt := range opts {
		opt(&def)
	}
	b.dialect.clauseDefs[t] = def
	// Register globally for error messages
	recordClause(t, t.String())
	return b
}

// AddClauseAfter inserts a clause into the sequence after another clause.
func (b *Builder) AddClauseAfter(after, t token.TokenType, handler spi.ClauseHandler, slot spi.ClauseSlot, opts ...ClauseOption) *Builder {
	// Find the position of 'after' in the sequence
	for i, tok := range b.dialect.clauseSequence {
		if tok == after {
			// Insert t after position i
			newSeq := make([]token.TokenType, 0, len(b.dialect.clauseSequence)+1)
			newSeq = append(newSeq, b.dialect.clauseSequence[:i+1]...)
			newSeq = append(newSeq, t)
			newSeq = append(newSeq, b.dialect.clauseSequence[i+1:]...)
			b.dialect.clauseSequence = newSeq
			break
		}
	}
	def := ClauseDef{Handler: handler, Slot: slot}
	for _, opt := range opts {
		opt(&def)
	}
	b.dialect.clauseDefs[t] = def
	// Register globally for error messages
	recordClause(t, t.String())
	return b
}

// RemoveClause removes a clause from the sequence.
func (b *Builder) RemoveClause(t token.TokenType) *Builder {
	for i, tok := range b.dialect.clauseSequence {
		if tok == t {
			b.dialect.clauseSequence = append(b.dialect.clauseSequence[:i], b.dialect.clauseSequence[i+1:]...)
			break
		}
	}
	delete(b.dialect.clauseDefs, t)
	return b
}

// AddInfix registers an infix operator with precedence.
func (b *Builder) AddInfix(t token.TokenType, precedence int) *Builder {
	b.dialect.precedence[t] = precedence
	return b
}

// AddInfixWithHandler registers an infix operator with custom handler.
func (b *Builder) AddInfixWithHandler(t token.TokenType, precedence int, handler spi.InfixHandler) *Builder {
	b.dialect.precedence[t] = precedence
	b.dialect.infixHandlers[t] = handler
	return b
}
