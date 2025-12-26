// Package dialect provides SQL dialect configuration and function classification.
//
// This package contains the public contract for dialect definitions used by the parser,
// lineage analyzer, and other SQL-aware components. Concrete dialect implementations
// are registered from pkg/dialects/*/ packages.
package dialect

import (
	"strconv"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

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

// JoinTypeDef defines a dialect-specific join type.
type JoinTypeDef struct {
	Token         token.TokenType // The trigger token for this join type
	Type          string          // JoinType value (e.g., "LEFT", "SEMI")
	OptionalToken token.TokenType // Optional modifier token (OUTER) - 0 means none
	RequiresOn    bool            // true if ON clause is required
	AllowsUsing   bool            // true if USING clause is allowed
}

// ClauseDef bundles clause parsing logic with storage destination.
type ClauseDef struct {
	Token    token.TokenType   // The trigger token for this clause (e.g., token.WHERE)
	Handler  spi.ClauseHandler // Handler function to parse the clause
	Slot     spi.ClauseSlot    // Where to store the parsed result
	Keywords []string          // Keywords to print for this clause (e.g. "GROUP", "BY")
	Inline   bool              // true for same-line clauses (LIMIT, OFFSET)
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
	Identifiers core.IdentifierConfig

	// Database-specific settings
	DefaultSchema string                // Default schema name ("main" for DuckDB, "public" for Postgres)
	Placeholder   core.PlaceholderStyle // How to format query parameters

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

	// Parsing behavior (for dialect-aware parsing)
	clauseSequence []token.TokenType                           // Order of clauses in SELECT statement
	clauseDefs     map[token.TokenType]ClauseDef               // Handler + Slot per clause
	symbols        map[string]token.TokenType                  // Custom operators: "::" -> DCOLON
	dynamicKw      map[string]token.TokenType                  // Custom keywords: "QUALIFY" -> QUALIFY
	precedence     map[token.TokenType]int                     // Operator precedence for expressions
	infixHandlers  map[token.TokenType]spi.InfixHandler        // Optional custom infix parsing
	prefixHandlers map[token.TokenType]spi.PrefixHandler       // Prefix expression handlers (e.g., [ for list literals)
	joinTypes      map[token.TokenType]JoinTypeDef             // Dialect-specific join types
	starModifiers  map[token.TokenType]spi.StarModifierHandler // Star expression modifiers (EXCLUDE, REPLACE, RENAME)
	fromItems      map[token.TokenType]spi.FromItemHandler     // FROM clause extensions (PIVOT, UNPIVOT, etc.)

}

// Config returns the pure data configuration for this dialect.
// This provides access to dialect configuration without SPI dependencies.
func (d *Dialect) Config() *core.DialectConfig {
	// Collect aggregates
	aggregates := make([]string, 0, len(d.aggregates))
	for f := range d.aggregates {
		aggregates = append(aggregates, f)
	}

	// Collect generators
	generators := make([]string, 0, len(d.generators))
	for f := range d.generators {
		generators = append(generators, f)
	}

	// Collect windows
	windows := make([]string, 0, len(d.windows))
	for f := range d.windows {
		windows = append(windows, f)
	}

	// Collect table functions
	tableFunctions := make([]string, 0, len(d.tableFunctions))
	for f := range d.tableFunctions {
		tableFunctions = append(tableFunctions, f)
	}

	// Collect keywords
	keywords := make([]string, 0, len(d.keywords))
	for kw := range d.keywords {
		keywords = append(keywords, kw)
	}

	return &core.DialectConfig{
		Name:           d.Name,
		Identifiers:    d.Identifiers,
		DefaultSchema:  d.DefaultSchema,
		Placeholder:    d.Placeholder,
		Aggregates:     aggregates,
		Generators:     generators,
		Windows:        windows,
		TableFunctions: tableFunctions,
		Keywords:       keywords,
		DataTypes:      d.dataTypes,
	}
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
	case core.NormUppercase:
		return strings.ToUpper(name)
	case core.NormLowercase, core.NormCaseInsensitive:
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

// GetName returns the dialect name.
// This method allows Dialect to satisfy interfaces that require Name() string.
func (d *Dialect) GetName() string {
	return d.Name
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
	case core.PlaceholderDollar:
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
	return d.clauseSequence
}

// ClauseHandler returns the handler for a clause token type.
func (d *Dialect) ClauseHandler(t token.TokenType) spi.ClauseHandler {
	if def, ok := d.clauseDefs[t]; ok {
		return def.Handler
	}
	return nil
}

// ClauseDef returns the definition (handler + slot) for a clause token type.
func (d *Dialect) ClauseDef(t token.TokenType) (ClauseDef, bool) {
	if def, ok := d.clauseDefs[t]; ok {
		return def, true
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
	tokens := make([]token.TokenType, 0, len(d.clauseDefs))
	for t := range d.clauseDefs {
		tokens = append(tokens, t)
	}
	return tokens
}

// Symbols returns the custom operators map for lexer symbol matching.
func (d *Dialect) Symbols() map[string]token.TokenType {
	return d.symbols
}

// LookupKeyword returns the token type for a dynamic keyword.
// Returns the token type and true if found, or IDENT and false if not.
func (d *Dialect) LookupKeyword(name string) (token.TokenType, bool) {
	lowerName := strings.ToLower(name)
	if t, ok := d.dynamicKw[lowerName]; ok {
		return t, true
	}
	return token.IDENT, false
}

// Precedence returns the precedence level for an operator token.
// Returns 0 (PrecedenceNone) if the operator is not recognized.
func (d *Dialect) Precedence(t token.TokenType) int {
	if p, ok := d.precedence[t]; ok {
		return p
	}
	return spi.PrecedenceNone
}

// InfixHandler returns the custom infix handler for an operator token.
func (d *Dialect) InfixHandler(t token.TokenType) spi.InfixHandler {
	if h, ok := d.infixHandlers[t]; ok {
		return h
	}
	return nil
}

// PrefixHandler returns the custom prefix handler for an operator token.
func (d *Dialect) PrefixHandler(t token.TokenType) spi.PrefixHandler {
	if h, ok := d.prefixHandlers[t]; ok {
		return h
	}
	return nil
}

// JoinTypeDef returns the definition for a dialect-specific join type.
func (d *Dialect) JoinTypeDef(t token.TokenType) (JoinTypeDef, bool) {
	if def, ok := d.joinTypes[t]; ok {
		return def, true
	}
	return JoinTypeDef{}, false
}

// IsJoinTypeToken returns true if the token is a dialect-specific join type.
func (d *Dialect) IsJoinTypeToken(t token.TokenType) bool {
	_, ok := d.JoinTypeDef(t)
	return ok
}

// AllJoinTypeTokens returns all join type tokens registered in this dialect.
func (d *Dialect) AllJoinTypeTokens() []token.TokenType {
	tokens := make([]token.TokenType, 0, len(d.joinTypes))
	for t := range d.joinTypes {
		tokens = append(tokens, t)
	}
	return tokens
}

// StarModifierHandler returns the handler for a star modifier token type.
func (d *Dialect) StarModifierHandler(t token.TokenType) spi.StarModifierHandler {
	if h, ok := d.starModifiers[t]; ok {
		return h
	}
	return nil
}

// IsStarModifierToken returns true if the token is a star modifier.
func (d *Dialect) IsStarModifierToken(t token.TokenType) bool {
	return d.StarModifierHandler(t) != nil
}

// FromItemHandler returns the handler for a FROM item token type.
func (d *Dialect) FromItemHandler(t token.TokenType) spi.FromItemHandler {
	if h, ok := d.fromItems[t]; ok {
		return h
	}
	return nil
}

// IsFromItemToken returns true if the token is a FROM item extension (e.g., PIVOT, UNPIVOT).
func (d *Dialect) IsFromItemToken(t token.TokenType) bool {
	return d.FromItemHandler(t) != nil
}

// Builder provides a fluent API for constructing dialects.
type Builder struct {
	dialect *Dialect
	config  *core.DialectConfig // Optional config for auto-wiring features
}

// NewDialect creates a new dialect builder with the given name.
func NewDialect(name string) *Builder {
	return &Builder{
		dialect: &Dialect{
			Name: name,
			Identifiers: core.IdentifierConfig{
				Quote:         `"`,
				QuoteEnd:      `"`,
				Escape:        `""`,
				Normalization: core.NormLowercase,
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
			prefixHandlers: make(map[token.TokenType]spi.PrefixHandler),
			joinTypes:      make(map[token.TokenType]JoinTypeDef),
			starModifiers:  make(map[token.TokenType]spi.StarModifierHandler),
			fromItems:      make(map[token.TokenType]spi.FromItemHandler),
		},
	}
}

// New creates a dialect builder from a DialectConfig.
// The builder will auto-wire features based on config flags when Build() is called.
// This is the preferred constructor for dialects that use feature flags.
func New(cfg *core.DialectConfig) *Builder {
	b := &Builder{
		config: cfg,
		dialect: &Dialect{
			Name:          cfg.Name,
			Identifiers:   cfg.Identifiers,
			DefaultSchema: cfg.DefaultSchema,
			Placeholder:   cfg.Placeholder,
			// Initialize all maps
			aggregates:     make(map[string]struct{}),
			generators:     make(map[string]struct{}),
			windows:        make(map[string]struct{}),
			tableFunctions: make(map[string]struct{}),
			docs:           make(map[string]FunctionDoc),
			keywords:       make(map[string]struct{}),
			reservedWords:  make(map[string]struct{}),
			dataTypes:      nil,
			clauseSequence: nil,
			clauseDefs:     make(map[token.TokenType]ClauseDef),
			symbols:        make(map[string]token.TokenType),
			dynamicKw:      make(map[string]token.TokenType),
			precedence:     make(map[token.TokenType]int),
			infixHandlers:  make(map[token.TokenType]spi.InfixHandler),
			prefixHandlers: make(map[token.TokenType]spi.PrefixHandler),
			joinTypes:      make(map[token.TokenType]JoinTypeDef),
			starModifiers:  make(map[token.TokenType]spi.StarModifierHandler),
			fromItems:      make(map[token.TokenType]spi.FromItemHandler),
		},
	}
	return b
}

// Identifiers configures identifier quoting and normalization.
func (b *Builder) Identifiers(quote, quoteEnd, escape string, norm core.NormalizationStrategy) *Builder {
	b.dialect.Identifiers = core.IdentifierConfig{
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
func (b *Builder) PlaceholderStyle(style core.PlaceholderStyle) *Builder {
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
// If the builder was created with New(cfg), this auto-wires features based on config flags.
func (b *Builder) Build() *Dialect {
	cfg := b.config
	if cfg == nil {
		return b.dialect
	}

	// ===== Auto-wire function classifications from config =====
	for _, f := range cfg.Aggregates {
		b.dialect.aggregates[b.dialect.NormalizeName(f)] = struct{}{}
	}
	for _, f := range cfg.Generators {
		b.dialect.generators[b.dialect.NormalizeName(f)] = struct{}{}
	}
	for _, f := range cfg.Windows {
		b.dialect.windows[b.dialect.NormalizeName(f)] = struct{}{}
	}
	for _, f := range cfg.TableFunctions {
		b.dialect.tableFunctions[b.dialect.NormalizeName(f)] = struct{}{}
	}
	for _, kw := range cfg.Keywords {
		b.dialect.keywords[b.dialect.NormalizeName(kw)] = struct{}{}
	}
	b.dialect.dataTypes = append(b.dialect.dataTypes, cfg.DataTypes...)

	// ===== Auto-wire clause extensions =====

	// GROUP BY - use standard or ALL-aware handler
	// Replace existing handler if config specifies ALL support
	if cfg.SupportsGroupByAll {
		b.replaceOrAddClause(token.GROUP, GroupBy(GroupByOpts{AllowAll: true}))
	} else {
		b.addClauseIfMissing(token.GROUP, StandardGroupBy)
	}

	// ORDER BY - use standard or ALL-aware handler
	// Replace existing handler if config specifies ALL support
	if cfg.SupportsOrderByAll {
		b.replaceOrAddClause(token.ORDER, OrderBy(OrderByOpts{AllowAll: true}))
	} else {
		b.addClauseIfMissing(token.ORDER, StandardOrderBy)
	}

	// QUALIFY
	if cfg.SupportsQualify {
		b.AddKeyword("QUALIFY", token.QUALIFY)
		b.addClauseIfMissing(token.QUALIFY, StandardQualify)
	}

	// ===== Auto-wire operator extensions =====

	if cfg.SupportsIlike {
		b.AddKeyword("ILIKE", token.ILIKE)
		b.dialect.precedence[token.ILIKE] = spi.PrecedenceComparison
	}

	if cfg.SupportsCastOperator {
		b.AddOperator("::", token.DCOLON)
		b.dialect.precedence[token.DCOLON] = spi.PrecedencePostfix
	}

	// ===== Auto-wire join extensions =====

	if cfg.SupportsSemiAntiJoins {
		b.AddKeyword("SEMI", token.SEMI)
		b.AddKeyword("ANTI", token.ANTI)
		b.AddJoinType(token.SEMI, JoinTypeDef{
			Token:       token.SEMI,
			Type:        "SEMI",
			RequiresOn:  true,
			AllowsUsing: true,
		})
		b.AddJoinType(token.ANTI, JoinTypeDef{
			Token:       token.ANTI,
			Type:        "ANTI",
			RequiresOn:  true,
			AllowsUsing: true,
		})
	}

	return b.dialect
}

// replaceOrAddClause replaces an existing clause handler or adds a new one.
// Use this when a config flag requires a specific handler variant.
func (b *Builder) replaceOrAddClause(t token.TokenType, def ClauseDef) {
	def.Token = t
	b.dialect.clauseDefs[t] = def
	recordClause(t, t.String())
	// Ensure the token is in the sequence
	found := false
	for _, tok := range b.dialect.clauseSequence {
		if tok == t {
			found = true
			break
		}
	}
	if !found {
		b.dialect.clauseSequence = append(b.dialect.clauseSequence, t)
	}
}

// addClauseIfMissing adds a clause only if not already registered.
func (b *Builder) addClauseIfMissing(t token.TokenType, def ClauseDef) {
	if _, exists := b.dialect.clauseDefs[t]; !exists {
		def.Token = t
		b.dialect.clauseDefs[t] = def
		recordClause(t, t.String())
		// Add to sequence if not present
		found := false
		for _, tok := range b.dialect.clauseSequence {
			if tok == t {
				found = true
				break
			}
		}
		if !found {
			b.dialect.clauseSequence = append(b.dialect.clauseSequence, t)
		}
	}
}

// ---------- Parsing Behavior Builder Methods ----------

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

// AddPrefix registers a prefix expression handler (e.g., [ for list literals, { for struct literals).
func (b *Builder) AddPrefix(t token.TokenType, handler spi.PrefixHandler) *Builder {
	b.dialect.prefixHandlers[t] = handler
	return b
}

// AddJoinType registers a dialect-specific join type.
func (b *Builder) AddJoinType(t token.TokenType, def JoinTypeDef) *Builder {
	b.dialect.joinTypes[t] = def
	return b
}

// AddStarModifier registers a star expression modifier handler (e.g., EXCLUDE, REPLACE, RENAME).
func (b *Builder) AddStarModifier(t token.TokenType, handler spi.StarModifierHandler) *Builder {
	b.dialect.starModifiers[t] = handler
	return b
}

// AddFromItem registers a FROM clause item handler (e.g., PIVOT, UNPIVOT).
func (b *Builder) AddFromItem(t token.TokenType, handler spi.FromItemHandler) *Builder {
	b.dialect.fromItems[t] = handler
	return b
}

// ---------- Bulk Builder Methods (Toolbox Composition) ----------

// Clauses sets the clause sequence from a list of ClauseDefs.
// This replaces inheritance - explicitly list all supported clauses.
func (b *Builder) Clauses(defs ...ClauseDef) *Builder {
	b.dialect.clauseSequence = make([]token.TokenType, len(defs))
	for i, def := range defs {
		b.dialect.clauseSequence[i] = def.Token
		b.dialect.clauseDefs[def.Token] = def
		recordClause(def.Token, def.Token.String())
	}
	return b
}

// Operators adds operator definitions in bulk.
// If Symbol is provided, it's registered with the lexer.
func (b *Builder) Operators(sets ...[]OperatorDef) *Builder {
	for _, set := range sets {
		for _, op := range set {
			b.dialect.precedence[op.Token] = op.Precedence
			if op.Handler != nil {
				b.dialect.infixHandlers[op.Token] = op.Handler
			}
			if op.Symbol != "" {
				b.dialect.symbols[op.Symbol] = op.Token
			}
		}
	}
	return b
}

// JoinTypes adds join type definitions in bulk.
func (b *Builder) JoinTypes(sets ...[]JoinTypeDef) *Builder {
	for _, set := range sets {
		for _, jt := range set {
			b.dialect.joinTypes[jt.Token] = jt
		}
	}
	return b
}
