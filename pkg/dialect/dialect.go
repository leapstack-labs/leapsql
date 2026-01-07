// Package dialect provides SQL dialect configuration and function classification.
//
// This package contains the public contract for dialect definitions used by the parser,
// lineage analyzer, and other SQL-aware components. Concrete dialect implementations
// are registered from pkg/dialects/*/ packages.
//
// The Dialect struct is now defined in pkg/core. This package provides a Builder
// for type-safe construction and manages the global registry.
package dialect

import (
	"strconv"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Dialect is an alias to core.Dialect for backward compatibility.
// New code should use core.Dialect directly.
type Dialect = core.Dialect

// ClauseDef is an alias to core.ClauseDef for backward compatibility.
type ClauseDef = core.ClauseDef

// JoinTypeDef is an alias to core.JoinTypeDef for backward compatibility.
type JoinTypeDef = core.JoinTypeDef

// OperatorDef is an alias to core.OperatorDef for backward compatibility.
type OperatorDef = core.OperatorDef

// FunctionDoc is an alias to core.FunctionDoc for backward compatibility.
// FunctionDoc is an alias to core.FunctionDoc for backward compatibility.
type FunctionDoc = core.FunctionDoc

// ErrDialectRequired is returned when a dialect is required but not provided.
var ErrDialectRequired = core.ErrDialectRequired

// Re-export join constants
const (
	JoinInner = core.JoinInner
	JoinLeft  = core.JoinLeft
	JoinRight = core.JoinRight
	JoinFull  = core.JoinFull
	JoinCross = core.JoinCross
)

// Type classifies how a function affects lineage.
// This is kept for backward compatibility.
type Type = core.FunctionLineageType

// Lineage type constants classify function behavior for lineage analysis.
const (
	// LineagePassthrough indicates the function passes through columns unchanged.
	LineagePassthrough = core.LineagePassthrough
	// LineageAggregate indicates an aggregate function.
	LineageAggregate = core.LineageAggregate
	// LineageGenerator indicates a function that generates values without input columns.
	LineageGenerator = core.LineageGenerator
	// LineageWindow indicates a window function.
	LineageWindow = core.LineageWindow
	// LineageTable indicates a table-valued function.
	LineageTable = core.LineageTable
)

// ClauseOption configures a ClauseDef.
type ClauseOption func(*core.ClauseDef)

// WithInline marks a clause as inline (keyword and value on same line).
func WithInline() ClauseOption {
	return func(c *core.ClauseDef) {
		c.Inline = true
	}
}

// WithKeywords sets the display keywords for a clause.
func WithKeywords(keywords ...string) ClauseOption {
	return func(c *core.ClauseDef) {
		c.Keywords = keywords
	}
}

// Builder provides a fluent API for constructing dialects.
type Builder struct {
	dialect *core.Dialect
	config  *core.DialectConfig
}

// NewDialect creates a new dialect builder with the given name.
func NewDialect(name string) *Builder {
	return &Builder{
		dialect: &core.Dialect{
			Name: name,
			Identifiers: core.IdentifierConfig{
				Quote:         `"`,
				QuoteEnd:      `"`,
				Escape:        `""`,
				Normalization: core.NormLowercase,
			},
			Aggregates:     make(map[string]struct{}),
			Generators:     make(map[string]struct{}),
			Windows:        make(map[string]struct{}),
			TableFunctions: make(map[string]struct{}),
			Docs:           make(map[string]core.FunctionDoc),
			Keywords:       make(map[string]struct{}),
			ReservedWords:  make(map[string]struct{}),
			DataTypes:      nil,
			ClauseSeq:      nil,
			ClauseDefs:     make(map[token.TokenType]core.ClauseDef),
			Symbols:        make(map[string]token.TokenType),
			DynamicKw:      make(map[string]token.TokenType),
			Precedences:    make(map[token.TokenType]int),
			InfixHandlers:  make(map[token.TokenType]any),
			PrefixHandlers: make(map[token.TokenType]any),
			JoinTypes:      make(map[token.TokenType]core.JoinTypeDef),
			StarModifiers:  make(map[token.TokenType]any),
			FromItems:      make(map[token.TokenType]any),
		},
	}
}

// New creates a dialect builder from a DialectConfig.
// The builder will auto-wire features based on config flags when Build() is called.
func New(cfg *core.DialectConfig) *Builder {
	b := &Builder{
		config: cfg,
		dialect: &core.Dialect{
			Name:           cfg.Name,
			Identifiers:    cfg.Identifiers,
			DefaultSchema:  cfg.DefaultSchema,
			Placeholder:    cfg.Placeholder,
			Aggregates:     make(map[string]struct{}),
			Generators:     make(map[string]struct{}),
			Windows:        make(map[string]struct{}),
			TableFunctions: make(map[string]struct{}),
			Docs:           make(map[string]core.FunctionDoc),
			Keywords:       make(map[string]struct{}),
			ReservedWords:  make(map[string]struct{}),
			DataTypes:      nil,
			ClauseSeq:      nil,
			ClauseDefs:     make(map[token.TokenType]core.ClauseDef),
			Symbols:        make(map[string]token.TokenType),
			DynamicKw:      make(map[string]token.TokenType),
			Precedences:    make(map[token.TokenType]int),
			InfixHandlers:  make(map[token.TokenType]any),
			PrefixHandlers: make(map[token.TokenType]any),
			JoinTypes:      make(map[token.TokenType]core.JoinTypeDef),
			StarModifiers:  make(map[token.TokenType]any),
			FromItems:      make(map[token.TokenType]any),
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
		b.dialect.Aggregates[b.dialect.NormalizeName(f)] = struct{}{}
	}
	return b
}

// Generators adds generator functions (no input columns) to the dialect.
func (b *Builder) Generators(funcs ...string) *Builder {
	for _, f := range funcs {
		b.dialect.Generators[b.dialect.NormalizeName(f)] = struct{}{}
	}
	return b
}

// Windows adds window-only functions to the dialect.
func (b *Builder) Windows(funcs ...string) *Builder {
	for _, f := range funcs {
		b.dialect.Windows[b.dialect.NormalizeName(f)] = struct{}{}
	}
	return b
}

// TableFunctions adds table-valued functions to the dialect.
func (b *Builder) TableFunctions(funcs ...string) *Builder {
	for _, f := range funcs {
		b.dialect.TableFunctions[b.dialect.NormalizeName(f)] = struct{}{}
	}
	return b
}

// WithDocs registers documentation for functions.
func (b *Builder) WithDocs(docs map[string]FunctionDoc) *Builder {
	for name, doc := range docs {
		b.dialect.Docs[b.dialect.NormalizeName(name)] = doc
	}
	return b
}

// WithKeywords registers reserved keywords.
func (b *Builder) WithKeywords(kws ...string) *Builder {
	for _, kw := range kws {
		b.dialect.Keywords[b.dialect.NormalizeName(kw)] = struct{}{}
	}
	return b
}

// WithDataTypes registers supported data types.
func (b *Builder) WithDataTypes(types ...string) *Builder {
	b.dialect.DataTypes = append(b.dialect.DataTypes, types...)
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
	for _, w := range words {
		b.dialect.ReservedWords[b.dialect.NormalizeName(w)] = struct{}{}
	}
	return b
}

// ---------- Parsing Behavior Builder Methods ----------

// AddOperator registers a custom operator symbol for the lexer.
func (b *Builder) AddOperator(symbol string, t token.TokenType) *Builder {
	b.dialect.Symbols[symbol] = t
	return b
}

// AddKeyword registers a dynamic keyword for the lexer.
func (b *Builder) AddKeyword(name string, t token.TokenType) *Builder {
	b.dialect.DynamicKw[strings.ToLower(name)] = t
	return b
}

// ClauseSequence sets the full clause sequence (for base dialects).
func (b *Builder) ClauseSequence(tokens ...token.TokenType) *Builder {
	b.dialect.ClauseSeq = tokens
	return b
}

// ClauseHandler registers a handler for a clause token with storage slot.
func (b *Builder) ClauseHandler(t token.TokenType, handler spi.ClauseHandler, slot spi.ClauseSlot, opts ...ClauseOption) *Builder {
	def := core.ClauseDef{Token: t, Handler: handler, Slot: slot}
	for _, opt := range opts {
		opt(&def)
	}
	b.dialect.ClauseDefs[t] = def
	recordClause(t, t.String())
	return b
}

// AddClauseAfter inserts a clause into the sequence after another clause.
func (b *Builder) AddClauseAfter(after, t token.TokenType, handler spi.ClauseHandler, slot spi.ClauseSlot, opts ...ClauseOption) *Builder {
	for i, tok := range b.dialect.ClauseSeq {
		if tok == after {
			newSeq := make([]token.TokenType, 0, len(b.dialect.ClauseSeq)+1)
			newSeq = append(newSeq, b.dialect.ClauseSeq[:i+1]...)
			newSeq = append(newSeq, t)
			newSeq = append(newSeq, b.dialect.ClauseSeq[i+1:]...)
			b.dialect.ClauseSeq = newSeq
			break
		}
	}
	def := core.ClauseDef{Token: t, Handler: handler, Slot: slot}
	for _, opt := range opts {
		opt(&def)
	}
	b.dialect.ClauseDefs[t] = def
	recordClause(t, t.String())
	return b
}

// RemoveClause removes a clause from the sequence.
func (b *Builder) RemoveClause(t token.TokenType) *Builder {
	for i, tok := range b.dialect.ClauseSeq {
		if tok == t {
			b.dialect.ClauseSeq = append(b.dialect.ClauseSeq[:i], b.dialect.ClauseSeq[i+1:]...)
			break
		}
	}
	delete(b.dialect.ClauseDefs, t)
	return b
}

// AddInfix registers an infix operator with precedence.
func (b *Builder) AddInfix(t token.TokenType, precedence int) *Builder {
	b.dialect.Precedences[t] = precedence
	return b
}

// AddInfixWithHandler registers an infix operator with custom handler.
func (b *Builder) AddInfixWithHandler(t token.TokenType, precedence int, handler spi.InfixHandler) *Builder {
	b.dialect.Precedences[t] = precedence
	b.dialect.InfixHandlers[t] = handler
	return b
}

// AddPrefix registers a prefix expression handler.
func (b *Builder) AddPrefix(t token.TokenType, handler spi.PrefixHandler) *Builder {
	b.dialect.PrefixHandlers[t] = handler
	return b
}

// AddJoinType registers a dialect-specific join type.
func (b *Builder) AddJoinType(t token.TokenType, def JoinTypeDef) *Builder {
	b.dialect.JoinTypes[t] = def
	return b
}

// AddStarModifier registers a star expression modifier handler.
func (b *Builder) AddStarModifier(t token.TokenType, handler spi.StarModifierHandler) *Builder {
	b.dialect.StarModifiers[t] = handler
	return b
}

// AddFromItem registers a FROM clause item handler.
func (b *Builder) AddFromItem(t token.TokenType, handler spi.FromItemHandler) *Builder {
	b.dialect.FromItems[t] = handler
	return b
}

// ---------- Bulk Builder Methods (Toolbox Composition) ----------

// Clauses sets the clause sequence from a list of ClauseDefs.
func (b *Builder) Clauses(defs ...ClauseDef) *Builder {
	b.dialect.ClauseSeq = make([]token.TokenType, len(defs))
	for i, def := range defs {
		b.dialect.ClauseSeq[i] = def.Token
		b.dialect.ClauseDefs[def.Token] = def
		recordClause(def.Token, def.Token.String())
	}
	return b
}

// Operators adds operator definitions in bulk.
func (b *Builder) Operators(sets ...[]OperatorDef) *Builder {
	for _, set := range sets {
		for _, op := range set {
			b.dialect.Precedences[op.Token] = op.Precedence
			if op.Handler != nil {
				b.dialect.InfixHandlers[op.Token] = op.Handler
			}
			if op.Symbol != "" {
				b.dialect.Symbols[op.Symbol] = op.Token
			}
		}
	}
	return b
}

// JoinTypes adds join type definitions in bulk.
func (b *Builder) JoinTypes(sets ...[]JoinTypeDef) *Builder {
	for _, set := range sets {
		for _, jt := range set {
			b.dialect.JoinTypes[jt.Token] = jt
		}
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

	// Auto-wire function classifications from config
	for _, f := range cfg.Aggregates {
		b.dialect.Aggregates[b.dialect.NormalizeName(f)] = struct{}{}
	}
	for _, f := range cfg.Generators {
		b.dialect.Generators[b.dialect.NormalizeName(f)] = struct{}{}
	}
	for _, f := range cfg.Windows {
		b.dialect.Windows[b.dialect.NormalizeName(f)] = struct{}{}
	}
	for _, f := range cfg.TableFunctions {
		b.dialect.TableFunctions[b.dialect.NormalizeName(f)] = struct{}{}
	}
	for _, kw := range cfg.Keywords {
		b.dialect.Keywords[b.dialect.NormalizeName(kw)] = struct{}{}
	}
	b.dialect.DataTypes = append(b.dialect.DataTypes, cfg.DataTypes...)

	// Auto-wire clause extensions
	if cfg.SupportsGroupByAll {
		b.replaceOrAddClause(token.GROUP, GroupBy(GroupByOpts{AllowAll: true}))
	} else {
		b.addClauseIfMissing(token.GROUP, StandardGroupBy)
	}

	if cfg.SupportsOrderByAll {
		b.replaceOrAddClause(token.ORDER, OrderBy(OrderByOpts{AllowAll: true}))
	} else {
		b.addClauseIfMissing(token.ORDER, StandardOrderBy)
	}

	if cfg.SupportsQualify {
		b.AddKeyword("QUALIFY", token.QUALIFY)
		b.addClauseIfMissing(token.QUALIFY, StandardQualify)
	}

	// Auto-wire operator extensions
	if cfg.SupportsIlike {
		b.AddKeyword("ILIKE", token.ILIKE)
		b.dialect.Precedences[token.ILIKE] = spi.PrecedenceComparison
	}

	if cfg.SupportsCastOperator {
		b.AddOperator("::", token.DCOLON)
		b.dialect.Precedences[token.DCOLON] = spi.PrecedencePostfix
	}

	// Auto-wire join extensions
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
func (b *Builder) replaceOrAddClause(t token.TokenType, def ClauseDef) {
	def.Token = t
	b.dialect.ClauseDefs[t] = def
	recordClause(t, t.String())
	found := false
	for _, tok := range b.dialect.ClauseSeq {
		if tok == t {
			found = true
			break
		}
	}
	if !found {
		b.dialect.ClauseSeq = append(b.dialect.ClauseSeq, t)
	}
}

// addClauseIfMissing adds a clause only if not already registered.
func (b *Builder) addClauseIfMissing(t token.TokenType, def ClauseDef) {
	if _, exists := b.dialect.ClauseDefs[t]; !exists {
		def.Token = t
		b.dialect.ClauseDefs[t] = def
		recordClause(t, t.String())
		found := false
		for _, tok := range b.dialect.ClauseSeq {
			if tok == t {
				found = true
				break
			}
		}
		if !found {
			b.dialect.ClauseSeq = append(b.dialect.ClauseSeq, t)
		}
	}
}

// ---------- Helper Methods for Dialect ----------

// FormatPlaceholder returns a placeholder for the given parameter index (1-based).
func FormatPlaceholder(d *Dialect, index int) string {
	switch d.Placeholder {
	case core.PlaceholderDollar:
		return "$" + strconv.Itoa(index)
	default:
		return "?"
	}
}

// FunctionLineageType returns the lineage classification for a function.
func FunctionLineageType(d *Dialect, name string) Type {
	return d.FunctionLineageTypeOf(name)
}

// ClauseHandler returns the handler for a clause token type.
func ClauseHandler(d *Dialect, t token.TokenType) spi.ClauseHandler {
	if def, ok := d.ClauseDefs[t]; ok {
		if h, ok := def.Handler.(spi.ClauseHandler); ok {
			return h
		}
	}
	return nil
}

// InfixHandler returns the custom infix handler for an operator token.
func InfixHandler(d *Dialect, t token.TokenType) spi.InfixHandler {
	if h := d.InfixHandler(t); h != nil {
		if handler, ok := h.(spi.InfixHandler); ok {
			return handler
		}
	}
	return nil
}

// PrefixHandler returns the custom prefix handler for an operator token.
func PrefixHandler(d *Dialect, t token.TokenType) spi.PrefixHandler {
	if h := d.PrefixHandler(t); h != nil {
		if handler, ok := h.(spi.PrefixHandler); ok {
			return handler
		}
	}
	return nil
}

// StarModifierHandler returns the handler for a star modifier token type.
func StarModifierHandler(d *Dialect, t token.TokenType) spi.StarModifierHandler {
	if h := d.StarModifierHandler(t); h != nil {
		if handler, ok := h.(spi.StarModifierHandler); ok {
			return handler
		}
	}
	return nil
}

// FromItemHandler returns the handler for a FROM item token type.
func FromItemHandler(d *Dialect, t token.TokenType) spi.FromItemHandler {
	if h := d.FromItemHandler(t); h != nil {
		if handler, ok := h.(spi.FromItemHandler); ok {
			return handler
		}
	}
	return nil
}

// AllFunctions returns all known function names.
func AllFunctions(d *Dialect) []string {
	seen := make(map[string]struct{})
	var funcs []string

	for f := range d.Aggregates {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}
	for f := range d.Generators {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}
	for f := range d.Windows {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}
	for f := range d.TableFunctions {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}
	for f := range d.Docs {
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			funcs = append(funcs, f)
		}
	}
	return funcs
}

// Keywords returns all reserved keywords.
func Keywords(d *Dialect) []string {
	kws := make([]string, 0, len(d.Keywords))
	for kw := range d.Keywords {
		kws = append(kws, kw)
	}
	return kws
}

// AllClauseTokens returns all clause tokens registered in this dialect.
func AllClauseTokens(d *Dialect) []token.TokenType {
	tokens := make([]token.TokenType, 0, len(d.ClauseDefs))
	for t := range d.ClauseDefs {
		tokens = append(tokens, t)
	}
	return tokens
}

// AllJoinTypeTokens returns all join type tokens registered in this dialect.
func AllJoinTypeTokens(d *Dialect) []token.TokenType {
	tokens := make([]token.TokenType, 0, len(d.JoinTypes))
	for t := range d.JoinTypes {
		tokens = append(tokens, t)
	}
	return tokens
}
