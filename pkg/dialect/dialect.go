// Package dialect provides SQL dialect configuration and function classification.
//
// This package contains the public contract for dialect definitions used by the parser,
// lineage analyzer, and other SQL-aware components. Concrete dialect implementations
// are registered from pkg/adapters/*/dialect packages.
package dialect

import (
	"strings"
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

// IdentifierConfig defines how identifiers are quoted and normalized.
type IdentifierConfig struct {
	Quote         string                // Quote character: ", `, [
	QuoteEnd      string                // End quote character (usually same as Quote, ] for [)
	Escape        string                // Escape sequence: "", ``, ]]
	Normalization NormalizationStrategy // How to normalize unquoted identifiers
}

// OperatorConfig defines operator behaviors that vary by dialect.
type OperatorConfig struct {
	DPipeIsConcat  bool // || means string concatenation (default: true, false in MySQL)
	ConcatCoalesce bool // CONCAT treats NULL as empty string (default: false)
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
)

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
	default:
		return "unknown"
	}
}

// Dialect represents a SQL dialect configuration.
type Dialect struct {
	Name        string
	Identifiers IdentifierConfig
	Operators   OperatorConfig

	// Function classifications (normalized to dialect's normalization strategy)
	aggregates map[string]struct{}
	generators map[string]struct{}
	windows    map[string]struct{}
	aliases    map[string]string // alias -> canonical name
}

// FunctionLineageType returns the lineage classification for a function.
func (d *Dialect) FunctionLineageType(name string) Type {
	normalized := d.NormalizeName(name)

	// Check for alias first
	if canonical, ok := d.aliases[normalized]; ok {
		normalized = canonical
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

// CanonicalFunctionName returns the canonical name for a function (resolving aliases).
func (d *Dialect) CanonicalFunctionName(name string) string {
	normalized := d.NormalizeName(name)
	if canonical, ok := d.aliases[normalized]; ok {
		return canonical
	}
	return normalized
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
			Operators: OperatorConfig{
				DPipeIsConcat:  true,
				ConcatCoalesce: false,
			},
			aggregates: make(map[string]struct{}),
			generators: make(map[string]struct{}),
			windows:    make(map[string]struct{}),
			aliases:    make(map[string]string),
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

// Operators configures operator behaviors.
func (b *Builder) Operators(dpipeIsConcat, concatCoalesce bool) *Builder {
	b.dialect.Operators = OperatorConfig{
		DPipeIsConcat:  dpipeIsConcat,
		ConcatCoalesce: concatCoalesce,
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

// Aliases adds function aliases (alias -> canonical name).
func (b *Builder) Aliases(aliases map[string]string) *Builder {
	for k, v := range aliases {
		b.dialect.aliases[b.dialect.NormalizeName(k)] = b.dialect.NormalizeName(v)
	}
	return b
}

// Build returns the constructed dialect.
func (b *Builder) Build() *Dialect {
	return b.dialect
}
