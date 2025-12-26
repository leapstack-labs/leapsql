// Package postgres provides the PostgreSQL SQL dialect definition.
// This package is pure Go with no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package postgres

import (
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

func init() {
	dialect.Register(Postgres)
}

// postgresReservedWords contains common PostgreSQL reserved words.
// This is a manually maintained list of frequently problematic identifiers.
// For a complete list, use pg_get_keywords() at runtime.
var postgresReservedWords = []string{
	"user", "order", "group", "table", "select", "from", "where", "index",
	"all", "and", "any", "array", "as", "asc", "asymmetric", "authorization",
	"between", "binary", "both", "case", "cast", "check", "collate", "column",
	"constraint", "create", "cross", "current_catalog", "current_date",
	"current_role", "current_schema", "current_time", "current_timestamp",
	"current_user", "default", "deferrable", "desc", "distinct", "do", "else",
	"end", "except", "false", "fetch", "for", "foreign", "freeze", "full",
	"grant", "having", "ilike", "in", "initially", "inner", "intersect",
	"into", "is", "isnull", "join", "lateral", "leading", "left", "like",
	"limit", "localtime", "localtimestamp", "natural", "not", "notnull",
	"null", "offset", "on", "only", "or", "outer", "overlaps", "placing",
	"primary", "references", "returning", "right", "session_user", "similar",
	"some", "symmetric", "then", "to", "trailing", "true", "union", "unique",
	"using", "variadic", "verbose", "when", "window", "with",
}

// Postgres is the PostgreSQL dialect.
// Builder reads Config flags and auto-wires standard features:
// - ILIKE operator (SupportsIlike)
// - :: cast operator (SupportsCastOperator)
// - RETURNING clause (SupportsReturning)
var Postgres = dialect.New(Config).
	// Clause Sequence - standard ANSI clauses (no QUALIFY)
	Clauses(dialect.StandardSelectClauses...).
	// Operators - standard ANSI operators (ILIKE and DCOLON are auto-wired)
	Operators(dialect.ANSIOperators).
	// Join Types - standard ANSI only (no SEMI/ANTI)
	JoinTypes(dialect.ANSIJoinTypes).
	// Reserved words
	WithReservedWords(postgresReservedWords...).
	Build()
