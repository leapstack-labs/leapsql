// Package sql provides a general-purpose SQL parsing toolkit.
//
// This package contains a complete SQL lexer, parser, and AST representation
// that can be used for parsing SQL statements across different database dialects.
// It is designed as a public API for external consumers who need SQL parsing
// capabilities without the domain-specific lineage extraction logic.
//
// # Features
//
//   - Lexer: Tokenizes SQL input into a stream of tokens
//   - Parser: Builds an Abstract Syntax Tree (AST) from tokens
//   - Dialect Support: Pluggable dialect system (e.g., DuckDB)
//   - Catalog: Built-in function catalog with search capabilities
//   - Resolver: Scope-based identifier resolution
//
// # Basic Usage
//
//	stmt, err := sql.Parse("SELECT id, name FROM users WHERE active = true")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	// Work with the AST...
//
// # Dialect Configuration
//
//	dialect := sql.GetDialect("duckdb")
//	// Use dialect-specific parsing options...
//
// # Function Catalog
//
//	functions := sql.SearchFunctions("DATE")
//	for _, fn := range functions {
//	    fmt.Printf("%s: %s\n", fn.Name, fn.Description)
//	}
package sql
