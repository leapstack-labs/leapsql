// Package main provides a generator that extracts DuckDB function metadata
// and generates Go code for the dialect package.
//
// Usage:
//
//	go run ./scripts/gendialect -dialect=duckdb -out=pkg/adapters/duckdb/dialect/dialect_gen.go
package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	dialectFlag = flag.String("dialect", "duckdb", "dialect to generate (only 'duckdb' supported)")
	outFlag     = flag.String("out", "", "output file path (required)")
)

// FunctionInfo holds metadata about a SQL function.
type FunctionInfo struct {
	Name           string
	FunctionType   string // scalar, aggregate, table, macro
	Parameters     []string
	ParameterTypes []string
	ReturnType     string
	Description    string
}

func main() {
	flag.Parse()

	if *outFlag == "" {
		log.Fatal("--out flag is required")
	}

	if *dialectFlag != "duckdb" {
		log.Fatalf("unsupported dialect: %s (only 'duckdb' is supported)", *dialectFlag)
	}

	// Connect to DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}

	ctx := context.Background()

	// Get DuckDB version
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		_ = db.Close()
		log.Fatalf("failed to get version: %v", err)
	}
	log.Printf("Connected to DuckDB %s", version)

	// Extract functions
	functions, err := extractFunctions(ctx, db)
	if err != nil {
		_ = db.Close()
		log.Fatalf("failed to extract functions: %v", err)
	}
	log.Printf("Extracted %d functions", len(functions))

	// Extract keywords
	keywords, err := extractKeywords(ctx, db)
	if err != nil {
		_ = db.Close()
		log.Fatalf("failed to extract keywords: %v", err)
	}
	log.Printf("Extracted %d keywords", len(keywords))

	// Extract data types
	dataTypes, err := extractDataTypes(ctx, db)
	if err != nil {
		_ = db.Close()
		log.Fatalf("failed to extract data types: %v", err)
	}
	log.Printf("Extracted %d data types", len(dataTypes))

	// Close db now that we're done with it
	if err := db.Close(); err != nil {
		log.Printf("warning: failed to close db: %v", err)
	}

	// Classify functions
	aggregates, tableFuncs, docs := classifyFunctions(functions)
	log.Printf("Classification: %d aggregates, %d table functions", len(aggregates), len(tableFuncs))

	// Generate code
	code := generateCode(version, aggregates, tableFuncs, docs, keywords, dataTypes)

	// Format the code
	formatted, err := format.Source([]byte(code))
	if err != nil {
		log.Printf("Warning: failed to format generated code: %v", err)
		formatted = []byte(code)
	}

	// Write output
	if err := os.WriteFile(*outFlag, formatted, 0o600); err != nil {
		log.Fatalf("failed to write output: %v", err)
	}

	log.Printf("Generated %s", *outFlag)
}

func extractFunctions(ctx context.Context, db *sql.DB) ([]FunctionInfo, error) {
	// Use list_to_string to convert arrays to comma-separated strings
	query := `
		SELECT 
			function_name,
			function_type,
			COALESCE(list_transform(parameters, x -> COALESCE(x, ''))::VARCHAR, ''),
			COALESCE(list_transform(parameter_types, x -> COALESCE(x, ''))::VARCHAR, ''),
			COALESCE(return_type, ''),
			COALESCE(description, '')
		FROM duckdb_functions()
		WHERE schema_name = 'main' OR schema_name IS NULL
		ORDER BY function_name, function_type
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query functions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var functions []FunctionInfo
	seen := make(map[string]bool) // Dedupe by name+type

	for rows.Next() {
		var fi FunctionInfo
		var params, paramTypes, returnType, desc string

		if err := rows.Scan(&fi.Name, &fi.FunctionType, &params, &paramTypes, &returnType, &desc); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		// Dedupe - keep first occurrence of each name+type combination
		key := fi.Name + "|" + fi.FunctionType
		if seen[key] {
			continue
		}
		seen[key] = true

		fi.Parameters = parseArrayString(params)
		fi.ParameterTypes = parseArrayString(paramTypes)
		fi.ReturnType = returnType
		fi.Description = desc

		functions = append(functions, fi)
	}

	return functions, rows.Err()
}

func extractKeywords(ctx context.Context, db *sql.DB) ([]string, error) {
	query := `SELECT keyword_name FROM duckdb_keywords() ORDER BY keyword_name`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query keywords: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var keywords []string
	for rows.Next() {
		var kw string
		if err := rows.Scan(&kw); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		keywords = append(keywords, kw)
	}

	return keywords, rows.Err()
}

func extractDataTypes(ctx context.Context, db *sql.DB) ([]string, error) {
	query := `
		SELECT DISTINCT type_name 
		FROM duckdb_types() 
		WHERE type_category NOT IN ('INVALID')
		ORDER BY type_name
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query types: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var types []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		types = append(types, t)
	}

	return types, rows.Err()
}

// parseArrayString parses DuckDB array string format like "[a, b, c]" or "a, b, c"
func parseArrayString(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")

	if s == "" {
		return nil
	}

	parts := strings.Split(s, ", ")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func classifyFunctions(functions []FunctionInfo) (aggregates, tableFuncs []string, docs map[string]FunctionDoc) {
	docs = make(map[string]FunctionDoc)
	aggregateSet := make(map[string]bool)
	tableSet := make(map[string]bool)

	for _, fi := range functions {
		name := strings.ToLower(fi.Name)

		// Build signature
		sig := buildSignature(fi)

		// Update docs (merge signatures for overloaded functions)
		if existing, ok := docs[name]; ok {
			// Check if this signature is already present
			hasSig := false
			for _, s := range existing.Signatures {
				if s == sig {
					hasSig = true
					break
				}
			}
			if !hasSig {
				existing.Signatures = append(existing.Signatures, sig)
				docs[name] = existing
			}
		} else {
			docs[name] = FunctionDoc{
				Description: fi.Description,
				Signatures:  []string{sig},
				ReturnType:  strings.ToUpper(fi.ReturnType),
			}
		}

		// Classify by function type
		switch fi.FunctionType {
		case "aggregate":
			aggregateSet[name] = true
		case "table":
			tableSet[name] = true
		}
	}

	// Convert sets to sorted slices
	aggregates = mapToSortedSlice(aggregateSet)
	tableFuncs = mapToSortedSlice(tableSet)

	return
}

// FunctionDoc matches the dialect.FunctionDoc struct for code generation.
type FunctionDoc struct {
	Description string
	Signatures  []string
	ReturnType  string
}

func buildSignature(fi FunctionInfo) string {
	var buf bytes.Buffer

	buf.WriteString(strings.ToLower(fi.Name))
	buf.WriteByte('(')

	for i, param := range fi.Parameters {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(param)
		if i < len(fi.ParameterTypes) {
			buf.WriteByte(' ')
			buf.WriteString(strings.ToUpper(fi.ParameterTypes[i]))
		}
	}

	buf.WriteString(") -> ")
	buf.WriteString(strings.ToUpper(fi.ReturnType))

	return buf.String()
}

func mapToSortedSlice(m map[string]bool) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

func generateCode(version string, aggregates, tableFuncs []string, docs map[string]FunctionDoc, keywords, dataTypes []string) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gendialect. DO NOT EDIT.\n")
	buf.WriteString(fmt.Sprintf("// Source: DuckDB %s\n", version))
	buf.WriteString(fmt.Sprintf("// Generated: %s\n\n", time.Now().Format("2006-01-02")))
	buf.WriteString("package dialect\n\n")
	buf.WriteString("import \"github.com/leapstack-labs/leapsql/pkg/dialect\"\n\n")

	// Generate docs map
	buf.WriteString("// duckDBFunctionDocs contains documentation for all DuckDB functions.\n")
	buf.WriteString("var duckDBFunctionDocs = map[string]dialect.FunctionDoc{\n")

	// Sort docs by name for consistent output
	docNames := make([]string, 0, len(docs))
	for name := range docs {
		docNames = append(docNames, name)
	}
	sort.Strings(docNames)

	for _, name := range docNames {
		doc := docs[name]
		buf.WriteString(fmt.Sprintf("\t%q: {\n", name))

		if doc.Description != "" {
			buf.WriteString(fmt.Sprintf("\t\tDescription: %q,\n", escapeString(doc.Description)))
		}

		if len(doc.Signatures) > 0 {
			buf.WriteString("\t\tSignatures: []string{\n")
			for _, sig := range doc.Signatures {
				buf.WriteString(fmt.Sprintf("\t\t\t%q,\n", sig))
			}
			buf.WriteString("\t\t},\n")
		}

		if doc.ReturnType != "" {
			buf.WriteString(fmt.Sprintf("\t\tReturnType: %q,\n", doc.ReturnType))
		}

		buf.WriteString("\t},\n")
	}
	buf.WriteString("}\n\n")

	// Generate aggregates slice
	buf.WriteString("// duckDBAggregates contains all aggregate function names.\n")
	buf.WriteString("var duckDBAggregates = []string{\n")
	writeStringSlice(&buf, aggregates)
	buf.WriteString("}\n\n")

	// Generate table functions slice
	buf.WriteString("// duckDBTableFunctions contains all table-valued function names.\n")
	buf.WriteString("var duckDBTableFunctions = []string{\n")
	writeStringSlice(&buf, tableFuncs)
	buf.WriteString("}\n\n")

	// Generate keywords slice
	buf.WriteString("// duckDBKeywords contains all reserved keywords.\n")
	buf.WriteString("var duckDBKeywords = []string{\n")
	writeStringSlice(&buf, keywords)
	buf.WriteString("}\n\n")

	// Generate data types slice
	buf.WriteString("// duckDBDataTypes contains all supported data types.\n")
	buf.WriteString("var duckDBDataTypes = []string{\n")
	writeStringSlice(&buf, dataTypes)
	buf.WriteString("}\n")

	return buf.String()
}

func writeStringSlice(buf *bytes.Buffer, items []string) {
	const itemsPerLine = 5
	for i, item := range items {
		if i%itemsPerLine == 0 {
			buf.WriteString("\t")
		}
		fmt.Fprintf(buf, "%q, ", item)
		if (i+1)%itemsPerLine == 0 {
			buf.WriteString("\n")
		}
	}
	if len(items)%itemsPerLine != 0 {
		buf.WriteString("\n")
	}
}

func escapeString(s string) string {
	// Escape special characters in description strings
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\t", "\\t")
	return s
}
