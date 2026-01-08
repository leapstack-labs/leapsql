// Package main provides a generator that extracts DuckDB function metadata
// and generates Go code for the dialect package.
//
// Usage:
//
//	go run ./scripts/gendialect -dialect=duckdb -out=pkg/dialects/duckdb/functions_gen.go
//	go run ./scripts/gendialect -dialect=duckdb -gen=keywords -out=pkg/dialects/duckdb/keywords_gen.go
//	go run ./scripts/gendialect -dialect=duckdb -gen=types -out=pkg/dialects/duckdb/types_gen.go
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
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	dialectFlag = flag.String("dialect", "duckdb", "dialect to generate (only 'duckdb' supported)")
	genFlag     = flag.String("gen", "functions", "what to generate: functions, keywords, types, all")
	outFlag     = flag.String("out", "", "output file path (required for single generation)")
	outDirFlag  = flag.String("outdir", "", "output directory (for 'all' generation)")
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

	if *dialectFlag != "duckdb" {
		log.Fatalf("unsupported dialect: %s (only 'duckdb' is supported)", *dialectFlag)
	}

	// Validate flags
	if *genFlag == "all" {
		if *outDirFlag == "" {
			log.Fatal("--outdir flag is required when using -gen=all")
		}
	} else {
		if *outFlag == "" {
			log.Fatal("--out flag is required")
		}
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

	// Validate gen flag early (before defer)
	validGenFlags := map[string]bool{"functions": true, "keywords": true, "types": true, "all": true}
	if !validGenFlags[*genFlag] {
		_ = db.Close()
		log.Fatalf("unknown -gen value: %s (use: functions, keywords, types, all)", *genFlag)
	}

	// Ensure db is closed at the end
	defer func() { _ = db.Close() }()

	switch *genFlag {
	case "functions":
		generateFunctionsFile(ctx, db, version, *outFlag)
	case "keywords":
		generateKeywordsFile(ctx, db, version, *outFlag)
	case "types":
		generateTypesFile(ctx, db, version, *outFlag)
	case "all":
		generateFunctionsFile(ctx, db, version, *outDirFlag+"/functions_gen.go")
		generateKeywordsFile(ctx, db, version, *outDirFlag+"/keywords_gen.go")
		generateTypesFile(ctx, db, version, *outDirFlag+"/types_gen.go")
	}
}

func generateFunctionsFile(ctx context.Context, db *sql.DB, version, outPath string) {
	// Extract functions
	functions, err := extractFunctions(ctx, db)
	if err != nil {
		log.Fatalf("failed to extract functions: %v", err)
	}
	log.Printf("Extracted %d functions", len(functions))

	// Filter out operators and internal functions
	functions = filterFunctions(functions)
	log.Printf("After filtering: %d functions", len(functions))

	// Classify functions
	aggregates, tableFuncs, docs := classifyFunctions(functions)
	log.Printf("Classification: %d aggregates, %d table functions", len(aggregates), len(tableFuncs))

	// Generate code
	code := generateFunctionsCode(version, aggregates, tableFuncs, docs)
	writeFormattedCode(outPath, code)
}

func generateKeywordsFile(ctx context.Context, db *sql.DB, version, outPath string) {
	// Extract reserved keywords (for LSP completions)
	reservedKeywords, err := extractKeywords(ctx, db, true)
	if err != nil {
		log.Fatalf("failed to extract reserved keywords: %v", err)
	}
	log.Printf("Extracted %d reserved keywords", len(reservedKeywords))

	// Extract all keywords (for identifier quoting)
	allKeywords, err := extractKeywords(ctx, db, false)
	if err != nil {
		log.Fatalf("failed to extract all keywords: %v", err)
	}
	log.Printf("Extracted %d total keywords", len(allKeywords))

	// Generate code
	code := generateKeywordsCode(version, reservedKeywords, allKeywords)
	writeFormattedCode(outPath, code)
}

func generateTypesFile(ctx context.Context, db *sql.DB, version, outPath string) {
	// Extract data types
	types, err := extractTypes(ctx, db)
	if err != nil {
		log.Fatalf("failed to extract types: %v", err)
	}
	log.Printf("Extracted %d data types", len(types))

	// Generate code
	code := generateTypesCode(version, types)
	writeFormattedCode(outPath, code)
}

func writeFormattedCode(outPath, code string) {
	// Format the code
	formatted, err := format.Source([]byte(code))
	if err != nil {
		log.Printf("Warning: failed to format generated code: %v", err)
		formatted = []byte(code)
	}

	// Write output
	if err := os.WriteFile(outPath, formatted, 0o600); err != nil {
		log.Fatalf("failed to write output: %v", err)
	}

	log.Printf("Generated %s", outPath)
}

func extractKeywords(ctx context.Context, db *sql.DB, reservedOnly bool) ([]string, error) {
	query := "SELECT keyword_name FROM duckdb_keywords()"
	if reservedOnly {
		query += " WHERE keyword_category = 'reserved'"
	}
	query += " ORDER BY keyword_name"

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query keywords: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var keywords []string
	for rows.Next() {
		var kw string
		if err := rows.Scan(&kw); err != nil {
			return nil, fmt.Errorf("scan keyword: %w", err)
		}
		keywords = append(keywords, strings.ToUpper(kw))
	}

	return keywords, rows.Err()
}

func extractTypes(ctx context.Context, db *sql.DB) ([]string, error) {
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
			return nil, fmt.Errorf("scan type: %w", err)
		}
		types = append(types, strings.ToUpper(t))
	}

	return types, rows.Err()
}

func generateKeywordsCode(version string, reserved, all []string) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gendialect. DO NOT EDIT.\n")
	fmt.Fprintf(&buf, "// Source: DuckDB %s\n", version)
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package duckdb\n\n")

	// Generate reserved keywords (for LSP completions)
	buf.WriteString("// duckDBCompletionKeywords contains reserved keywords for LSP completions.\n")
	buf.WriteString("// Source: SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category = 'reserved'\n")
	buf.WriteString("var duckDBCompletionKeywords = []string{\n")
	writeStringSlice(&buf, reserved)
	buf.WriteString("}\n\n")

	// Generate all keywords (for identifier quoting)
	buf.WriteString("// duckDBAllKeywords contains all keywords that need quoting when used as identifiers.\n")
	buf.WriteString("// Source: SELECT keyword_name FROM duckdb_keywords()\n")
	buf.WriteString("var duckDBAllKeywords = []string{\n")
	writeStringSlice(&buf, all)
	buf.WriteString("}\n")

	return buf.String()
}

func generateTypesCode(version string, types []string) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gendialect. DO NOT EDIT.\n")
	fmt.Fprintf(&buf, "// Source: DuckDB %s\n", version)
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package duckdb\n\n")

	buf.WriteString("// duckDBTypes contains all supported data types for LSP completions.\n")
	buf.WriteString("// Source: SELECT DISTINCT type_name FROM duckdb_types()\n")
	buf.WriteString("var duckDBTypes = []string{\n")
	writeStringSlice(&buf, types)
	buf.WriteString("}\n")

	return buf.String()
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

// filterFunctions removes operators and internal functions that shouldn't be exposed.
func filterFunctions(functions []FunctionInfo) []FunctionInfo {
	// Pattern for symbolic operators (names consisting only of operator characters)
	symbolicOp := regexp.MustCompile(`^[!@#$%^&*+\-=<>|/~]+$`)

	result := make([]FunctionInfo, 0, len(functions))
	filtered := 0

	for _, fi := range functions {
		if shouldSkipFunction(fi.Name, symbolicOp) {
			filtered++
			continue
		}
		result = append(result, fi)
	}

	log.Printf("Filtered out %d operators/internal functions", filtered)
	return result
}

// shouldSkipFunction returns true if a function should be excluded from generation.
func shouldSkipFunction(name string, symbolicOp *regexp.Regexp) bool {
	// Skip internal functions (__internal_*)
	if strings.HasPrefix(name, "__internal_") {
		return true
	}

	// Skip postfix operator notation (e.g., !__postfix)
	if strings.HasSuffix(name, "__postfix") {
		return true
	}

	// Skip tilde operators (LIKE/ILIKE internal representations)
	if name == "~~" || name == "~~*" || name == "!~~" || name == "!~~*" || name == "~~~" {
		return true
	}

	// Skip symbolic operators (pure punctuation like +, -, *, /, etc.)
	if symbolicOp.MatchString(name) {
		return true
	}

	return false
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

// FunctionDoc matches the core.FunctionDoc struct for code generation.
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

func generateFunctionsCode(version string, aggregates, tableFuncs []string, docs map[string]FunctionDoc) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gendialect. DO NOT EDIT.\n")
	fmt.Fprintf(&buf, "// Source: DuckDB %s\n", version)
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package duckdb\n\n")
	buf.WriteString("import \"github.com/leapstack-labs/leapsql/pkg/dialect\"\n\n")

	// Generate docs map
	buf.WriteString("// duckDBFunctionDocs contains documentation for all DuckDB functions.\n")
	buf.WriteString("var duckDBFunctionDocs = map[string]core.FunctionDoc{\n")

	// Sort docs by name for consistent output
	docNames := make([]string, 0, len(docs))
	for name := range docs {
		docNames = append(docNames, name)
	}
	sort.Strings(docNames)

	for _, name := range docNames {
		doc := docs[name]
		fmt.Fprintf(&buf, "\t%q: {\n", name)

		if doc.Description != "" {
			fmt.Fprintf(&buf, "\t\tDescription: %q,\n", escapeString(doc.Description))
		}

		if len(doc.Signatures) > 0 {
			buf.WriteString("\t\tSignatures: []string{\n")
			for _, sig := range doc.Signatures {
				fmt.Fprintf(&buf, "\t\t\t%q,\n", sig)
			}
			buf.WriteString("\t\t},\n")
		}

		if doc.ReturnType != "" {
			fmt.Fprintf(&buf, "\t\tReturnType: %q,\n", doc.ReturnType)
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
