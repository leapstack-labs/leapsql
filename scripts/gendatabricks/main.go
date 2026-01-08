// Package main provides a web scraper that extracts Databricks SQL function metadata
// from the Databricks documentation and generates Go code for the dialect package.
//
// Usage:
//
//	go run ./scripts/gendatabricks -gen=all -outdir=pkg/dialects/databricks/
//	go run ./scripts/gendatabricks -gen=functions -out=pkg/dialects/databricks/functions_gen.go
//	go run ./scripts/gendatabricks -gen=keywords -out=pkg/dialects/databricks/keywords_gen.go
//	go run ./scripts/gendatabricks -gen=types -out=pkg/dialects/databricks/types_gen.go
//
// The scraper fetches function names from the alphabetical list, then concurrently
// fetches documentation from each individual function page for full coverage.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

const (
	baseURL      = "https://docs.databricks.com/aws/en/sql/language-manual"
	functionsURL = baseURL + "/sql-ref-functions-builtin-alpha"
	keywordsURL  = baseURL + "/sql-ref-reserved-words"
	typesURL     = baseURL + "/sql-ref-datatypes"
)

var (
	genFlag     = flag.String("gen", "all", "what to generate: functions, keywords, types, all")
	outFlag     = flag.String("out", "", "output file path (required for single generation)")
	outDirFlag  = flag.String("outdir", "", "output directory (for 'all' generation)")
	workersFlag = flag.Int("workers", 20, "number of concurrent workers for fetching docs")
)

// FunctionType represents the classification of a SQL function.
type FunctionType int

const (
	FuncScalar FunctionType = iota
	FuncAggregate
	FuncWindow
	FuncTableValued
	FuncOperator // Skip these
)

// FunctionInfo holds metadata about a SQL function.
type FunctionInfo struct {
	Name     string
	Type     FunctionType
	Href     string // URL path to function documentation
	LinkText string // Original link text for debugging
}

// FunctionDoc contains scraped documentation for a function.
type FunctionDoc struct {
	Description string
	Syntax      string
}

func main() {
	flag.Parse()

	// Validate flags
	validGenFlags := map[string]bool{"functions": true, "keywords": true, "types": true, "all": true}
	if !validGenFlags[*genFlag] {
		log.Fatalf("unknown -gen value: %s (use: functions, keywords, types, all)", *genFlag)
	}

	if *genFlag == "all" {
		if *outDirFlag == "" {
			log.Fatal("--outdir flag is required when using -gen=all")
		}
	} else {
		if *outFlag == "" {
			log.Fatal("--out flag is required")
		}
	}

	switch *genFlag {
	case "functions":
		generateFunctionsFile(*outFlag)
	case "keywords":
		generateKeywordsFile(*outFlag)
	case "types":
		generateTypesFile(*outFlag)
	case "all":
		generateFunctionsFile(*outDirFlag + "/functions_gen.go")
		generateKeywordsFile(*outDirFlag + "/keywords_gen.go")
		generateTypesFile(*outDirFlag + "/types_gen.go")
	}
}

func generateFunctionsFile(outPath string) {
	log.Printf("Fetching functions from %s", functionsURL)

	body, err := fetchURL(functionsURL)
	if err != nil {
		log.Fatalf("failed to fetch functions page: %v", err)
	}

	functions, err := parseFunctionsPage(body)
	if err != nil {
		log.Fatalf("failed to parse functions page: %v", err)
	}

	log.Printf("Extracted %d functions", len(functions))

	// Classify functions
	scalars, aggregates, windows, tableFuncs := classifyFunctions(functions)
	log.Printf("Classification: %d scalars, %d aggregates, %d windows, %d table functions",
		len(scalars), len(aggregates), len(windows), len(tableFuncs))

	// Scrape documentation concurrently
	log.Printf("Scraping function documentation with %d workers...", *workersFlag)
	docs := scrapeFunctionDocsConcurrent(functions, *workersFlag)
	log.Printf("Scraped documentation for %d functions", len(docs))

	// Generate code
	code := generateFunctionsCode(scalars, aggregates, windows, tableFuncs, docs)
	writeFormattedCode(outPath, code)
}

func generateKeywordsFile(outPath string) {
	log.Printf("Fetching keywords from %s", keywordsURL)

	body, err := fetchURL(keywordsURL)
	if err != nil {
		log.Fatalf("failed to fetch keywords page: %v", err)
	}

	reserved, ansi, err := parseKeywordsPage(body)
	if err != nil {
		log.Fatalf("failed to parse keywords page: %v", err)
	}

	log.Printf("Extracted %d reserved words, %d ANSI keywords", len(reserved), len(ansi))

	// Generate code
	code := generateKeywordsCode(reserved, ansi)
	writeFormattedCode(outPath, code)
}

func generateTypesFile(outPath string) {
	log.Printf("Fetching types from %s", typesURL)

	body, err := fetchURL(typesURL)
	if err != nil {
		log.Fatalf("failed to fetch types page: %v", err)
	}

	types, err := parseTypesPage(body)
	if err != nil {
		log.Fatalf("failed to parse types page: %v", err)
	}

	log.Printf("Extracted %d data types", len(types))

	// Generate code
	code := generateTypesCode(types)
	writeFormattedCode(outPath, code)
}

func fetchURL(url string) ([]byte, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Set headers to appear as a browser
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; LeapSQL/1.0; +https://github.com/leapstack-labs/leapsql)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	return io.ReadAll(resp.Body)
}

func parseFunctionsPage(body []byte) ([]FunctionInfo, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	var functions []FunctionInfo

	// The HTML structure is:
	// <li><a href=...><code>function_name</code> type description</a></li>
	// Examples:
	//   <code>avg</code> aggregate function
	//   <code>abs</code> function
	//   <code>dense_rank</code> ranking window function
	//   <code>explode</code> table-valued generator function

	var findLinks func(*html.Node)
	findLinks = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			// Look for <code> element inside the <a> tag
			name, typeStr, href := extractFunctionFromLink(n)
			if name != "" {
				funcType := classifyFunctionType(typeStr)
				if funcType != FuncOperator { // Skip operators
					functions = append(functions, FunctionInfo{
						Name:     strings.ToLower(name),
						Type:     funcType,
						Href:     href,
						LinkText: name + " " + typeStr,
					})
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findLinks(c)
		}
	}

	findLinks(doc)
	return functions, nil
}

// extractFunctionFromLink extracts function name from <code> element,
// function type from remaining text, and href from the <a> element.
// Structure: <a href="..."><code>name</code> type description</a>
func extractFunctionFromLink(a *html.Node) (name, typeStr, href string) {
	// Get href attribute
	for _, attr := range a.Attr {
		if attr.Key == "href" {
			href = attr.Val
			break
		}
	}

	var foundCode bool
	var remainingText strings.Builder

	for c := a.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == "code" {
			// Extract function name from <code> element
			name = strings.TrimSpace(extractText(c))
			foundCode = true
		} else if c.Type == html.TextNode && foundCode {
			// Collect text after the <code> element
			remainingText.WriteString(c.Data)
		}
	}

	if !foundCode || name == "" {
		return "", "", ""
	}

	typeStr = strings.ToLower(strings.TrimSpace(remainingText.String()))
	return name, typeStr, href
}

func classifyFunctionType(typeStr string) FunctionType {
	switch {
	case strings.Contains(typeStr, "aggregate"):
		return FuncAggregate
	case strings.Contains(typeStr, "window"):
		return FuncWindow
	case strings.Contains(typeStr, "table-valued"),
		strings.Contains(typeStr, "table function"):
		return FuncTableValued
	case strings.Contains(typeStr, "operator"),
		strings.Contains(typeStr, "predicate"),
		strings.Contains(typeStr, "expression"):
		return FuncOperator
	default:
		return FuncScalar
	}
}

func extractText(n *html.Node) string {
	var buf bytes.Buffer
	var extract func(*html.Node)
	extract = func(n *html.Node) {
		if n.Type == html.TextNode {
			buf.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extract(c)
		}
	}
	extract(n)
	return buf.String()
}

func classifyFunctions(functions []FunctionInfo) (scalars, aggregates, windows, tableFuncs []string) {
	scalarSet := make(map[string]bool)
	aggSet := make(map[string]bool)
	winSet := make(map[string]bool)
	tblSet := make(map[string]bool)

	for _, f := range functions {
		switch f.Type {
		case FuncScalar:
			scalarSet[f.Name] = true
		case FuncAggregate:
			aggSet[f.Name] = true
		case FuncWindow:
			winSet[f.Name] = true
		case FuncTableValued:
			tblSet[f.Name] = true
		}
	}

	scalars = mapToSortedSlice(scalarSet)
	aggregates = mapToSortedSlice(aggSet)
	windows = mapToSortedSlice(winSet)
	tableFuncs = mapToSortedSlice(tblSet)
	return
}

// docResult holds the result of scraping a function's documentation.
type docResult struct {
	name string
	doc  FunctionDoc
	err  error
}

// scrapeFunctionDocsConcurrent fetches documentation for all functions concurrently.
func scrapeFunctionDocsConcurrent(functions []FunctionInfo, numWorkers int) map[string]FunctionDoc {
	docs := make(map[string]FunctionDoc)

	// Filter functions that have hrefs
	var toFetch []FunctionInfo
	for _, f := range functions {
		if f.Href != "" {
			toFetch = append(toFetch, f)
		}
	}

	if len(toFetch) == 0 {
		return docs
	}

	// Create channels
	jobs := make(chan FunctionInfo, len(toFetch))
	results := make(chan docResult, len(toFetch))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range jobs {
				doc, err := fetchFunctionDoc(f)
				results <- docResult{name: f.Name, doc: doc, err: err}
			}
		}()
	}

	// Send jobs
	for _, f := range toFetch {
		jobs <- f
	}
	close(jobs)

	// Wait for workers in a goroutine and close results when done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results with progress logging
	completed := 0
	errors := 0
	for r := range results {
		completed++
		if r.err != nil {
			errors++
			if errors <= 5 {
				log.Printf("Warning: failed to fetch docs for %s: %v", r.name, r.err)
			}
		} else if r.doc.Description != "" || r.doc.Syntax != "" {
			docs[r.name] = r.doc
		}

		// Progress logging every 100 functions
		if completed%100 == 0 || completed == len(toFetch) {
			log.Printf("Progress: %d/%d functions fetched (%d errors)", completed, len(toFetch), errors)
		}
	}

	if errors > 5 {
		log.Printf("Warning: %d total errors fetching function docs", errors)
	}

	return docs
}

// fetchFunctionDoc fetches documentation from a function's individual page.
func fetchFunctionDoc(f FunctionInfo) (FunctionDoc, error) {
	// Build full URL
	url := "https://docs.databricks.com" + f.Href

	body, err := fetchURL(url)
	if err != nil {
		return FunctionDoc{}, err
	}

	return parseFunctionDocPage(body)
}

// parseFunctionDocPage extracts documentation from a function's page.
func parseFunctionDocPage(body []byte) (FunctionDoc, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return FunctionDoc{}, err
	}

	var result FunctionDoc

	// Find the first <p> in main content for description
	// Find <h2>Syntax</h2> followed by <pre> or <code> for syntax

	var inSyntaxSection bool
	var foundDescription bool

	var findDoc func(*html.Node)
	findDoc = func(n *html.Node) {
		if n.Type == html.ElementNode {
			// Look for h2 "Syntax"
			if n.Data == "h2" {
				text := strings.ToLower(extractText(n))
				inSyntaxSection = strings.Contains(text, "syntax")
			}

			// Get first paragraph as description
			if n.Data == "p" && !foundDescription {
				text := strings.TrimSpace(extractText(n))
				// Skip very short paragraphs or those that look like metadata
				if len(text) > 20 && !strings.HasPrefix(text, "Applies to:") {
					result.Description = cleanDescription(text)
					foundDescription = true
				}
			}

			// Get syntax from pre/code block after Syntax header
			if inSyntaxSection && (n.Data == "pre" || n.Data == "code") {
				if result.Syntax == "" {
					result.Syntax = strings.TrimSpace(extractText(n))
					inSyntaxSection = false // Only get first syntax block
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findDoc(c)
		}
	}

	findDoc(doc)
	return result, nil
}

// cleanDescription cleans up extracted description text.
func cleanDescription(s string) string {
	// Remove multiple whitespace
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	// Truncate very long descriptions
	if len(s) > 200 {
		s = s[:197] + "..."
	}
	return s
}

func parseKeywordsPage(body []byte) (reserved, ansi []string, err error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}

	reservedSet := make(map[string]bool)
	ansiSet := make(map[string]bool)

	// The reserved words page has sections:
	// - Reserved words (list items with keywords like ANTI, CROSS, etc.)
	// - ANSI Reserved words (grouped by letter)

	var inReservedSection, inANSISection bool

	var findKeywords func(*html.Node)
	findKeywords = func(n *html.Node) {
		if n.Type == html.ElementNode {
			// Check for section headers
			if n.Data == "h2" {
				text := strings.ToLower(extractText(n))
				inReservedSection = strings.Contains(text, "reserved words") && !strings.Contains(text, "ansi")
				inANSISection = strings.Contains(text, "ansi reserved words")
			}

			// Extract keywords from list items
			if n.Data == "li" && inReservedSection {
				text := strings.TrimSpace(extractText(n))
				if text != "" && !strings.Contains(text, " ") {
					reservedSet[strings.ToUpper(text)] = true
				}
			}

			// Extract ANSI keywords from paragraphs (they're listed comma-separated)
			if n.Data == "p" && inANSISection {
				text := extractText(n)
				// Pattern: letter header followed by keywords
				// e.g., "ALL, ALTER, AND, ANY, ARRAY, AS, AT, AUTHORIZATION"
				keywords := extractKeywordsFromText(text)
				for _, kw := range keywords {
					ansiSet[kw] = true
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findKeywords(c)
		}
	}

	findKeywords(doc)

	reserved = mapToSortedSlice(reservedSet)
	ansi = mapToSortedSlice(ansiSet)
	return reserved, ansi, nil
}

func extractKeywordsFromText(text string) []string {
	var keywords []string
	// Split by comma
	parts := strings.Split(text, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		// Keywords are all uppercase letters and underscores
		if p != "" && regexp.MustCompile(`^[A-Z_]+$`).MatchString(p) {
			keywords = append(keywords, p)
		}
	}
	return keywords
}

func parseTypesPage(body []byte) ([]string, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	typeSet := make(map[string]bool)

	// Data types are in a table, first column contains type links
	// e.g., BIGINT, BINARY, BOOLEAN, etc.
	typePattern := regexp.MustCompile(`^([A-Z_]+)`)

	var findTypes func(*html.Node)
	findTypes = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			// Look for type links
			text := strings.TrimSpace(extractText(n))
			if text != "" {
				// Extract type name (handle parameterized types like DECIMAL(p,s))
				text = strings.ToUpper(text)
				if matches := typePattern.FindStringSubmatch(text); matches != nil {
					typeSet[matches[1]] = true
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findTypes(c)
		}
	}

	findTypes(doc)

	// Add common type aliases that might not be in the table
	additionalTypes := []string{
		"INTEGER", "REAL", "NUMERIC", "VARCHAR", "CHAR",
		"LONG", "SHORT", "BYTE",
	}
	for _, t := range additionalTypes {
		typeSet[t] = true
	}

	return mapToSortedSlice(typeSet), nil
}

func mapToSortedSlice(m map[string]bool) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

func generateFunctionsCode(scalars, aggregates, windows, tableFuncs []string, docs map[string]FunctionDoc) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gendatabricks. DO NOT EDIT.\n")
	buf.WriteString("// Source: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin-alpha\n")
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package databricks\n\n")
	buf.WriteString("import \"github.com/leapstack-labs/leapsql/pkg/dialect\"\n\n")

	// Generate scalars slice
	buf.WriteString("// databricksScalars contains all scalar function names.\n")
	buf.WriteString("var databricksScalars = []string{\n")
	writeStringSlice(&buf, scalars)
	buf.WriteString("}\n\n")

	// Generate aggregates slice
	buf.WriteString("// databricksAggregates contains all aggregate function names.\n")
	buf.WriteString("var databricksAggregates = []string{\n")
	writeStringSlice(&buf, aggregates)
	buf.WriteString("}\n\n")

	// Generate windows slice
	buf.WriteString("// databricksWindows contains all window function names.\n")
	buf.WriteString("var databricksWindows = []string{\n")
	writeStringSlice(&buf, windows)
	buf.WriteString("}\n\n")

	// Generate table functions slice
	buf.WriteString("// databricksTableFunctions contains all table-valued function names.\n")
	buf.WriteString("var databricksTableFunctions = []string{\n")
	writeStringSlice(&buf, tableFuncs)
	buf.WriteString("}\n\n")

	// Generate function docs map
	buf.WriteString("// databricksFunctionDocs contains documentation for Databricks functions.\n")
	if len(docs) == 0 {
		buf.WriteString("var databricksFunctionDocs = map[string]core.FunctionDoc{}\n")
	} else {
		buf.WriteString("var databricksFunctionDocs = map[string]core.FunctionDoc{\n")
		// Sort keys for deterministic output
		keys := make([]string, 0, len(docs))
		for k := range docs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, name := range keys {
			doc := docs[name]
			buf.WriteString(fmt.Sprintf("\t%q: {\n", name))
			if doc.Description != "" {
				buf.WriteString(fmt.Sprintf("\t\tDescription: %q,\n", doc.Description))
			}
			if doc.Syntax != "" {
				buf.WriteString(fmt.Sprintf("\t\tSignatures: []string{%q},\n", doc.Syntax))
			}
			buf.WriteString("\t},\n")
		}
		buf.WriteString("}\n")
	}

	return buf.String()
}

func generateKeywordsCode(reserved, ansi []string) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gendatabricks. DO NOT EDIT.\n")
	buf.WriteString("// Source: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-reserved-words\n")
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package databricks\n\n")

	// Combine reserved and ANSI for completion keywords
	allKeywords := make(map[string]bool)
	for _, k := range reserved {
		allKeywords[k] = true
	}
	for _, k := range ansi {
		allKeywords[k] = true
	}
	combined := mapToSortedSlice(allKeywords)

	// Generate completion keywords (all keywords)
	buf.WriteString("// databricksCompletionKeywords contains all keywords for LSP completions.\n")
	buf.WriteString("var databricksCompletionKeywords = []string{\n")
	writeStringSlice(&buf, combined)
	buf.WriteString("}\n\n")

	// Generate reserved words (words that need quoting)
	buf.WriteString("// databricksReservedWords contains words that need quoting when used as identifiers.\n")
	buf.WriteString("var databricksReservedWords = []string{\n")
	writeStringSlice(&buf, reserved)
	buf.WriteString("}\n")

	return buf.String()
}

func generateTypesCode(types []string) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gendatabricks. DO NOT EDIT.\n")
	buf.WriteString("// Source: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes\n")
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package databricks\n\n")

	buf.WriteString("// databricksTypes contains all supported data types for LSP completions.\n")
	buf.WriteString("var databricksTypes = []string{\n")
	writeStringSlice(&buf, types)
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
