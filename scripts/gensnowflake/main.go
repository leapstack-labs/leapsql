// Package main provides a web scraper that extracts Snowflake SQL function metadata
// from the Snowflake documentation and generates Go code for the dialect package.
//
// Usage:
//
//	go run ./scripts/gensnowflake -gen=all -outdir=pkg/dialects/snowflake/
//	go run ./scripts/gensnowflake -gen=functions -out=pkg/dialects/snowflake/functions_gen.go
//	go run ./scripts/gensnowflake -gen=keywords -out=pkg/dialects/snowflake/keywords_gen.go
//	go run ./scripts/gensnowflake -gen=types -out=pkg/dialects/snowflake/types_gen.go
//
// The scraper fetches function names from the alphabetical functions page, then concurrently
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
	baseURL      = "https://docs.snowflake.com/en/sql-reference"
	functionsURL = baseURL + "/functions-all"
	keywordsURL  = baseURL + "/reserved-keywords"
	typesURL     = baseURL + "/intro-summary-data-types"
)

var (
	genFlag     = flag.String("gen", "all", "what to generate: functions, keywords, types, all")
	outFlag     = flag.String("out", "", "output file path (required for single generation)")
	outDirFlag  = flag.String("outdir", "", "output directory (for 'all' generation)")
	workersFlag = flag.Int("workers", 20, "number of concurrent workers for fetching docs")
)

// FunctionInfo holds metadata about a SQL function.
type FunctionInfo struct {
	Name     string
	Types    []string // Function can belong to multiple types
	Href     string   // URL path to function documentation
	Category string   // Raw category from docs for debugging
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
	scalars, aggregates, windows, tableFuncs, generators := classifyFunctions(functions)
	log.Printf("Classification: %d scalars, %d aggregates, %d windows, %d table functions, %d generators",
		len(scalars), len(aggregates), len(windows), len(tableFuncs), len(generators))

	// Scrape documentation concurrently
	log.Printf("Scraping function documentation with %d workers...", *workersFlag)
	docs := scrapeFunctionDocsConcurrent(functions, *workersFlag)
	log.Printf("Scraped documentation for %d functions", len(docs))

	// Generate code
	code := generateFunctionsCode(scalars, aggregates, windows, tableFuncs, generators, docs)
	writeFormattedCode(outPath, code)
}

func generateKeywordsFile(outPath string) {
	log.Printf("Fetching keywords from %s", keywordsURL)

	body, err := fetchURL(keywordsURL)
	if err != nil {
		log.Fatalf("failed to fetch keywords page: %v", err)
	}

	keywords, reserved, err := parseKeywordsPage(body)
	if err != nil {
		log.Fatalf("failed to parse keywords page: %v", err)
	}

	log.Printf("Extracted %d keywords, %d reserved words", len(keywords), len(reserved))

	// Generate code
	code := generateKeywordsCode(keywords, reserved)
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
	seen := make(map[string]bool)

	// Snowflake docs: single table with 3 columns (Function Name, Summary, Category)
	// Letter headers (A-Z) are single char rows to skip
	var inTable bool
	var findFunctions func(*html.Node)
	findFunctions = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "table" {
			inTable = true
		}

		if inTable && n.Type == html.ElementNode && n.Data == "tr" {
			fn := extractFunctionFromRow(n)
			if fn != nil && fn.Name != "" && !seen[fn.Name] {
				// Skip letter headers (single char A-Z)
				if len(fn.Name) == 1 && fn.Name >= "A" && fn.Name <= "Z" {
					return
				}
				seen[fn.Name] = true
				functions = append(functions, *fn)
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findFunctions(c)
		}

		if n.Type == html.ElementNode && n.Data == "table" {
			inTable = false
		}
	}

	findFunctions(doc)
	return functions, nil
}

// extractFunctionFromRow extracts function info from a table row.
// Structure: <tr><td><a href="...">function_name</a></td><td>summary</td><td>category</td></tr>
func extractFunctionFromRow(tr *html.Node) *FunctionInfo {
	var cells []*html.Node

	// Collect td elements
	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == "td" {
			cells = append(cells, c)
		}
	}

	if len(cells) < 3 {
		return nil
	}

	// Extract function name and href from first cell
	name, href := extractLinkFromCell(cells[0])
	if name == "" {
		return nil
	}

	// Clean up function name (remove parenthetical suffixes, extra info)
	name = cleanFunctionName(name)
	if name == "" {
		return nil
	}

	// Extract category from third cell
	category := strings.TrimSpace(extractText(cells[2]))

	// Classify based on category
	types := classifyCategory(category)

	return &FunctionInfo{
		Name:     strings.ToLower(name),
		Types:    types,
		Href:     href,
		Category: category,
	}
}

// cleanFunctionName extracts a valid SQL function name from documentation text.
// Handles cases like:
//   - "ABS" -> "abs"
//   - "CHR , CHAR" -> "chr" (first function)
//   - "CONCAT , ||" -> "concat"
//   - "AI_COMPLETE (Prompt Object)" -> "ai_complete"
//   - "[ NOT ] BETWEEN" -> "" (skip, it's an operator)
//   - "A" -> "" (skip, it's a letter header)
func cleanFunctionName(name string) string {
	// Skip letter headers (single char A-Z)
	if len(name) == 1 && name >= "A" && name <= "z" {
		return ""
	}

	// Skip operator patterns like "[ NOT ] BETWEEN"
	if strings.Contains(name, "[") || strings.Contains(name, "]") {
		return ""
	}

	// Skip special service name patterns
	if strings.Contains(name, "<") || strings.Contains(name, ">") {
		return ""
	}

	// Remove parenthetical suffixes like "(system data metric function)" or "(snowflake.cortex)"
	if idx := strings.Index(name, "("); idx > 0 {
		name = strings.TrimSpace(name[:idx])
	}

	// Handle comma-separated alternatives (e.g., "CHR , CHAR" -> take first)
	if idx := strings.Index(name, ","); idx > 0 {
		name = strings.TrimSpace(name[:idx])
	}

	// Handle slash-separated alternatives (e.g., "LENGTH, LEN" or "HOUR / MINUTE / SECOND" -> take first)
	if idx := strings.Index(name, "/"); idx > 0 {
		name = strings.TrimSpace(name[:idx])
	}

	// Skip if empty after cleanup
	if name == "" {
		return ""
	}

	// Validate: must be alphanumeric + underscores only
	validName := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if !validName.MatchString(name) {
		return ""
	}

	return name
}

// extractLinkFromCell extracts text and href from a cell containing a link.
func extractLinkFromCell(td *html.Node) (name, href string) {
	var findLink func(*html.Node)
	findLink = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			name = strings.TrimSpace(extractText(n))
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					href = attr.Val
					break
				}
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findLink(c)
		}
	}
	findLink(td)

	// If no link found, just get text content
	if name == "" {
		name = strings.TrimSpace(extractText(td))
	}
	return name, href
}

// classifyCategory maps Snowflake category strings to function types.
// A function can have multiple types (e.g., ANY_VALUE is both Aggregate and Window).
func classifyCategory(cat string) []string {
	var types []string
	catLower := strings.ToLower(cat)

	if strings.Contains(catLower, "aggregate") {
		types = append(types, "aggregate")
	}
	if strings.Contains(catLower, "window") {
		types = append(types, "window")
	}
	if strings.Contains(catLower, "table function") {
		types = append(types, "table")
	}
	if strings.Contains(catLower, "data generation") || strings.Contains(catLower, "context") {
		types = append(types, "generator")
	}

	// Default to scalar if no other classification
	if len(types) == 0 {
		types = append(types, "scalar")
	}

	return types
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

func classifyFunctions(functions []FunctionInfo) (scalars, aggregates, windows, tableFuncs, generators []string) {
	scalarSet := make(map[string]bool)
	aggSet := make(map[string]bool)
	winSet := make(map[string]bool)
	tblSet := make(map[string]bool)
	genSet := make(map[string]bool)

	for _, f := range functions {
		for _, t := range f.Types {
			switch t {
			case "scalar":
				scalarSet[f.Name] = true
			case "aggregate":
				aggSet[f.Name] = true
			case "window":
				winSet[f.Name] = true
			case "table":
				tblSet[f.Name] = true
			case "generator":
				genSet[f.Name] = true
			}
		}
	}

	scalars = mapToSortedSlice(scalarSet)
	aggregates = mapToSortedSlice(aggSet)
	windows = mapToSortedSlice(winSet)
	tableFuncs = mapToSortedSlice(tblSet)
	generators = mapToSortedSlice(genSet)
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
	// Build full URL - Snowflake hrefs are relative: functions/<name>
	url := f.Href
	if !strings.HasPrefix(url, "http") {
		// Handle relative URLs
		if strings.HasPrefix(url, "/") {
			url = "https://docs.snowflake.com" + url
		} else {
			url = baseURL + "/" + url
		}
	}

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
				if len(text) > 20 {
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

func parseKeywordsPage(body []byte) (keywords, reserved []string, err error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}

	keywordSet := make(map[string]bool)
	reservedSet := make(map[string]bool)

	// Snowflake keywords page: single table with 2 columns (Keyword, Comment)
	// ANSI reserved words have "ANSI" in the comment
	var inTable bool
	var findKeywords func(*html.Node)
	findKeywords = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "table" {
			inTable = true
		}

		if inTable && n.Type == html.ElementNode && n.Data == "tr" {
			kw, comment := extractKeywordFromRow(n)
			if kw != "" {
				// Skip letter headers (single char A-Z)
				if len(kw) == 1 && kw >= "A" && kw <= "Z" {
					return
				}
				keywordSet[strings.ToUpper(kw)] = true
				// All Snowflake reserved keywords go to reserved list
				reservedSet[strings.ToUpper(kw)] = true
				_ = comment // Comment used for debugging if needed
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findKeywords(c)
		}

		if n.Type == html.ElementNode && n.Data == "table" {
			inTable = false
		}
	}

	findKeywords(doc)

	keywords = mapToSortedSlice(keywordSet)
	reserved = mapToSortedSlice(reservedSet)
	return keywords, reserved, nil
}

// extractKeywordFromRow extracts keyword and comment from a table row.
func extractKeywordFromRow(tr *html.Node) (keyword, comment string) {
	var cells []*html.Node

	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == "td" {
			cells = append(cells, c)
		}
	}

	if len(cells) < 1 {
		return "", ""
	}

	keyword = strings.TrimSpace(extractText(cells[0]))
	if len(cells) >= 2 {
		comment = strings.TrimSpace(extractText(cells[1]))
	}
	return keyword, comment
}

func parseTypesPage(body []byte) ([]string, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	typeSet := make(map[string]bool)

	// Snowflake types page: single table with 3 columns (Category, Type, Notes)
	// Types in column 1 may be comma-separated and parameterized
	typePattern := regexp.MustCompile(`^([A-Z_]+)`)

	var inTable bool
	var findTypes func(*html.Node)
	findTypes = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "table" {
			inTable = true
		}

		if inTable && n.Type == html.ElementNode && n.Data == "tr" {
			extractTypesFromRow(n, typeSet, typePattern)
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findTypes(c)
		}

		if n.Type == html.ElementNode && n.Data == "table" {
			inTable = false
		}
	}

	findTypes(doc)

	// Add known Snowflake types that might not be in the table
	knownTypes := []string{
		"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
		"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL", "DECFLOAT",
		"VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "BINARY", "VARBINARY",
		"BOOLEAN",
		"DATE", "DATETIME", "TIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ",
		"VARIANT", "OBJECT", "ARRAY", "MAP", "FILE", "GEOGRAPHY", "GEOMETRY", "VECTOR",
	}
	for _, t := range knownTypes {
		typeSet[t] = true
	}

	return mapToSortedSlice(typeSet), nil
}

// extractTypesFromRow extracts types from a table row.
func extractTypesFromRow(tr *html.Node, typeSet map[string]bool, typePattern *regexp.Regexp) {
	var cells []*html.Node

	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == "td" {
			cells = append(cells, c)
		}
	}

	if len(cells) < 2 {
		return
	}

	// Types are in the second column (index 1)
	typeText := strings.TrimSpace(extractText(cells[1]))
	typeText = strings.ToUpper(typeText)

	// Split by comma and extract base types
	for _, part := range strings.Split(typeText, ",") {
		part = strings.TrimSpace(part)
		// Extract base type name (strip parentheses)
		if idx := strings.Index(part, "("); idx > 0 {
			part = part[:idx]
		}
		part = strings.TrimSpace(part)
		if matches := typePattern.FindStringSubmatch(part); matches != nil {
			typeSet[matches[1]] = true
		}
	}
}

func mapToSortedSlice(m map[string]bool) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

func generateFunctionsCode(scalars, aggregates, windows, tableFuncs, generators []string, docs map[string]FunctionDoc) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gensnowflake. DO NOT EDIT.\n")
	buf.WriteString("// Source: https://docs.snowflake.com/en/sql-reference/functions-all\n")
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package snowflake\n\n")
	buf.WriteString("import \"github.com/leapstack-labs/leapsql/pkg/dialect\"\n\n")

	// Generate scalars slice
	buf.WriteString("// snowflakeScalars contains all scalar function names.\n")
	buf.WriteString("var snowflakeScalars = []string{\n")
	writeStringSlice(&buf, scalars)
	buf.WriteString("}\n\n")

	// Generate aggregates slice
	buf.WriteString("// snowflakeAggregates contains all aggregate function names.\n")
	buf.WriteString("var snowflakeAggregates = []string{\n")
	writeStringSlice(&buf, aggregates)
	buf.WriteString("}\n\n")

	// Generate windows slice
	buf.WriteString("// snowflakeWindows contains all window function names.\n")
	buf.WriteString("var snowflakeWindows = []string{\n")
	writeStringSlice(&buf, windows)
	buf.WriteString("}\n\n")

	// Generate table functions slice
	buf.WriteString("// snowflakeTableFunctions contains all table-valued function names.\n")
	buf.WriteString("var snowflakeTableFunctions = []string{\n")
	writeStringSlice(&buf, tableFuncs)
	buf.WriteString("}\n\n")

	// Generate generators slice
	buf.WriteString("// snowflakeGenerators contains generator functions (produce values without input).\n")
	buf.WriteString("var snowflakeGenerators = []string{\n")
	writeStringSlice(&buf, generators)
	buf.WriteString("}\n\n")

	// Generate function docs map
	buf.WriteString("// snowflakeFunctionDocs contains documentation for Snowflake functions.\n")
	if len(docs) == 0 {
		buf.WriteString("var snowflakeFunctionDocs = map[string]core.FunctionDoc{}\n")
	} else {
		buf.WriteString("var snowflakeFunctionDocs = map[string]core.FunctionDoc{\n")
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

func generateKeywordsCode(keywords, reserved []string) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gensnowflake. DO NOT EDIT.\n")
	buf.WriteString("// Source: https://docs.snowflake.com/en/sql-reference/reserved-keywords\n")
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package snowflake\n\n")

	// Generate completion keywords (all keywords)
	buf.WriteString("// snowflakeCompletionKeywords contains all keywords for LSP completions.\n")
	buf.WriteString("var snowflakeCompletionKeywords = []string{\n")
	writeStringSlice(&buf, keywords)
	buf.WriteString("}\n\n")

	// Generate reserved words (words that need quoting)
	buf.WriteString("// snowflakeReservedWords contains words that need quoting when used as identifiers.\n")
	buf.WriteString("var snowflakeReservedWords = []string{\n")
	writeStringSlice(&buf, reserved)
	buf.WriteString("}\n")

	return buf.String()
}

func generateTypesCode(types []string) string {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by scripts/gensnowflake. DO NOT EDIT.\n")
	buf.WriteString("// Source: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types\n")
	fmt.Fprintf(&buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package snowflake\n\n")

	buf.WriteString("// snowflakeTypes contains all supported data types for LSP completions.\n")
	buf.WriteString("var snowflakeTypes = []string{\n")
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
