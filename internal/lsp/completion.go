package lsp

import (
	"fmt"
	"regexp"
	"strings"

	pkgparser "github.com/leapstack-labs/leapsql/pkg/parser"
)

// CompletionContextType describes what kind of completion context we're in.
type CompletionContextType int

// Completion context type constants.
const (
	ContextUnknown CompletionContextType = iota
	ContextSelectClause
	ContextFromClause
	ContextWhereClause
	ContextColumnAccess // After "table."
	ContextFunctionArgs // Inside "func("
	ContextStarlarkRoot // Inside {{ }} at root level
	ContextMacroAccess  // Inside {{ namespace. }}
	ContextConfigAccess // Inside config. or config["
)

// builtinGlobals are the reserved Starlark globals with documentation.
// Note: LeapSQL auto-detects dependencies from SQL - no ref() function needed.
var builtinGlobals = []CompletionItem{
	{
		Label:         "config",
		Kind:          CompletionItemKindVariable,
		Detail:        "dict",
		Documentation: "Dictionary containing parsed YAML frontmatter. Access with config[\"key\"] or config.key",
	},
	{
		Label:         "env",
		Kind:          CompletionItemKindVariable,
		Detail:        "string",
		Documentation: "Current environment name (e.g., \"prod\", \"dev\", \"staging\")",
	},
	{
		Label:         "target",
		Kind:          CompletionItemKindVariable,
		Detail:        "struct",
		Documentation: "Target database configuration. Fields: target.type, target.schema, target.database",
	},
	{
		Label:         "this",
		Kind:          CompletionItemKindVariable,
		Detail:        "struct",
		Documentation: "Current model information. Fields: this.name, this.schema, this.database",
	},
}

// sqlKeywords for basic SQL completion
var sqlKeywords = []CompletionItem{
	{Label: "SELECT", Kind: CompletionItemKindKeyword},
	{Label: "FROM", Kind: CompletionItemKindKeyword},
	{Label: "WHERE", Kind: CompletionItemKindKeyword},
	{Label: "JOIN", Kind: CompletionItemKindKeyword},
	{Label: "LEFT JOIN", Kind: CompletionItemKindKeyword},
	{Label: "RIGHT JOIN", Kind: CompletionItemKindKeyword},
	{Label: "INNER JOIN", Kind: CompletionItemKindKeyword},
	{Label: "GROUP BY", Kind: CompletionItemKindKeyword},
	{Label: "ORDER BY", Kind: CompletionItemKindKeyword},
	{Label: "HAVING", Kind: CompletionItemKindKeyword},
	{Label: "LIMIT", Kind: CompletionItemKindKeyword},
	{Label: "OFFSET", Kind: CompletionItemKindKeyword},
	{Label: "AS", Kind: CompletionItemKindKeyword},
	{Label: "ON", Kind: CompletionItemKindKeyword},
	{Label: "AND", Kind: CompletionItemKindKeyword},
	{Label: "OR", Kind: CompletionItemKindKeyword},
	{Label: "NOT", Kind: CompletionItemKindKeyword},
	{Label: "IN", Kind: CompletionItemKindKeyword},
	{Label: "BETWEEN", Kind: CompletionItemKindKeyword},
	{Label: "LIKE", Kind: CompletionItemKindKeyword},
	{Label: "IS NULL", Kind: CompletionItemKindKeyword},
	{Label: "IS NOT NULL", Kind: CompletionItemKindKeyword},
	{Label: "DISTINCT", Kind: CompletionItemKindKeyword},
	{Label: "CASE", Kind: CompletionItemKindKeyword},
	{Label: "WHEN", Kind: CompletionItemKindKeyword},
	{Label: "THEN", Kind: CompletionItemKindKeyword},
	{Label: "ELSE", Kind: CompletionItemKindKeyword},
	{Label: "END", Kind: CompletionItemKindKeyword},
	{Label: "WITH", Kind: CompletionItemKindKeyword},
	{Label: "UNION", Kind: CompletionItemKindKeyword},
	{Label: "UNION ALL", Kind: CompletionItemKindKeyword},
	{Label: "EXCEPT", Kind: CompletionItemKindKeyword},
	{Label: "INTERSECT", Kind: CompletionItemKindKeyword},
}

// getSQLFunctionCompletions returns SQL function completions from the DuckDB catalog.
func getSQLFunctionCompletions(prefix string) []CompletionItem {
	functions := pkgparser.SearchFunctions(prefix)
	items := make([]CompletionItem, 0, len(functions))
	for _, fn := range functions {
		item := CompletionItem{
			Label:         fn.Name,
			Kind:          CompletionItemKindFunction,
			Detail:        fn.Signature,
			Documentation: fn.Description,
		}
		if fn.Snippet != "" {
			item.InsertText = fn.Snippet
			item.InsertTextFormat = InsertTextFormatSnippet
		}
		items = append(items, item)
	}
	return items
}

// configKeys for config.* completion
var configKeys = []string{"name", "materialized", "schema", "owner", "tags", "unique_key", "meta"}

// getCompletions returns completion items for the given position.
func (s *Server) getCompletions(params CompletionParams) []CompletionItem {
	doc := s.documents.Get(params.TextDocument.URI)
	if doc == nil {
		return nil
	}

	ctx, extra := s.detectContext(doc, params.Position)
	prefix := s.extractPrefix(doc, params.Position)

	var items []CompletionItem

	switch ctx {
	case ContextStarlarkRoot:
		// Add builtin globals
		for _, builtin := range builtinGlobals {
			if strings.HasPrefix(strings.ToLower(builtin.Label), strings.ToLower(prefix)) {
				items = append(items, builtin)
			}
		}

		// Add macro namespaces from SQLite
		if s.store != nil {
			namespaces, _ := s.store.GetMacroNamespaces()
			for _, ns := range namespaces {
				if strings.HasPrefix(ns.Name, prefix) {
					detail := "macro"
					if ns.Package != "" {
						detail = fmt.Sprintf("macro (from %s)", ns.Package)
					}
					items = append(items, CompletionItem{
						Label:         ns.Name,
						Kind:          CompletionItemKindModule,
						Detail:        detail,
						Documentation: fmt.Sprintf("Macro namespace from %s", ns.FilePath),
					})
				}
			}
		}

	case ContextMacroAccess:
		// User typed "namespace." - get functions for that namespace
		namespace := extra
		if s.store != nil {
			functions, _ := s.store.GetMacroFunctions(namespace)
			for _, fn := range functions {
				if strings.HasPrefix(fn.Name, prefix) {
					items = append(items, CompletionItem{
						Label:            fn.Name,
						Kind:             CompletionItemKindFunction,
						Detail:           formatSignature(fn.Name, fn.Args),
						Documentation:    fn.Docstring,
						InsertText:       fn.Name + "($1)",
						InsertTextFormat: InsertTextFormatSnippet,
					})
				}
			}
		}

	case ContextConfigAccess:
		// Suggest known config keys
		for _, key := range configKeys {
			if strings.HasPrefix(key, prefix) {
				items = append(items, CompletionItem{
					Label:  key,
					Kind:   CompletionItemKindProperty,
					Detail: "config field",
				})
			}
		}

	case ContextSelectClause, ContextWhereClause:
		// Suggest SQL functions from catalog
		items = append(items, getSQLFunctionCompletions(prefix)...)

	case ContextFromClause:
		// Suggest models from SQLite
		if s.store != nil {
			models, _ := s.store.ListModels()
			for _, model := range models {
				if strings.HasPrefix(strings.ToLower(model.Name), strings.ToLower(prefix)) {
					items = append(items, CompletionItem{
						Label:      model.Name,
						Kind:       CompletionItemKindModule,
						Detail:     model.Path,
						InsertText: model.Name,
					})
				}
			}
		}

	default:
		// Default: provide SQL keywords and functions from catalog
		for _, kw := range sqlKeywords {
			if strings.HasPrefix(strings.ToUpper(kw.Label), strings.ToUpper(prefix)) {
				items = append(items, kw)
			}
		}
		items = append(items, getSQLFunctionCompletions(prefix)...)
	}

	return items
}

// detectContext determines the completion context at the given position.
func (s *Server) detectContext(doc *Document, pos Position) (CompletionContextType, string) {
	before := doc.GetTextBefore(pos)

	// 1. Check if inside template expression {{ }}
	if inTemplateExpr(before) {
		exprContent := extractTemplateExprContent(before)

		// Check for namespace.function pattern (e.g., "utils.")
		if dotIdx := strings.LastIndex(exprContent, "."); dotIdx != -1 {
			namespace := extractIdentifierBefore(exprContent, dotIdx)
			if namespace != "" {
				if namespace == "config" {
					return ContextConfigAccess, ""
				}
				return ContextMacroAccess, namespace
			}
		}

		// Root level inside {{ }}
		return ContextStarlarkRoot, ""
	}

	// 2. Check SQL clause context
	lastKeyword := findLastSQLKeyword(before)
	switch strings.ToUpper(lastKeyword) {
	case "SELECT", "DISTINCT":
		return ContextSelectClause, ""
	case "FROM", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "LATERAL":
		return ContextFromClause, ""
	case "WHERE", "AND", "OR", "ON", "HAVING":
		return ContextWhereClause, ""
	}

	return ContextUnknown, ""
}

// extractPrefix gets the word being typed at the cursor position.
func (s *Server) extractPrefix(doc *Document, pos Position) string {
	before := doc.GetTextBefore(pos)
	if len(before) == 0 {
		return ""
	}

	// Find the start of the current word
	start := len(before)
	for start > 0 && isIdentChar(before[start-1]) {
		start--
	}

	return before[start:]
}

// Helper functions

func inTemplateExpr(before string) bool {
	lastOpen := strings.LastIndex(before, "{{")
	lastClose := strings.LastIndex(before, "}}")
	return lastOpen != -1 && lastOpen > lastClose
}

func extractTemplateExprContent(before string) string {
	lastOpen := strings.LastIndex(before, "{{")
	if lastOpen == -1 {
		return ""
	}
	return strings.TrimSpace(before[lastOpen+2:])
}

func extractIdentifierBefore(s string, pos int) string {
	end := pos
	start := pos
	for start > 0 && isIdentChar(s[start-1]) {
		start--
	}
	if start == end {
		return ""
	}
	return s[start:end]
}

func findLastSQLKeyword(before string) string {
	// Look for SQL keywords in reverse order
	keywords := []string{"SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER",
		"OUTER", "CROSS", "LATERAL", "GROUP", "ORDER", "HAVING", "LIMIT", "DISTINCT",
		"AND", "OR", "ON"}

	upper := strings.ToUpper(before)
	lastPos := -1
	lastKeyword := ""

	for _, kw := range keywords {
		pattern := regexp.MustCompile(`\b` + kw + `\b`)
		matches := pattern.FindAllStringIndex(upper, -1)
		if len(matches) > 0 {
			lastMatch := matches[len(matches)-1]
			if lastMatch[0] > lastPos {
				lastPos = lastMatch[0]
				lastKeyword = kw
			}
		}
	}

	return lastKeyword
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '_'
}

func formatSignature(name string, args []string) string {
	return fmt.Sprintf("%s(%s)", name, strings.Join(args, ", "))
}

// getHover returns hover information for the position.
func (s *Server) getHover(params HoverParams) *Hover {
	doc := s.documents.Get(params.TextDocument.URI)
	if doc == nil {
		return nil
	}

	word, _ := doc.GetWordAtPosition(params.Position)
	if word == "" {
		return nil
	}

	// Check for builtin globals
	for _, builtin := range builtinGlobals {
		if builtin.Label == word {
			return &Hover{
				Contents: MarkupContent{
					Kind:  MarkupKindMarkdown,
					Value: fmt.Sprintf("**%s** (%s)\n\n%s", builtin.Label, builtin.Detail, builtin.Documentation),
				},
			}
		}
	}

	// Check for macro patterns
	before := doc.GetTextBefore(params.Position)
	if inTemplateExpr(before) {
		// Try to find namespace.function pattern
		exprContent := extractTemplateExprContent(before) + word
		macroPattern := regexp.MustCompile(`(\w+)\.(\w+)$`)
		if match := macroPattern.FindStringSubmatch(exprContent); match != nil {
			namespace := match[1]
			funcName := match[2]

			if s.store != nil {
				fn, _ := s.store.GetMacroFunction(namespace, funcName)
				if fn != nil {
					content := fmt.Sprintf("```\n%s.%s(%s)\n```", namespace, fn.Name, strings.Join(fn.Args, ", "))
					if fn.Docstring != "" {
						content += "\n\n" + fn.Docstring
					}

					return &Hover{
						Contents: MarkupContent{
							Kind:  MarkupKindMarkdown,
							Value: content,
						},
					}
				}
			}
		}
	}

	// Check for SQL functions in the catalog
	upperWord := strings.ToUpper(word)
	for _, fn := range pkgparser.DuckDBCatalog {
		if fn.Name == upperWord {
			content := fmt.Sprintf("**%s** (%s)\n\n%s", fn.Name, fn.Signature, fn.Description)
			if fn.IsAggregate {
				content += "\n\n*Aggregate function*"
			} else if fn.Category == pkgparser.CategoryWindow {
				content += "\n\n*Window function*"
			}
			return &Hover{
				Contents: MarkupContent{
					Kind:  MarkupKindMarkdown,
					Value: content,
				},
			}
		}
	}

	return nil
}

// getDefinition returns the definition location for the position.
func (s *Server) getDefinition(params DefinitionParams) *Location {
	doc := s.documents.Get(params.TextDocument.URI)
	if doc == nil {
		return nil
	}

	word, _ := doc.GetWordAtPosition(params.Position)
	if word == "" {
		return nil
	}

	before := doc.GetTextBefore(params.Position)

	// Check if inside template expression
	if inTemplateExpr(before) && s.store != nil {
		// Try to find namespace.function pattern
		exprContent := extractTemplateExprContent(before) + word
		macroPattern := regexp.MustCompile(`(\w+)\.(\w+)$`)
		if match := macroPattern.FindStringSubmatch(exprContent); match != nil {
			namespace := match[1]
			funcName := match[2]

			// Get namespace info
			ns, _ := s.store.GetMacroNamespace(namespace)
			if ns == nil {
				return nil
			}

			// Get function info for line number
			fn, _ := s.store.GetMacroFunction(namespace, funcName)
			line := 0
			if fn != nil {
				line = fn.Line - 1 // Convert to 0-based
			}

			return &Location{
				URI: PathToURI(ns.FilePath),
				Range: Range{
					Start: Position{Line: uint32(line), Character: 0}, //nolint:gosec // G115: line is always non-negative from AST
					End:   Position{Line: uint32(line), Character: 0}, //nolint:gosec // G115: line is always non-negative from AST
				},
			}
		}
	}

	return nil
}
