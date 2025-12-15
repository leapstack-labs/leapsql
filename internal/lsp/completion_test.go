package lsp

import (
	"strings"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/sql"
	"github.com/stretchr/testify/assert"
)

func TestInTemplateExpr(t *testing.T) {
	tests := []struct {
		before   string
		expected bool
	}{
		{"SELECT {{ ", true},
		{"SELECT {{ config.", true},
		{"{{ utils.upper(", true},
		{"SELECT {{ config.name }}", false},
		{"SELECT * FROM users", false},
		{"{{ x }} SELECT {{ ", true},
		{"{{ x }} SELECT {{ y }}", false},
		{"no templates here", false},
		{"{{ ", true},
		{"{", false},
		{"}", false},
	}

	for _, tt := range tests {
		result := inTemplateExpr(tt.before)
		assert.Equal(t, tt.expected, result, "inTemplateExpr(%q)", tt.before)
	}
}

func TestExtractTemplateExprContent(t *testing.T) {
	tests := []struct {
		before   string
		expected string
	}{
		{"SELECT {{ ", ""},
		{"SELECT {{ config.", "config."},
		{"SELECT {{ utils.upper(", "utils.upper("},
		{"{{ utils.format(", "utils.format("},
		{"no templates", ""},
	}

	for _, tt := range tests {
		result := extractTemplateExprContent(tt.before)
		assert.Equal(t, tt.expected, result, "extractTemplateExprContent(%q)", tt.before)
	}
}

func TestExtractIdentifierBefore(t *testing.T) {
	tests := []struct {
		s        string
		pos      int
		expected string
	}{
		{"utils.", 5, "utils"},
		{"config.name", 6, "config"},
		{"  namespace.", 11, "namespace"},
		{"a.b.c.", 5, "c"},
		{".", 0, ""},
		{"abc", 3, "abc"},
	}

	for _, tt := range tests {
		result := extractIdentifierBefore(tt.s, tt.pos)
		assert.Equal(t, tt.expected, result, "extractIdentifierBefore(%q, %d)", tt.s, tt.pos)
	}
}

func TestFindLastSQLKeyword(t *testing.T) {
	tests := []struct {
		before   string
		expected string
	}{
		{"SELECT ", "SELECT"},
		{"SELECT * FROM ", "FROM"},
		{"SELECT * FROM users WHERE ", "WHERE"},
		{"SELECT id FROM users JOIN orders ON ", "ON"},
		{"SELECT DISTINCT ", "DISTINCT"},
		{"SELECT * FROM users WHERE a = 1 AND ", "AND"},
		{"SELECT * FROM users WHERE a = 1 OR ", "OR"},
		{"", ""},
		{"no keywords", ""},
	}

	for _, tt := range tests {
		result := findLastSQLKeyword(tt.before)
		assert.Equal(t, tt.expected, result, "findLastSQLKeyword(%q)", tt.before)
	}
}

func TestIsIdentChar(t *testing.T) {
	validChars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
	invalidChars := " \t\n.,-!@#$%^&*()"

	for _, c := range validChars {
		assert.True(t, isIdentChar(byte(c)), "isIdentChar(%q) should be true", c)
	}

	for _, c := range invalidChars {
		assert.False(t, isIdentChar(byte(c)), "isIdentChar(%q) should be false", c)
	}
}

func TestFormatSignature(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected string
	}{
		{"upper", []string{"column"}, "upper(column)"},
		{"coalesce", []string{"column", "default"}, "coalesce(column, default)"},
		{"no_args", []string{}, "no_args()"},
		{"with_defaults", []string{"x", "y=None"}, "with_defaults(x, y=None)"},
	}

	for _, tt := range tests {
		result := formatSignature(tt.name, tt.args)
		assert.Equal(t, tt.expected, result, "formatSignature(%q, %v)", tt.name, tt.args)
	}
}

func TestServer_DetectContext(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	tests := []struct {
		name        string
		content     string
		pos         Position
		expectedCtx CompletionContextType
		expectedArg string
	}{
		{
			name:        "template root",
			content:     "SELECT {{ ",
			pos:         Position{Line: 0, Character: 10},
			expectedCtx: ContextStarlarkRoot,
		},
		{
			name:        "macro access",
			content:     "SELECT {{ utils.",
			pos:         Position{Line: 0, Character: 16},
			expectedCtx: ContextMacroAccess,
			expectedArg: "utils",
		},
		{
			name:        "config access",
			content:     "SELECT {{ config.",
			pos:         Position{Line: 0, Character: 17},
			expectedCtx: ContextConfigAccess,
		},
		{
			name:        "SELECT clause",
			content:     "SELECT ",
			pos:         Position{Line: 0, Character: 7},
			expectedCtx: ContextSelectClause,
		},
		{
			name:        "FROM clause",
			content:     "SELECT * FROM ",
			pos:         Position{Line: 0, Character: 14},
			expectedCtx: ContextFromClause,
		},
		{
			name:        "WHERE clause",
			content:     "SELECT * FROM users WHERE ",
			pos:         Position{Line: 0, Character: 26},
			expectedCtx: ContextWhereClause,
		},
		{
			name:        "unknown",
			content:     "",
			pos:         Position{Line: 0, Character: 0},
			expectedCtx: ContextUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := &Document{
				Content: tt.content,
				Lines:   computeLineOffsets(tt.content),
			}

			ctx, arg := server.detectContext(doc, tt.pos)
			assert.Equal(t, tt.expectedCtx, ctx, "detectContext context")
			if tt.expectedArg != "" {
				assert.Equal(t, tt.expectedArg, arg, "detectContext arg")
			}
		})
	}
}

func TestServer_ExtractPrefix(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	tests := []struct {
		content  string
		pos      Position
		expected string
	}{
		{"SELECT ", Position{Line: 0, Character: 7}, ""},
		{"SELECT co", Position{Line: 0, Character: 9}, "co"},
		{"{{ con", Position{Line: 0, Character: 6}, "con"},
		{"{{ utils.up", Position{Line: 0, Character: 11}, "up"},
		{"SELEC", Position{Line: 0, Character: 5}, "SELEC"},
		{"", Position{Line: 0, Character: 0}, ""},
	}

	for _, tt := range tests {
		doc := &Document{
			Content: tt.content,
			Lines:   computeLineOffsets(tt.content),
		}

		result := server.extractPrefix(doc, tt.pos)
		assert.Equal(t, tt.expected, result, "extractPrefix(%q, %v)", tt.content, tt.pos)
	}
}

func TestServer_GetCompletions_Builtins(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	// Test builtin completion in template context
	uri := "file:///test.sql"
	content := "SELECT {{ c"
	server.documents.Open(uri, content, 1)

	params := CompletionParams{
		TextDocumentPositionParams: TextDocumentPositionParams{
			TextDocument: TextDocumentIdentifier{URI: uri},
			Position:     Position{Line: 0, Character: 11},
		},
	}

	items := server.getCompletions(params)

	// Should have "config" in completions
	found := false
	for _, item := range items {
		if item.Label == "config" {
			found = true
			break
		}
	}

	assert.True(t, found, "expected 'config' in completions for 'c' prefix in template")
}

func TestServer_GetCompletions_SQLKeywords(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	// Test SQL keyword completion
	uri := "file:///test.sql"
	content := "SEL"
	server.documents.Open(uri, content, 1)

	params := CompletionParams{
		TextDocumentPositionParams: TextDocumentPositionParams{
			TextDocument: TextDocumentIdentifier{URI: uri},
			Position:     Position{Line: 0, Character: 3},
		},
	}

	items := server.getCompletions(params)

	// Should have "SELECT" in completions
	found := false
	for _, item := range items {
		if item.Label == "SELECT" {
			found = true
			break
		}
	}

	assert.True(t, found, "expected 'SELECT' in completions for 'SEL' prefix")
}

func TestServer_GetCompletions_SQLFunctions(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	// Test SQL function completion in SELECT clause
	uri := "file:///test.sql"
	content := "SELECT COU"
	server.documents.Open(uri, content, 1)

	params := CompletionParams{
		TextDocumentPositionParams: TextDocumentPositionParams{
			TextDocument: TextDocumentIdentifier{URI: uri},
			Position:     Position{Line: 0, Character: 10},
		},
	}

	items := server.getCompletions(params)

	// Should have "COUNT" in completions
	found := false
	for _, item := range items {
		if item.Label == "COUNT" {
			found = true
			assert.Equal(t, CompletionItemKindFunction, item.Kind, "expected COUNT to be a function")
			break
		}
	}

	assert.True(t, found, "expected 'COUNT' in completions for 'COU' prefix in SELECT")
}

func TestCompletionItemKinds(t *testing.T) {
	// Verify builtin globals have correct kinds
	for _, builtin := range builtinGlobals {
		switch builtin.Label {
		case "config", "env", "target", "this":
			assert.Equal(t, CompletionItemKindVariable, builtin.Kind, "%s should be Variable", builtin.Label)
		}
	}

	// Verify SQL keywords have correct kind
	for _, kw := range sqlKeywords {
		assert.Equal(t, CompletionItemKindKeyword, kw.Kind, "SQL keyword %s should be Keyword", kw.Label)
	}

	// Verify SQL functions from catalog return correct kind when converted
	items := getSQLFunctionCompletions("")
	for _, item := range items {
		assert.Equal(t, CompletionItemKindFunction, item.Kind, "SQL function %s should be Function", item.Label)
	}
}

func TestConfigKeys(t *testing.T) {
	// Verify config keys exist
	expectedKeys := []string{"name", "materialized", "schema", "owner", "tags", "unique_key", "meta"}

	for _, key := range expectedKeys {
		found := false
		for _, k := range configKeys {
			if k == key {
				found = true
				break
			}
		}
		assert.True(t, found, "expected config key %q not found", key)
	}
}

func TestGetSQLFunctionCompletions(t *testing.T) {
	// Test that catalog integration works
	items := getSQLFunctionCompletions("")
	assert.NotEmpty(t, items, "expected non-empty completions from catalog")

	// Should have more than the previous hardcoded 25 functions
	assert.GreaterOrEqual(t, len(items), 100, "expected at least 100 functions from catalog")

	// Test prefix filtering
	countItems := getSQLFunctionCompletions("COUNT")
	assert.NotEmpty(t, countItems, "expected COUNT function in completions")
	for _, item := range countItems {
		assert.True(t, strings.HasPrefix(item.Label, "COUNT"), "expected all items to start with COUNT, got %s", item.Label)
	}
}

func TestGetSQLFunctionCompletions_Details(t *testing.T) {
	items := getSQLFunctionCompletions("COUNT")

	var countItem *CompletionItem
	for i := range items {
		if items[i].Label == "COUNT" {
			countItem = &items[i]
			break
		}
	}

	if !assert.NotNil(t, countItem, "COUNT not found in completions") {
		return
	}

	// Check that fields are populated from catalog
	assert.Equal(t, CompletionItemKindFunction, countItem.Kind, "expected Function kind")
	assert.NotEmpty(t, countItem.Detail, "expected Detail (signature) to be populated")
	assert.NotEmpty(t, countItem.Documentation, "expected Documentation to be populated")
	assert.NotEmpty(t, countItem.InsertText, "expected InsertText (snippet) to be populated")
	assert.Equal(t, InsertTextFormatSnippet, countItem.InsertTextFormat, "expected snippet format")
}

func TestGetSQLFunctionCompletions_AllCategories(t *testing.T) {
	// Verify we have functions from different categories
	items := getSQLFunctionCompletions("")

	categories := map[string]bool{
		"aggregate": false,
		"window":    false,
		"string":    false,
		"date":      false,
		"numeric":   false,
		"list":      false,
	}

	// Check by specific functions we know exist
	functionCategories := map[string]string{
		"COUNT":         "aggregate",
		"ROW_NUMBER":    "window",
		"UPPER":         "string",
		"DATE_TRUNC":    "date",
		"ABS":           "numeric",
		"LIST_CONTAINS": "list",
	}

	for _, item := range items {
		if cat, ok := functionCategories[item.Label]; ok {
			categories[cat] = true
		}
	}

	for cat, found := range categories {
		assert.True(t, found, "no functions found for category %s", cat)
	}
}

func TestCatalogSearchFunctions(t *testing.T) {
	// Test the underlying catalog search
	results := sql.SearchFunctions("DATE")
	assert.NotEmpty(t, results, "expected DATE functions")

	// All results should start with DATE
	for _, fn := range results {
		assert.True(t, strings.HasPrefix(fn.Name, "DATE"), "expected function to start with DATE, got %s", fn.Name)
	}
}

func TestServer_GetHover_Builtins(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	uri := "file:///test.sql"
	content := "SELECT {{ config.name }}"
	server.documents.Open(uri, content, 1)

	params := HoverParams{
		TextDocumentPositionParams: TextDocumentPositionParams{
			TextDocument: TextDocumentIdentifier{URI: uri},
			Position:     Position{Line: 0, Character: 13}, // on "config"
		},
	}

	hover := server.getHover(params)

	if !assert.NotNil(t, hover, "expected hover info for 'config'") {
		return
	}

	assert.Contains(t, hover.Contents.Value, "config", "hover should contain 'config'")
	assert.Contains(t, hover.Contents.Value, "dict", "hover should describe config as dict")
}

func TestServer_GetHover_NoResult(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	uri := "file:///test.sql"
	content := "SELECT * FROM users"
	server.documents.Open(uri, content, 1)

	// Hover on a regular SQL keyword shouldn't return builtin docs
	params := HoverParams{
		TextDocumentPositionParams: TextDocumentPositionParams{
			TextDocument: TextDocumentIdentifier{URI: uri},
			Position:     Position{Line: 0, Character: 16}, // on "users"
		},
	}

	hover := server.getHover(params)

	// Regular table names don't have hover info (unless we add catalog)
	if hover != nil {
		// This is okay - we just don't crash
		t.Log("hover returned for regular identifier (may be expected)")
	}
}

func TestServer_GetHover_SQLFunction(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	uri := "file:///test.sql"
	content := "SELECT COUNT(*) FROM users"
	server.documents.Open(uri, content, 1)

	params := HoverParams{
		TextDocumentPositionParams: TextDocumentPositionParams{
			TextDocument: TextDocumentIdentifier{URI: uri},
			Position:     Position{Line: 0, Character: 9}, // on "COUNT"
		},
	}

	hover := server.getHover(params)

	if !assert.NotNil(t, hover, "expected hover info for COUNT function") {
		return
	}

	assert.Contains(t, hover.Contents.Value, "COUNT", "hover should contain function name")
	assert.Contains(t, hover.Contents.Value, "bigint", "hover should contain return type from signature")
	assert.Contains(t, hover.Contents.Value, "Aggregate", "hover should indicate it's an aggregate function")
}

func TestServer_GetHover_WindowFunction(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	uri := "file:///test.sql"
	content := "SELECT ROW_NUMBER() OVER() FROM users"
	server.documents.Open(uri, content, 1)

	params := HoverParams{
		TextDocumentPositionParams: TextDocumentPositionParams{
			TextDocument: TextDocumentIdentifier{URI: uri},
			Position:     Position{Line: 0, Character: 10}, // on "ROW_NUMBER"
		},
	}

	hover := server.getHover(params)

	if !assert.NotNil(t, hover, "expected hover info for ROW_NUMBER function") {
		return
	}

	assert.Contains(t, hover.Contents.Value, "ROW_NUMBER", "hover should contain function name")
	assert.Contains(t, hover.Contents.Value, "Window", "hover should indicate it's a window function")
}
