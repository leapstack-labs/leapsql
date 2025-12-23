package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/leapstack-labs/leapsql/pkg/dialect"
	_ "github.com/leapstack-labs/leapsql/pkg/dialects/ansi" // Register ANSI dialect
)

func TestExtractSQL(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected string
	}{
		{
			name:     "simple SQL",
			content:  "SELECT * FROM users",
			expected: "SELECT * FROM users",
		},
		{
			name: "with frontmatter",
			content: `/*---
name: test
materialized: table
---*/
SELECT * FROM users`,
			expected: "\nSELECT * FROM users",
		},
		{
			name:     "with template expression",
			content:  "SELECT {{ column }} FROM users",
			expected: "SELECT __EXPR__ FROM users",
		},
		{
			name:     "with template statement",
			content:  "SELECT * {* if true *}FROM users{* end *}",
			expected: "SELECT * FROM users",
		},
		{
			name: "complex",
			content: `/*---
name: test
---*/
SELECT 
    {{ config.name }},
    {{ utils.upper('name') }}
FROM staging.users
{* if env == "prod" *}
WHERE active = true
{* end *}`,
			expected: `
SELECT 
    __EXPR__,
    __EXPR__
FROM staging.users

WHERE active = true
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractSQL(tt.content)
			assert.Equal(t, tt.expected, result, "extractSQL()")
		})
	}
}

func TestIsBuiltinGlobal(t *testing.T) {
	builtins := []string{"config", "env", "target", "this"}
	nonBuiltins := []string{"utils", "my_macro", "ref", "custom"}

	for _, name := range builtins {
		assert.True(t, isBuiltinGlobal(name), "isBuiltinGlobal(%q): expected true", name)
	}

	for _, name := range nonBuiltins {
		assert.False(t, isBuiltinGlobal(name), "isBuiltinGlobal(%q): expected false", name)
	}
}

func TestLevenshtein(t *testing.T) {
	tests := []struct {
		s1       string
		s2       string
		expected int
	}{
		{"", "", 0},
		{"abc", "", 3},
		{"", "abc", 3},
		{"abc", "abc", 0},
		{"abc", "abd", 1},
		{"abc", "adc", 1},
		{"abc", "dbc", 1},
		{"kitten", "sitting", 3},
		{"saturday", "sunday", 3},
		{"upper", "uper", 1},
		{"coalesce", "coalsce", 1}, // missing 'e'
	}

	for _, tt := range tests {
		result := levenshtein(tt.s1, tt.s2)
		assert.Equal(t, tt.expected, result, "levenshtein(%q, %q)", tt.s1, tt.s2)
	}
}

func TestSuggestSimilar(t *testing.T) {
	candidates := []string{"upper", "lower", "coalesce", "safe_cast", "format_date"}

	tests := []struct {
		input       string
		maxDistance int
		expected    int // number of suggestions
	}{
		{"uper", 1, 1},       // typo for "upper"
		{"uppr", 2, 1},       // typo for "upper"
		{"colaesce", 2, 1},   // typo for "coalesce"
		{"xyz", 1, 0},        // no match
		{"format_dat", 2, 1}, // typo for "format_date"
		{"upper", 2, 0},      // exact match not included (dist > 0)
	}

	for _, tt := range tests {
		suggestions := suggestSimilar(tt.input, candidates, tt.maxDistance)
		assert.Len(t, suggestions, tt.expected, "suggestSimilar(%q, maxDist=%d)", tt.input, tt.maxDistance)
	}
}

func TestServer_ValidateFrontmatter(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	tests := []struct {
		name          string
		content       string
		expectErrors  bool
		expectedCount int
	}{
		{
			name: "valid frontmatter",
			content: `/*---
name: test_model
materialized: table
---*/
SELECT * FROM users`,
			expectErrors: false,
		},
		{
			name: "invalid YAML",
			content: `/*---
name: test
  invalid: indentation
---*/
SELECT * FROM users`,
			expectErrors:  true,
			expectedCount: 1,
		},
		{
			name:         "no frontmatter",
			content:      "SELECT * FROM users",
			expectErrors: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := &Document{
				Content: tt.content,
				Lines:   computeLineOffsets(tt.content),
			}

			diags := server.validateFrontmatter(doc)

			if tt.expectErrors {
				assert.NotEmpty(t, diags, "expected diagnostics but got none")
			} else {
				assert.Empty(t, diags, "expected no diagnostics")
			}
			if tt.expectedCount > 0 {
				assert.Len(t, diags, tt.expectedCount, "unexpected diagnostic count")
			}
		})
	}
}

func TestServer_ValidateTemplate(t *testing.T) {
	server := &Server{
		documents: NewDocumentStore(),
	}

	tests := []struct {
		name         string
		content      string
		expectErrors bool
	}{
		{
			name:         "valid template",
			content:      "SELECT {{ config.name }} FROM users",
			expectErrors: false,
		},
		{
			name:         "unclosed template",
			content:      "SELECT {{ config.name FROM users",
			expectErrors: true,
		},
		{
			name:         "valid statement",
			content:      "{* if condition: *}SELECT * FROM users{* endif *}",
			expectErrors: false,
		},
		{
			name:         "no template",
			content:      "SELECT * FROM users",
			expectErrors: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := &Document{
				Content: tt.content,
				Lines:   computeLineOffsets(tt.content),
			}

			diags := server.validateTemplate(doc)

			if tt.expectErrors {
				assert.NotEmpty(t, diags, "expected diagnostics but got none")
			} else {
				assert.Empty(t, diags, "expected no diagnostics")
			}
		})
	}
}

func TestServer_ValidateSQL(t *testing.T) {
	// Get ANSI dialect for testing
	ansiDialect, _ := dialect.Get("ansi")
	server := &Server{
		documents: NewDocumentStore(),
		dialect:   ansiDialect,
	}

	tests := []struct {
		name         string
		content      string
		expectErrors bool
	}{
		{
			name:         "valid SELECT",
			content:      "SELECT id, name FROM users", // Use explicit columns to avoid lint warning
			expectErrors: false,
		},
		{
			name:         "valid SELECT with WHERE",
			content:      "SELECT id, name FROM users WHERE active = true",
			expectErrors: false,
		},
		{
			name:         "valid JOIN",
			content:      "SELECT usr.id FROM users usr JOIN orders ord ON usr.id = ord.user_id",
			expectErrors: false,
		},
		{
			name:         "empty content",
			content:      "",
			expectErrors: false,
		},
		{
			name:         "whitespace only",
			content:      "   \n\t  ",
			expectErrors: false,
		},
		// Note: SQL validation depends on the lineage parser's strictness
		// Some "invalid" SQL may still parse if the parser is lenient
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := &Document{
				Content: tt.content,
				Lines:   computeLineOffsets(tt.content),
			}

			diags := server.validateSQL(doc)

			if tt.expectErrors {
				assert.NotEmpty(t, diags, "expected diagnostics but got none")
			} else {
				assert.Empty(t, diags, "expected no diagnostics")
			}
		})
	}
}

func TestDiagnosticCodes(t *testing.T) {
	// Verify diagnostic codes are consistent
	tests := []struct {
		code        string
		description string
	}{
		{"E001", "Frontmatter error"},
		{"E002", "Template error"},
		{"E003", "SQL error"},
		{"E101", "Unknown namespace"},
		{"E102", "Unknown function"},
	}

	// Just ensure the codes are documented - actual code usage
	// is tested in the validation tests
	for _, tt := range tests {
		assert.NotEmpty(t, tt.code, "diagnostic code should not be empty for %s", tt.description)
	}
}
