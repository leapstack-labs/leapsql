package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_BasicSQL(t *testing.T) {
	content := `SELECT id, name FROM users WHERE id = 1`
	doc := Parse(content, "test.sql", 1, nil)

	require.NotNil(t, doc)
	assert.Equal(t, "test.sql", doc.URI)
	assert.Equal(t, 1, doc.Version)
	assert.Equal(t, content, doc.Content)
	assert.NoError(t, doc.FrontmatterError)
	assert.NoError(t, doc.TemplateError)
	// SQL won't parse without dialect, that's expected
}

func TestParse_WithFrontmatter(t *testing.T) {
	content := `/*---
name: my_model
materialized: table
---*/
SELECT id, name FROM users`

	doc := Parse(content, "test.sql", 1, nil)

	require.NotNil(t, doc)
	require.NotNil(t, doc.Frontmatter)
	assert.True(t, doc.Frontmatter.HasYAML)
	assert.Equal(t, "my_model", doc.Frontmatter.Config.Name)
	assert.Equal(t, "table", doc.Frontmatter.Config.Materialized)
	assert.NoError(t, doc.FrontmatterError)
}

func TestParse_InvalidFrontmatter(t *testing.T) {
	content := `/*---
invalid_field: value
---*/
SELECT id FROM users`

	doc := Parse(content, "test.sql", 1, nil)

	require.NotNil(t, doc)
	require.Error(t, doc.FrontmatterError)
	assert.True(t, doc.HasFrontmatterError())
}

func TestParse_WithTemplate(t *testing.T) {
	content := `/*---
name: my_model
---*/
SELECT id, {{ utils.column_name() }} FROM users`

	doc := Parse(content, "test.sql", 1, nil)

	require.NotNil(t, doc)
	assert.NoError(t, doc.FrontmatterError)
	assert.NoError(t, doc.TemplateError)
	assert.Contains(t, doc.SQLContent, "__EXPR__")
}

func TestParse_SQLContentExtraction(t *testing.T) {
	content := `/*---
name: my_model
---*/
SELECT id, {{ expr }} FROM users
{* this is a comment *}
WHERE status = 'active'`

	doc := Parse(content, "test.sql", 1, nil)

	require.NotNil(t, doc)
	// Template expressions should be replaced
	assert.Contains(t, doc.SQLContent, "__EXPR__")
	// Template statements should be removed
	assert.NotContains(t, doc.SQLContent, "{*")
	// SQL content should be preserved
	assert.Contains(t, doc.SQLContent, "SELECT")
	assert.Contains(t, doc.SQLContent, "WHERE")
}

func TestParsedDocument_HasErrors(t *testing.T) {
	tests := []struct {
		name           string
		content        string
		hasFrontmatter bool
		hasTemplate    bool
		hasSQL         bool
	}{
		{
			name:    "valid content",
			content: "SELECT 1",
		},
		{
			name:           "invalid frontmatter",
			content:        "/*---\nbad_field: x\n---*/\nSELECT 1",
			hasFrontmatter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.content, "test.sql", 1, nil)

			assert.Equal(t, tt.hasFrontmatter, doc.HasFrontmatterError())
			assert.Equal(t, tt.hasTemplate, doc.HasTemplateError())
			// SQL errors depend on dialect, skip checking
		})
	}
}

func TestParsedDocument_AllErrors(t *testing.T) {
	content := `/*---
unknown_field: value
---*/
SELECT 1`

	doc := Parse(content, "test.sql", 1, nil)

	errs := doc.AllErrors()
	require.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "unknown")
}

func TestExtractSQL_NoFrontmatter(t *testing.T) {
	content := "SELECT * FROM users"
	result := extractSQL(content, 0)

	assert.Equal(t, content, result)
}

func TestExtractSQL_WithFrontmatter(t *testing.T) {
	content := `/*---
name: test
---*/
SELECT * FROM users`

	result := extractSQL(content, 0)

	assert.Contains(t, result, "SELECT")
	assert.NotContains(t, result, "name: test")
}

func TestExtractSQL_WithTemplates(t *testing.T) {
	content := "SELECT {{ column }}, {* comment *} FROM users"
	result := extractSQL(content, 0)

	assert.Contains(t, result, "__EXPR__")
	assert.NotContains(t, result, "{{")
	assert.NotContains(t, result, "{*")
}
