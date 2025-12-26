package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// generateGlobalsDocs generates template globals documentation.
func generateGlobalsDocs(outDir string) error {
	log.Printf("Generating globals docs to %s", outDir)

	// Create output directory
	if err := os.MkdirAll(outDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Read existing globals.md and append generated section
	globalsPath := filepath.Clean(filepath.Join(outDir, "globals.md"))

	// Check if file exists
	existingContent, err := os.ReadFile(globalsPath) //#nosec G304 -- path is constructed from trusted config
	if err != nil {
		// File doesn't exist, generate full file
		return generateFullGlobalsDoc(globalsPath)
	}

	// File exists, check if it already has generated section
	content := string(existingContent)
	if strings.Contains(content, generatedHeader) {
		// Replace existing generated section
		return updateGlobalsDoc(globalsPath, content)
	}

	// Append generated section
	return appendGlobalsDoc(globalsPath, content)
}

// GlobalProperty represents a property of a global object.
type GlobalProperty struct {
	Name        string
	Type        string
	Description string
}

// GlobalObject represents a global object available in templates.
type GlobalObject struct {
	Name        string
	Description string
	Properties  []GlobalProperty
}

// getGlobalsSchema returns the template globals schema.
// Based on internal/starlark/types.go and builtins.go
func getGlobalsSchema() []GlobalObject {
	return []GlobalObject{
		{
			Name:        "target",
			Description: "Information about the current database target.",
			Properties: []GlobalProperty{
				{Name: "target.type", Type: "string", Description: "Database type (duckdb, postgres, snowflake)"},
				{Name: "target.schema", Type: "string", Description: "Default schema for the target"},
				{Name: "target.database", Type: "string", Description: "Database name"},
			},
		},
		{
			Name:        "this",
			Description: "Information about the current model being rendered.",
			Properties: []GlobalProperty{
				{Name: "this.name", Type: "string", Description: "Model name"},
				{Name: "this.schema", Type: "string", Description: "Model schema"},
			},
		},
		{
			Name:        "env",
			Description: "Environment name as a string (e.g., \"dev\", \"staging\", \"prod\").",
			Properties:  nil, // env is a simple string, not an object
		},
		{
			Name:        "config",
			Description: "Parsed frontmatter as a dictionary. Access any field defined in the model's frontmatter.",
			Properties:  nil, // config is dynamic based on frontmatter
		},
	}
}

// generateGlobalsReferenceSection generates the reference section markdown.
func generateGlobalsReferenceSection() string {
	w := NewMarkdownWriter()

	w.Header(2, "Reference")
	w.GeneratedMarker()

	globals := getGlobalsSchema()

	for _, g := range globals {
		w.Header(3, InlineCode(g.Name))
		w.Paragraph(g.Description)

		if len(g.Properties) > 0 {
			headers := []string{"Property", "Type", "Description"}
			var rows [][]string
			for _, p := range g.Properties {
				rows = append(rows, []string{
					InlineCode(p.Name),
					p.Type,
					p.Description,
				})
			}
			w.Table(headers, rows)
		}
	}

	// Add usage examples
	w.Header(3, "Usage Examples")
	w.CodeBlock("sql", `-- Access target information
SELECT * FROM {{ target.schema }}.customers

-- Check target type for conditional SQL
{* if target.type == "duckdb" *}
-- DuckDB-specific syntax
{* else *}
-- Standard SQL
{* endif *}

-- Access current model information
SELECT '{{ this.name }}' as model_name, *
FROM source_table

-- Access frontmatter config
SELECT '{{ config.owner }}' as owner
WHERE '{{ config.materialized }}' = 'table'`)

	return w.String()
}

// generateFullGlobalsDoc generates a complete globals.md file.
func generateFullGlobalsDoc(filepath string) error {
	w := NewMarkdownWriter()

	// Frontmatter
	w.Frontmatter("Global Variables", "Built-in variables available in LeapSQL templates")
	w.GeneratedMarker()

	// Title and intro
	w.Header(1, "Global Variables")
	w.Paragraph("LeapSQL provides several global variables that are available in all templates. These let you access environment configuration, model metadata, and more.")

	// Overview table
	w.Header(2, "Available Globals")
	headers := []string{"Variable", "Type", "Description"}
	rows := [][]string{
		{InlineCode("target"), "object", "Current target environment"},
		{InlineCode("this"), "object", "Current model metadata"},
		{InlineCode("env"), "string", "Environment name"},
		{InlineCode("config"), "dict", "Parsed frontmatter configuration"},
		{InlineCode("ref()"), "function", "Reference another model"},
		{InlineCode("source()"), "function", "Reference a source table"},
		{InlineCode("var()"), "function", "Get variables with defaults"},
	}
	w.Table(headers, rows)

	// Reference section
	w.Text(generateGlobalsReferenceSection())

	return os.WriteFile(filepath, w.Bytes(), 0600)
}

// updateGlobalsDoc updates the generated section in an existing file.
func updateGlobalsDoc(filepath, content string) error {
	// Find the start of the generated section
	markerIdx := strings.Index(content, "## Reference")
	if markerIdx == -1 {
		// No Reference section, append
		return appendGlobalsDoc(filepath, content)
	}

	// Keep everything before Reference section and append new generated content
	newContent := strings.TrimSpace(content[:markerIdx]) + "\n\n" + generateGlobalsReferenceSection()

	return os.WriteFile(filepath, []byte(newContent), 0600)
}

// appendGlobalsDoc appends the generated reference section to an existing file.
func appendGlobalsDoc(filepath, content string) error {
	newContent := strings.TrimSpace(content) + "\n\n" + generateGlobalsReferenceSection()
	return os.WriteFile(filepath, []byte(newContent), 0600)
}
