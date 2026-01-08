package provider

import (
	"regexp"
	"strings"
	"time"

	"github.com/leapstack-labs/leapsql/internal/loader"
	"github.com/leapstack-labs/leapsql/internal/template"
	"github.com/leapstack-labs/leapsql/pkg/core"
	pkgparser "github.com/leapstack-labs/leapsql/pkg/parser"
)

// ParsedDocument holds all parse results for a single file.
// This eliminates redundant parsing by caching all intermediate results.
type ParsedDocument struct {
	URI     string
	Version int
	Content string

	// Frontmatter parsing result
	Frontmatter      *loader.FrontmatterResult
	FrontmatterError error
	FrontmatterEnd   int // Byte offset where frontmatter ends

	// Template parsing result
	Template      *template.Template
	TemplateError error
	SQLContent    string // Content with templates replaced

	// SQL parsing result
	SQL      *core.SelectStmt
	SQLError error

	// Metadata
	ParsedAt time.Time
}

// Parse creates a ParsedDocument from content, performing all parse phases once.
func Parse(content string, uri string, version int, d *core.Dialect) *ParsedDocument {
	doc := &ParsedDocument{
		URI:      uri,
		Version:  version,
		Content:  content,
		ParsedAt: time.Now(),
	}

	// Phase 1: Extract frontmatter
	fm, err := loader.ExtractFrontmatter(content)
	doc.Frontmatter = fm
	doc.FrontmatterError = err

	// Determine where frontmatter ends for content extraction
	if idx := strings.Index(content, "---*/"); idx != -1 {
		doc.FrontmatterEnd = idx + 5
	}

	// Phase 2: Parse template (content after frontmatter)
	templateContent := content
	if doc.FrontmatterEnd > 0 {
		templateContent = content[doc.FrontmatterEnd:]
	}

	tmpl, err := template.ParseString(templateContent, uri)
	doc.Template = tmpl
	doc.TemplateError = err

	// Phase 3: Extract and parse SQL
	doc.SQLContent = extractSQL(content, doc.FrontmatterEnd)

	if strings.TrimSpace(doc.SQLContent) != "" && d != nil {
		stmt, err := pkgparser.ParseWithDialect(doc.SQLContent, d)
		doc.SQL = stmt
		doc.SQLError = err
	}

	return doc
}

// extractSQL extracts SQL content from a model file, handling frontmatter and templates.
func extractSQL(content string, frontmatterEnd int) string {
	// Skip frontmatter
	if frontmatterEnd > 0 && frontmatterEnd < len(content) {
		content = content[frontmatterEnd:]
	} else {
		// Try to detect frontmatter if frontmatterEnd wasn't provided
		if idx := strings.Index(content, "/*---"); idx != -1 {
			if endIdx := strings.Index(content, "---*/"); endIdx != -1 {
				content = content[endIdx+5:]
			}
		}
	}

	// Replace template expressions with placeholders
	// {{ expr }} -> __EXPR__
	content = regexp.MustCompile(`\{\{[^}]+\}\}`).ReplaceAllString(content, "__EXPR__")

	// Remove template statements {* ... *}
	content = regexp.MustCompile(`\{\*[^*]*\*\}`).ReplaceAllString(content, "")

	return content
}

// HasFrontmatterError returns true if frontmatter parsing failed.
func (d *ParsedDocument) HasFrontmatterError() bool {
	return d.FrontmatterError != nil
}

// HasTemplateError returns true if template parsing failed.
func (d *ParsedDocument) HasTemplateError() bool {
	return d.TemplateError != nil
}

// HasSQLError returns true if SQL parsing failed.
func (d *ParsedDocument) HasSQLError() bool {
	return d.SQLError != nil
}

// HasAnyError returns true if any parsing phase failed.
func (d *ParsedDocument) HasAnyError() bool {
	return d.HasFrontmatterError() || d.HasTemplateError() || d.HasSQLError()
}

// IsValid returns true if the document was parsed successfully (SQL may have errors).
func (d *ParsedDocument) IsValid() bool {
	return !d.HasFrontmatterError() && !d.HasTemplateError()
}

// AllErrors returns all parse errors.
func (d *ParsedDocument) AllErrors() []error {
	var errs []error
	if d.FrontmatterError != nil {
		errs = append(errs, d.FrontmatterError)
	}
	if d.TemplateError != nil {
		errs = append(errs, d.TemplateError)
	}
	if d.SQLError != nil {
		errs = append(errs, d.SQLError)
	}
	return errs
}
