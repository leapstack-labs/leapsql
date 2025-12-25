package lsp

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/loader"
	"github.com/leapstack-labs/leapsql/internal/provider"
	"github.com/leapstack-labs/leapsql/internal/template"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/rules" // Register SQLFluff-style lint rules
	pkgparser "github.com/leapstack-labs/leapsql/pkg/parser"
)

// publishDiagnostics parses the document and publishes any errors.
// Uses the shared provider for caching to avoid redundant parsing.
func (s *Server) publishDiagnostics(uri string) {
	doc := s.documents.Get(uri)
	if doc == nil {
		return
	}

	var diagnostics []Diagnostic

	// Only process SQL files
	if !strings.HasSuffix(uri, ".sql") {
		s.sendNotification("textDocument/publishDiagnostics", &PublishDiagnosticsParams{
			URI:         uri,
			Diagnostics: diagnostics,
		})
		return
	}

	// Use provider for cached parsing (single parse instead of 3x)
	if s.provider != nil {
		parsed := s.provider.GetOrParse(uri, doc.Content, doc.Version)
		diagnostics = s.getDiagnosticsFromParsed(uri, parsed, doc)
	} else {
		// Fallback to direct parsing if provider not initialized
		diagnostics = s.getDiagnosticsLegacy(doc)
	}

	// Validate macro references (if store available)
	if s.store != nil {
		macroDiags := s.validateMacroReferences(doc)
		diagnostics = append(diagnostics, macroDiags...)
	}

	s.sendNotification("textDocument/publishDiagnostics", &PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

// getDiagnosticsFromParsed extracts diagnostics from a cached ParsedDocument.
func (s *Server) getDiagnosticsFromParsed(uri string, parsed *provider.ParsedDocument, _ *Document) []Diagnostic {
	var diagnostics []Diagnostic

	// 1. Frontmatter errors
	if parsed.FrontmatterError != nil {
		diagnostics = append(diagnostics, s.frontmatterErrorToDiagnostic(parsed.FrontmatterError)...)
	}

	// 2. Template errors
	if parsed.TemplateError != nil {
		diagnostics = append(diagnostics, s.templateErrorToDiagnostic(parsed.TemplateError)...)
	}

	// 3. SQL parse errors
	if parsed.SQLError != nil {
		diagnostics = append(diagnostics, s.sqlErrorToDiagnostic(parsed.SQLError)...)
	}

	// 4. Run lint rules if SQL parsed successfully
	if parsed.SQL != nil {
		lintDiags := s.runLinter(uri, parsed.SQL)
		diagnostics = append(diagnostics, lintDiags...)
	}

	return diagnostics
}

// getDiagnosticsLegacy performs direct parsing without caching (fallback).
func (s *Server) getDiagnosticsLegacy(doc *Document) []Diagnostic {
	var diagnostics []Diagnostic

	// 1. Check frontmatter
	frontmatterDiags := s.validateFrontmatter(doc)
	diagnostics = append(diagnostics, frontmatterDiags...)

	// 2. Check template syntax
	templateDiags := s.validateTemplate(doc)
	diagnostics = append(diagnostics, templateDiags...)

	// 3. Check SQL syntax
	sqlDiags := s.validateSQL(doc)
	diagnostics = append(diagnostics, sqlDiags...)

	return diagnostics
}

// frontmatterErrorToDiagnostic converts a frontmatter error to LSP diagnostic.
func (s *Server) frontmatterErrorToDiagnostic(err error) []Diagnostic {
	var pos Position
	var msg string

	var parseErr *loader.FrontmatterParseError
	var unknownErr *loader.UnknownFieldError

	switch {
	case errors.As(err, &parseErr):
		pos = Position{Line: uint32(parseErr.Line), Character: 0} //nolint:gosec // G115: line is always non-negative from parser
		msg = parseErr.Message
	case errors.As(err, &unknownErr):
		msg = fmt.Sprintf("Unknown frontmatter field: %s", unknownErr.Field)
		pos = Position{Line: 0, Character: 0}
	default:
		msg = err.Error()
		pos = Position{Line: 0, Character: 0}
	}

	return []Diagnostic{{
		Range: Range{
			Start: pos,
			End:   Position{Line: pos.Line, Character: 1000},
		},
		Severity: DiagnosticSeverityError,
		Code:     "E001",
		Source:   "leapsql",
		Message:  msg,
	}}
}

// templateErrorToDiagnostic converts a template error to LSP diagnostic.
func (s *Server) templateErrorToDiagnostic(err error) []Diagnostic {
	var pos Position
	var msg string

	var te template.Error
	if errors.As(err, &te) {
		tpos := te.Position()
		pos = Position{Line: uint32(tpos.Line - 1), Character: uint32(tpos.Column - 1)} //nolint:gosec // G115: line/column are always non-negative
		msg = err.Error()
	} else {
		msg = err.Error()
	}

	return []Diagnostic{{
		Range: Range{
			Start: pos,
			End:   Position{Line: pos.Line, Character: pos.Character + 10},
		},
		Severity: DiagnosticSeverityError,
		Code:     "E002",
		Source:   "leapsql",
		Message:  "Template error: " + msg,
	}}
}

// sqlErrorToDiagnostic converts a SQL parse error to LSP diagnostic.
func (s *Server) sqlErrorToDiagnostic(err error) []Diagnostic {
	var pe *pkgparser.ParseError
	if errors.As(err, &pe) {
		return []Diagnostic{{
			Range: Range{
				Start: Position{Line: uint32(pe.Pos.Line - 1), Character: uint32(pe.Pos.Column - 1)},  //nolint:gosec // G115: line/column are always non-negative
				End:   Position{Line: uint32(pe.Pos.Line - 1), Character: uint32(pe.Pos.Column + 10)}, //nolint:gosec // G115: line/column are always non-negative
			},
			Severity: DiagnosticSeverityError,
			Code:     "E003",
			Source:   "leapsql",
			Message:  pe.Message,
		}}
	}

	return []Diagnostic{{
		Range: Range{
			Start: Position{Line: 0, Character: 0},
			End:   Position{Line: 0, Character: 10},
		},
		Severity: DiagnosticSeverityError,
		Code:     "E003",
		Source:   "leapsql",
		Message:  "SQL error: " + err.Error(),
	}}
}

// validateFrontmatter checks YAML frontmatter syntax.
func (s *Server) validateFrontmatter(doc *Document) []Diagnostic {
	var diagnostics []Diagnostic

	_, err := loader.ExtractFrontmatter(doc.Content)
	if err != nil {
		// Try to get position from error
		var pos Position
		var msg string

		var parseErr *loader.FrontmatterParseError
		var unknownErr *loader.UnknownFieldError

		switch {
		case errors.As(err, &parseErr):
			pos = Position{Line: uint32(parseErr.Line), Character: 0} //nolint:gosec // G115: line is always non-negative from parser
			msg = parseErr.Message
		case errors.As(err, &unknownErr):
			// Unknown field - try to find it in the content
			msg = fmt.Sprintf("Unknown frontmatter field: %s", unknownErr.Field)
			pos = Position{Line: 0, Character: 0}
		default:
			msg = err.Error()
			pos = Position{Line: 0, Character: 0}
		}

		diagnostics = append(diagnostics, Diagnostic{
			Range: Range{
				Start: pos,
				End:   Position{Line: pos.Line, Character: 1000},
			},
			Severity: DiagnosticSeverityError,
			Code:     "E001",
			Source:   "leapsql",
			Message:  msg,
		})
	}

	return diagnostics
}

// validateTemplate checks template syntax ({{ }} and {* *}).
func (s *Server) validateTemplate(doc *Document) []Diagnostic {
	var diagnostics []Diagnostic

	// Extract content after frontmatter
	content := doc.Content
	if idx := strings.Index(content, "---*/"); idx != -1 {
		content = content[idx+5:]
	}

	_, err := template.ParseString(content, "")
	if err != nil {
		var pos Position
		var msg string

		// Check if the error implements the template.Error interface
		var te template.Error
		if errors.As(err, &te) {
			tpos := te.Position()
			pos = Position{Line: uint32(tpos.Line - 1), Character: uint32(tpos.Column - 1)} //nolint:gosec // G115: line/column are always non-negative
			msg = err.Error()
		} else {
			msg = err.Error()
		}

		diagnostics = append(diagnostics, Diagnostic{
			Range: Range{
				Start: pos,
				End:   Position{Line: pos.Line, Character: pos.Character + 10},
			},
			Severity: DiagnosticSeverityError,
			Code:     "E002",
			Source:   "leapsql",
			Message:  "Template error: " + msg,
		})
	}

	return diagnostics
}

// validateSQL checks SQL syntax using the dialect-aware parser.
func (s *Server) validateSQL(doc *Document) []Diagnostic {
	var diagnostics []Diagnostic

	// Extract SQL content (skip frontmatter, simplify templates)
	sqlContent := extractSQL(doc.Content)
	if strings.TrimSpace(sqlContent) == "" {
		return diagnostics
	}

	// Always use dialect-aware parsing (s.dialect is never nil after initialization)
	stmt, err := pkgparser.ParseWithDialect(sqlContent, s.dialect)
	if err != nil {
		var pe *pkgparser.ParseError
		if errors.As(err, &pe) {
			diagnostics = append(diagnostics, Diagnostic{
				Range: Range{
					Start: Position{Line: uint32(pe.Pos.Line - 1), Character: uint32(pe.Pos.Column - 1)},  //nolint:gosec // G115: line/column are always non-negative
					End:   Position{Line: uint32(pe.Pos.Line - 1), Character: uint32(pe.Pos.Column + 10)}, //nolint:gosec // G115: line/column are always non-negative
				},
				Severity: DiagnosticSeverityError,
				Code:     "E003",
				Source:   "leapsql",
				Message:  pe.Message,
			})
		} else {
			diagnostics = append(diagnostics, Diagnostic{
				Range: Range{
					Start: Position{Line: 0, Character: 0},
					End:   Position{Line: 0, Character: 10},
				},
				Severity: DiagnosticSeverityError,
				Code:     "E003",
				Source:   "leapsql",
				Message:  "SQL error: " + err.Error(),
			})
		}
	}

	// Run lint rules if statement parsed successfully (even if there were parser warnings)
	if stmt != nil {
		lintDiags := s.runLinter(doc.URI, stmt)
		diagnostics = append(diagnostics, lintDiags...)
	}

	return diagnostics
}

// validateMacroReferences checks that macro namespaces and functions exist.
func (s *Server) validateMacroReferences(doc *Document) []Diagnostic {
	var diagnostics []Diagnostic

	// Find all {{ namespace.function() }} patterns
	macroCallPattern := regexp.MustCompile(`\{\{\s*(\w+)\.(\w+)\s*\(`)
	matches := macroCallPattern.FindAllStringSubmatchIndex(doc.Content, -1)

	for _, match := range matches {
		if len(match) < 6 {
			continue
		}

		namespace := doc.Content[match[2]:match[3]]
		funcName := doc.Content[match[4]:match[5]]

		// Skip builtin globals
		if isBuiltinGlobal(namespace) {
			continue
		}

		// Check namespace exists
		s.cacheMu.RLock()
		nsExists := s.macroNamespaceCache[namespace]
		s.cacheMu.RUnlock()

		startPos := doc.OffsetToPosition(match[2])
		endPos := doc.OffsetToPosition(match[5])

		if !nsExists {
			diagnostics = append(diagnostics, Diagnostic{
				Range: Range{
					Start: startPos,
					End:   Position{Line: startPos.Line, Character: startPos.Character + uint32(len(namespace))}, //nolint:gosec // G115: len is always non-negative
				},
				Severity: DiagnosticSeverityError,
				Code:     "E101",
				Source:   "leapsql",
				Message:  fmt.Sprintf("Unknown namespace '%s'", namespace),
			})
			continue
		}

		// Check function exists in namespace
		exists, _ := s.store.MacroFunctionExists(namespace, funcName)
		if !exists {
			// Get available functions for suggestion
			functions, _ := s.store.GetMacroFunctions(namespace)
			funcNames := make([]string, len(functions))
			for i, f := range functions {
				funcNames[i] = f.Name
			}

			msg := fmt.Sprintf("Unknown function '%s' in namespace '%s'", funcName, namespace)
			if suggestions := suggestSimilar(funcName, funcNames, 2); len(suggestions) > 0 {
				msg += fmt.Sprintf(". Did you mean '%s'?", suggestions[0])
			}

			diagnostics = append(diagnostics, Diagnostic{
				Range: Range{
					Start: Position{Line: startPos.Line, Character: startPos.Character + uint32(len(namespace)) + 1}, //nolint:gosec // G115: len is always non-negative
					End:   endPos,
				},
				Severity: DiagnosticSeverityError,
				Code:     "E102",
				Source:   "leapsql",
				Message:  msg,
			})
		}
	}

	return diagnostics
}

// extractSQL extracts SQL content from a model file, handling frontmatter and templates.
func extractSQL(content string) string {
	// Remove frontmatter
	if idx := strings.Index(content, "/*---"); idx != -1 {
		if endIdx := strings.Index(content, "---*/"); endIdx != -1 {
			content = content[endIdx+5:]
		}
	}

	// Replace template expressions with placeholders
	// {{ expr }} -> __EXPR__
	content = regexp.MustCompile(`\{\{[^}]+\}\}`).ReplaceAllString(content, "__EXPR__")

	// Remove template statements {* ... *}
	content = regexp.MustCompile(`\{\*[^*]*\*\}`).ReplaceAllString(content, "")

	return content
}

// isBuiltinGlobal returns true for reserved Starlark globals.
func isBuiltinGlobal(name string) bool {
	switch name {
	case "config", "env", "target", "this":
		return true
	}
	return false
}

// suggestSimilar finds similar strings using Levenshtein distance.
func suggestSimilar(input string, candidates []string, maxDistance int) []string {
	inputLower := strings.ToLower(input)
	var suggestions []string

	for _, candidate := range candidates {
		dist := levenshtein(inputLower, strings.ToLower(candidate))
		if dist <= maxDistance && dist > 0 {
			suggestions = append(suggestions, candidate)
		}
	}

	return suggestions
}

// levenshtein calculates the Levenshtein distance between two strings.
func levenshtein(s1, s2 string) int {
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	// Create distance matrix
	matrix := make([][]int, len(s1)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(s2)+1)
		matrix[i][0] = i
	}
	for j := 0; j <= len(s2); j++ {
		matrix[0][j] = j
	}

	// Fill in the rest of the matrix
	for i := 1; i <= len(s1); i++ {
		for j := 1; j <= len(s2); j++ {
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
			}
			matrix[i][j] = min(
				matrix[i-1][j]+1,      // deletion
				matrix[i][j-1]+1,      // insertion
				matrix[i-1][j-1]+cost, // substitution
			)
		}
	}

	return matrix[len(s1)][len(s2)]
}

// runLinter runs lint rules against a parsed SQL statement.
func (s *Server) runLinter(uri string, stmt *pkgparser.SelectStmt) []Diagnostic {
	// Use analyzer with registry to get SQLFluff-style rules in addition to dialect rules
	analyzer := lint.NewAnalyzerWithRegistry(lint.NewConfig(), s.dialect.GetName())
	lintDiags := analyzer.Analyze(stmt, s.dialect)

	// Convert lint.Diagnostic to LSP Diagnostic
	var result []Diagnostic
	for _, d := range lintDiags {
		// Determine end position - use EndPos if available, otherwise estimate
		endLine := d.EndPos.Line
		endCol := d.EndPos.Column
		if endLine == 0 && endCol == 0 {
			// Fallback: estimate end position
			endLine = d.Pos.Line
			endCol = d.Pos.Column + 10
		}

		diag := Diagnostic{
			Range: Range{
				Start: Position{
					Line:      uint32(max(0, d.Pos.Line-1)),   //nolint:gosec // G115: line is always non-negative
					Character: uint32(max(0, d.Pos.Column-1)), //nolint:gosec // G115: column is always non-negative
				},
				End: Position{
					Line:      uint32(max(0, endLine-1)), //nolint:gosec // G115: line is always non-negative
					Character: uint32(max(0, endCol-1)),  //nolint:gosec // G115: column is always non-negative
				},
			},
			Severity: toLSPSeverity(d.Severity),
			Code:     d.RuleID,
			Source:   "leapsql-lint",
			Message:  d.Message,
		}

		// Add documentation URL if available
		if d.DocumentationURL != "" {
			diag.CodeDescription = &CodeDescription{Href: d.DocumentationURL}
		} else {
			// Generate default documentation URL
			diag.CodeDescription = &CodeDescription{Href: lint.BuildDocURL(d.RuleID)}
		}

		result = append(result, diag)
	}

	// Cache fixes for code actions
	s.cacheDiagnosticFixes(uri, lintDiags)

	return result
}

// toLSPSeverity converts lint.Severity to LSP DiagnosticSeverity.
func toLSPSeverity(s lint.Severity) DiagnosticSeverity {
	switch s {
	case lint.SeverityError:
		return DiagnosticSeverityError
	case lint.SeverityWarning:
		return DiagnosticSeverityWarning
	case lint.SeverityInfo:
		return DiagnosticSeverityInformation
	case lint.SeverityHint:
		return DiagnosticSeverityHint
	default:
		return DiagnosticSeverityWarning
	}
}

// runProjectHealthDiagnostics runs project-level lint rules and returns diagnostics grouped by file.
func (s *Server) runProjectHealthDiagnostics() map[string][]Diagnostic {
	// Use provider's cached project context if available
	var ctx *project.Context
	if s.provider != nil {
		ctx = s.provider.GetProjectContext()
	} else {
		ctx = s.buildProjectContext()
	}
	if ctx == nil {
		return nil
	}

	diags := s.projectAnalyzer.Analyze(ctx)
	if len(diags) == 0 {
		return nil
	}

	// Group diagnostics by file
	byFile := make(map[string][]Diagnostic)
	for _, d := range diags {
		filePath := d.FilePath
		if filePath == "" {
			continue // Skip diagnostics without file paths
		}

		lspDiag := Diagnostic{
			Range: Range{
				// Project health diagnostics typically apply to the whole file
				Start: Position{Line: 0, Character: 0},
				End:   Position{Line: 0, Character: 100},
			},
			Severity: projectSeverityToLSP(d.Severity),
			Code:     d.RuleID,
			Source:   "leapsql-project-health",
			Message:  d.Message,
		}
		byFile[filePath] = append(byFile[filePath], lspDiag)
	}

	return byFile
}

// projectSeverityToLSP converts lint.Severity to LSP DiagnosticSeverity.
func projectSeverityToLSP(s lint.Severity) DiagnosticSeverity {
	switch s {
	case lint.SeverityError:
		return DiagnosticSeverityError
	case lint.SeverityWarning:
		return DiagnosticSeverityWarning
	case lint.SeverityInfo:
		return DiagnosticSeverityInformation
	case lint.SeverityHint:
		return DiagnosticSeverityHint
	default:
		return DiagnosticSeverityWarning
	}
}

// publishProjectHealthDiagnostics runs project health analysis and publishes diagnostics.
func (s *Server) publishProjectHealthDiagnostics() {
	diagsByFile := s.runProjectHealthDiagnostics()
	if diagsByFile == nil {
		return
	}

	for filePath, diags := range diagsByFile {
		// Convert file path to URI
		uri := PathToURI(filePath)

		// Get existing file-level diagnostics (if any)
		doc := s.documents.Get(uri)
		if doc != nil {
			// Merge with existing diagnostics by re-running publish for this file
			s.publishDiagnosticsWithProjectHealth(uri, diags)
		} else {
			// File not open, just publish project health diagnostics
			s.sendNotification("textDocument/publishDiagnostics", &PublishDiagnosticsParams{
				URI:         uri,
				Diagnostics: diags,
			})
		}
	}
}

// publishDiagnosticsWithProjectHealth publishes diagnostics for a file including project health.
func (s *Server) publishDiagnosticsWithProjectHealth(uri string, projectDiags []Diagnostic) {
	doc := s.documents.Get(uri)
	if doc == nil {
		return
	}

	var diagnostics []Diagnostic

	// Only process SQL files for file-level diagnostics
	if strings.HasSuffix(uri, ".sql") {
		// Use provider for cached parsing (single parse instead of 3x)
		if s.provider != nil {
			parsed := s.provider.GetOrParse(uri, doc.Content, doc.Version)
			diagnostics = s.getDiagnosticsFromParsed(uri, parsed, doc)
		} else {
			diagnostics = s.getDiagnosticsLegacy(doc)
		}

		// Validate macro references
		if s.store != nil {
			macroDiags := s.validateMacroReferences(doc)
			diagnostics = append(diagnostics, macroDiags...)
		}
	}

	// Add project health diagnostics
	diagnostics = append(diagnostics, projectDiags...)

	s.sendNotification("textDocument/publishDiagnostics", &PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}
