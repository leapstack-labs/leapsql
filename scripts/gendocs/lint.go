package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/project/rules"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules"
)

// sqlGroupDescriptions provides human-readable descriptions for SQL rule groups.
var sqlGroupDescriptions = map[string]string{
	"aliasing":   "Rules about alias usage and naming conventions.",
	"ambiguous":  "Rules about ambiguous SQL constructs that may cause confusion or errors.",
	"convention": "Rules about SQL coding conventions and style consistency.",
	"references": "Rules about column and table references in queries.",
	"structure":  "Rules about SQL query structure and organization.",
}

// projectGroupDescriptions provides human-readable descriptions for project rule groups.
var projectGroupDescriptions = map[string]string{
	"modeling":  "Rules about model structure and DAG organization.",
	"lineage":   "Rules about data lineage and column dependencies.",
	"structure": "Rules about project structure and naming conventions.",
}

// generateLintDocs generates all lint documentation files.
func generateLintDocs(outDir string) error {
	log.Printf("Generating lint docs to %s", outDir)

	if err := os.MkdirAll(outDir, 0750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get all rules
	sqlRules := lint.GetAllSQLRules()
	projectRules := lint.GetAllProjectRules()

	// Generate index page
	if err := generateLintIndex(outDir, len(sqlRules), len(projectRules)); err != nil {
		return err
	}
	log.Printf("  Generated index.md")

	// Generate SQL rules page
	if err := generateSQLRulesPage(outDir, sqlRules); err != nil {
		return err
	}
	log.Printf("  Generated sql-rules.md")

	// Generate project rules page
	if err := generateProjectRulesPage(outDir, projectRules); err != nil {
		return err
	}
	log.Printf("  Generated project-rules.md")

	return nil
}

// generateLintIndex generates the main linting overview page.
func generateLintIndex(outDir string, sqlCount, projectCount int) error {
	w := NewMarkdownWriter()

	w.Frontmatter("Linting", "SQL and project lint rules for LeapSQL")
	w.GeneratedMarker()

	w.Header(1, "Linting")
	w.Paragraph(fmt.Sprintf("LeapSQL includes a comprehensive linter with **%d SQL rules** and **%d project rules**.", sqlCount, projectCount))

	w.Header(2, "Rule Types")
	w.BulletList([]string{
		Bold("SQL Rules") + ": Analyze individual SQL statements for style, correctness, and best practices",
		Bold("Project Rules") + ": Analyze DAG structure, model organization, and data lineage",
	})

	w.Header(2, "Severity Levels")
	w.Table(
		[]string{"Severity", "Description"},
		[][]string{
			{InlineCode("error"), "Critical issue that should be fixed"},
			{InlineCode("warning"), "Potential issue that should be reviewed"},
			{InlineCode("info"), "Informational feedback"},
			{InlineCode("hint"), "Suggestion for improvement"},
		},
	)

	w.Header(2, "Configuration")
	w.Paragraph("Rules can be configured in `leapsql.yaml`:")
	w.CodeBlock("yaml", `lint:
  rules:
    AM01: off              # disable rule
    AL06:
      severity: error      # override severity
      max_length: 30       # rule-specific option`)

	w.Header(2, "Rule Categories")

	w.Header(3, "SQL Rules")
	w.Table(
		[]string{"Category", "Prefix", "Description"},
		[][]string{
			{"[Aliasing](/linting/sql-rules#aliasing)", "AL", "Alias usage and naming"},
			{"[Ambiguous](/linting/sql-rules#ambiguous)", "AM", "Ambiguous SQL constructs"},
			{"[Convention](/linting/sql-rules#convention)", "CV", "SQL coding conventions"},
			{"[References](/linting/sql-rules#references)", "RF", "Column and table references"},
			{"[Structure](/linting/sql-rules#structure)", "ST", "Query structure"},
		},
	)

	w.Header(3, "Project Rules")
	w.Table(
		[]string{"Category", "Prefix", "Description"},
		[][]string{
			{"[Modeling](/linting/project-rules#modeling)", "PM", "Model structure and organization"},
			{"[Lineage](/linting/project-rules#lineage)", "PL", "Data lineage and dependencies"},
			{"[Structure](/linting/project-rules#structure)", "PS", "Project structure and naming"},
		},
	)

	return os.WriteFile(filepath.Join(outDir, "index.md"), w.Bytes(), 0600)
}

// generateSQLRulesPage generates the SQL rules documentation page.
func generateSQLRulesPage(outDir string, rules []lint.SQLRule) error {
	w := NewMarkdownWriter()

	w.Frontmatter("SQL Lint Rules", "SQL statement analysis rules for LeapSQL")
	w.GeneratedMarker()

	w.Header(1, "SQL Lint Rules")
	w.Paragraph(fmt.Sprintf("LeapSQL includes %d SQL lint rules organized into 5 categories.", len(rules)))

	// Group rules by their group
	grouped := groupSQLRulesByGroup(rules)

	// Define group order
	groupOrder := []string{"aliasing", "ambiguous", "convention", "references", "structure"}

	for _, group := range groupOrder {
		groupRules, ok := grouped[group]
		if !ok || len(groupRules) == 0 {
			continue
		}

		// Write group header with anchor
		w.Line(fmt.Sprintf("## %s {#%s}", capitalizeFirst(group), group))
		w.Newline()

		desc, ok := sqlGroupDescriptions[group]
		if ok {
			w.Paragraph(desc)
		}

		// Write each rule with full documentation
		for _, rule := range groupRules {
			writeRuleDoc(w, rule)
		}
	}

	return os.WriteFile(filepath.Join(outDir, "sql-rules.md"), w.Bytes(), 0600)
}

// generateProjectRulesPage generates the project rules documentation page.
func generateProjectRulesPage(outDir string, rules []lint.ProjectRule) error {
	w := NewMarkdownWriter()

	w.Frontmatter("Project Lint Rules", "Project-level analysis rules for LeapSQL")
	w.GeneratedMarker()

	w.Header(1, "Project Lint Rules")
	w.Paragraph(fmt.Sprintf("LeapSQL includes %d project lint rules organized into 3 categories.", len(rules)))

	// Group rules by their group
	grouped := groupProjectRulesByGroup(rules)

	// Define group order
	groupOrder := []string{"modeling", "lineage", "structure"}

	for _, group := range groupOrder {
		groupRules, ok := grouped[group]
		if !ok || len(groupRules) == 0 {
			continue
		}

		// Write group header with anchor
		w.Line(fmt.Sprintf("## %s {#%s}", capitalizeFirst(group), group))
		w.Newline()

		desc, ok := projectGroupDescriptions[group]
		if ok {
			w.Paragraph(desc)
		}

		// Write each rule with full documentation
		for _, rule := range groupRules {
			writeRuleDoc(w, rule)
		}
	}

	return os.WriteFile(filepath.Join(outDir, "project-rules.md"), w.Bytes(), 0600)
}

// groupSQLRulesByGroup organizes SQL rules by their Group field.
func groupSQLRulesByGroup(rules []lint.SQLRule) map[string][]lint.SQLRule {
	grouped := make(map[string][]lint.SQLRule)
	for _, r := range rules {
		grouped[r.Group()] = append(grouped[r.Group()], r)
	}
	// Sort rules within each group by ID
	for group := range grouped {
		sort.Slice(grouped[group], func(i, j int) bool {
			return grouped[group][i].ID() < grouped[group][j].ID()
		})
	}
	return grouped
}

// groupProjectRulesByGroup organizes project rules by their Group field.
func groupProjectRulesByGroup(rules []lint.ProjectRule) map[string][]lint.ProjectRule {
	grouped := make(map[string][]lint.ProjectRule)
	for _, r := range rules {
		grouped[r.Group()] = append(grouped[r.Group()], r)
	}
	// Sort rules within each group by ID
	for group := range grouped {
		sort.Slice(grouped[group], func(i, j int) bool {
			return grouped[group][i].ID() < grouped[group][j].ID()
		})
	}
	return grouped
}

// capitalizeFirst capitalizes the first letter of a string.
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// writeRuleDoc writes detailed documentation for a single rule.
func writeRuleDoc(w *MarkdownWriter, rule lint.Rule) {
	// Rule header with anchor: ### AM01 - ambiguous.distinct {#AM01}
	w.Line(fmt.Sprintf("### %s - %s {#%s}", rule.ID(), rule.Name(), rule.ID()))
	w.Newline()

	// Severity badge and description
	w.Line(fmt.Sprintf("**Severity:** %s", InlineCode(rule.DefaultSeverity().String())))
	w.Newline()

	w.Paragraph(cleanDescription(rule.Description()))

	// Rationale (if available)
	if rationale := rule.Rationale(); rationale != "" {
		w.Header(4, "Why This Matters")
		w.Paragraph(strings.TrimSpace(rationale))
	}

	// Bad example (if available)
	if badExample := rule.BadExample(); badExample != "" {
		w.Header(4, "Bad")
		w.CodeBlock("sql", badExample)
	}

	// Good example (if available)
	if goodExample := rule.GoodExample(); goodExample != "" {
		w.Header(4, "Good")
		w.CodeBlock("sql", goodExample)
	}

	// Fix (if available)
	if fix := rule.Fix(); fix != "" {
		w.Header(4, "How to Fix")
		w.Paragraph(strings.TrimSpace(fix))
	}

	// Config keys (if available)
	if configKeys := rule.ConfigKeys(); len(configKeys) > 0 {
		w.Header(4, "Configuration")
		w.Paragraph(fmt.Sprintf("This rule accepts the following configuration options: %s",
			InlineCode(strings.Join(configKeys, ", "))))
	}

	// Dialect restrictions for SQL rules
	if sqlRule, ok := rule.(lint.SQLRule); ok {
		if dialects := sqlRule.Dialects(); len(dialects) > 0 {
			w.Line(fmt.Sprintf("**Dialects:** %s", strings.Join(dialects, ", ")))
			w.Newline()
		}
	}

	// Horizontal rule between rules for readability
	w.Line("---")
	w.Newline()
}
