package commands

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/project/rules" // register project rules
	_ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules"     // register SQL rules
	"github.com/spf13/cobra"
)

// RulesOptions holds options for the rules command.
type RulesOptions struct {
	Group   string // Filter by group
	Type    string // Filter by type: sql, project
	Verbose bool   // Show full documentation
	Format  string // Output format
}

// NewRulesCommand creates the rules command.
func NewRulesCommand() *cobra.Command {
	opts := &RulesOptions{}
	cmd := &cobra.Command{
		Use:   "rules [rule-id]",
		Short: "List available lint rules",
		Long: `List all available lint rules with their documentation.

Rules are organized by type (SQL or project) and group (e.g., aliasing, modeling).
Use --verbose to see full documentation including examples and fix guidance.

Output adapts to environment:
  - Terminal: Styled output with colors
  - Piped/Scripted: Markdown format
  - JSON: Machine-readable format`,
		Example: `  # List all rules
  leapsql rules

  # Show details for a specific rule
  leapsql rules AM01

  # List SQL rules only
  leapsql rules --type sql

  # List rules in the modeling group
  leapsql rules --group modeling

  # Show full documentation
  leapsql rules -V

  # Output as JSON
  leapsql rules --format json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return showRule(cmd, args[0], opts)
			}
			return listRules(cmd, opts)
		},
	}

	cmd.Flags().StringVarP(&opts.Group, "group", "g", "", "Filter by group")
	cmd.Flags().StringVar(&opts.Type, "type", "", "Filter by type: sql, project")
	cmd.Flags().BoolVarP(&opts.Verbose, "verbose", "V", false, "Show full documentation")
	cmd.Flags().StringVarP(&opts.Format, "format", "f", "", "Output format: text, json, markdown")

	return cmd
}

func listRules(cmd *cobra.Command, opts *RulesOptions) error {
	cmdCtx := NewCommandContextWithoutEngine(cmd)
	r := cmdCtx.Renderer

	// Override renderer if format flag is set
	if opts.Format != "" {
		r = output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), output.Mode(opts.Format))
	}

	// Get all rules
	rules := lint.AllRules()

	// Apply filters
	rules = filterRulesByOptions(rules, opts)

	// Sort by type, then group, then ID
	sort.Slice(rules, func(i, j int) bool {
		if rules[i].Type != rules[j].Type {
			return rules[i].Type < rules[j].Type
		}
		if rules[i].Group != rules[j].Group {
			return rules[i].Group < rules[j].Group
		}
		return rules[i].ID < rules[j].ID
	})

	mode := r.EffectiveMode()
	switch mode {
	case output.ModeJSON:
		return listRulesJSON(r, rules)
	case output.ModeMarkdown:
		return listRulesMarkdown(r, rules, opts.Verbose)
	default:
		return listRulesText(r, rules, opts.Verbose)
	}
}

func filterRulesByOptions(rules []core.RuleInfo, opts *RulesOptions) []core.RuleInfo {
	if opts.Group == "" && opts.Type == "" {
		return rules
	}

	var filtered []core.RuleInfo
	for _, r := range rules {
		if opts.Group != "" && r.Group != opts.Group {
			continue
		}
		if opts.Type != "" && r.Type != opts.Type {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}

func showRule(cmd *cobra.Command, ruleID string, opts *RulesOptions) error {
	cmdCtx := NewCommandContextWithoutEngine(cmd)
	r := cmdCtx.Renderer

	// Override renderer if format flag is set
	if opts.Format != "" {
		r = output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), output.Mode(opts.Format))
	}

	// Find the rule
	rules := lint.AllRules()
	var rule *core.RuleInfo
	for _, ri := range rules {
		if ri.ID == ruleID {
			rule = &ri
			break
		}
	}

	if rule == nil {
		return fmt.Errorf("rule %q not found", ruleID)
	}

	mode := r.EffectiveMode()
	switch mode {
	case output.ModeJSON:
		return showRuleJSON(r, rule)
	case output.ModeMarkdown:
		return showRuleMarkdown(r, rule)
	default:
		return showRuleText(r, rule)
	}
}

// listRulesText outputs rules in styled text format.
func listRulesText(r *output.Renderer, rules []core.RuleInfo, verbose bool) error {
	styles := r.Styles()

	// Count by type
	sqlCount, projectCount := 0, 0
	for _, rule := range rules {
		if rule.Type == "sql" {
			sqlCount++
		} else {
			projectCount++
		}
	}

	r.Println("")
	r.Println(styles.Header1.Render(fmt.Sprintf("Lint Rules (%d SQL, %d Project)", sqlCount, projectCount)))
	r.Println("")

	currentType := ""
	currentGroup := ""

	for _, rule := range rules {
		// Type header
		if rule.Type != currentType {
			currentType = rule.Type
			currentGroup = ""
			typeLabel := "SQL Rules"
			if currentType == "project" {
				typeLabel = "Project Rules"
			}
			r.Println(styles.Header2.Render(typeLabel))
			r.Println("")
		}

		// Group header
		if rule.Group != currentGroup {
			currentGroup = rule.Group
			r.Println(styles.Bold.Render("  " + capitalizeFirst(currentGroup)))
		}

		// Rule line
		severityStyle := getSeverityStyle(styles, rule.DefaultSeverity)
		r.Printf("    %s  %s - %s\n",
			styles.Muted.Render(rule.ID),
			rule.Name,
			severityStyle.Render(rule.DefaultSeverity.String()),
		)

		if verbose {
			r.Println(styles.Muted.Render("        " + rule.Description))
			if rule.Rationale != "" {
				r.Println(styles.Muted.Render("        Why: " + truncateOneLine(rule.Rationale, 80)))
			}
			r.Println("")
		}
	}

	r.Println("")
	r.Println(styles.Muted.Render("Use 'leapsql rules <rule-id>' for detailed documentation"))
	r.Println("")

	return nil
}

// listRulesMarkdown outputs rules in markdown format.
func listRulesMarkdown(r *output.Renderer, rules []core.RuleInfo, verbose bool) error {
	r.Println("# Lint Rules")
	r.Println("")

	currentType := ""
	currentGroup := ""

	for _, rule := range rules {
		if rule.Type != currentType {
			currentType = rule.Type
			currentGroup = ""
			typeLabel := "SQL Rules"
			if currentType == "project" {
				typeLabel = "Project Rules"
			}
			r.Println("## " + typeLabel)
			r.Println("")
		}

		if rule.Group != currentGroup {
			currentGroup = rule.Group
			r.Println("### " + capitalizeFirst(currentGroup))
			r.Println("")
		}

		r.Printf("- **%s** - %s (`%s`)\n", rule.ID, rule.Name, rule.DefaultSeverity.String())
		if verbose {
			r.Println("  " + rule.Description)
			if rule.Rationale != "" {
				r.Println("  > " + rule.Rationale)
			}
		}
	}

	r.Println("")
	return nil
}

// RulesJSONOutput is the JSON output structure for rules listing.
type RulesJSONOutput struct {
	Rules []core.RuleInfo `json:"rules"`
	Count struct {
		SQL     int `json:"sql"`
		Project int `json:"project"`
		Total   int `json:"total"`
	} `json:"count"`
}

// listRulesJSON outputs rules in JSON format.
func listRulesJSON(r *output.Renderer, rules []core.RuleInfo) error {
	jsonOutput := RulesJSONOutput{
		Rules: rules,
	}

	for _, rule := range rules {
		if rule.Type == "sql" {
			jsonOutput.Count.SQL++
		} else {
			jsonOutput.Count.Project++
		}
	}
	jsonOutput.Count.Total = len(rules)

	enc := json.NewEncoder(r.Writer())
	enc.SetIndent("", "  ")
	return enc.Encode(jsonOutput)
}

// showRuleText displays detailed rule info in text format.
func showRuleText(r *output.Renderer, rule *core.RuleInfo) error {
	styles := r.Styles()

	r.Println("")
	r.Println(styles.Header1.Render(fmt.Sprintf("%s - %s", rule.ID, rule.Name)))
	r.Println("")

	r.Printf("  %s: %s\n", styles.Bold.Render("Type"), rule.Type)
	r.Printf("  %s: %s\n", styles.Bold.Render("Group"), rule.Group)
	r.Printf("  %s: %s\n", styles.Bold.Render("Severity"), rule.DefaultSeverity.String())
	r.Println("")

	r.Println(styles.Bold.Render("Description"))
	r.Println("  " + rule.Description)
	r.Println("")

	if rule.Rationale != "" {
		r.Println(styles.Bold.Render("Why This Matters"))
		r.Println("  " + rule.Rationale)
		r.Println("")
	}

	if rule.BadExample != "" {
		r.Println(styles.Bold.Render("Bad Example"))
		for _, line := range strings.Split(rule.BadExample, "\n") {
			r.Println(styles.Muted.Render("  " + line))
		}
		r.Println("")
	}

	if rule.GoodExample != "" {
		r.Println(styles.Bold.Render("Good Example"))
		for _, line := range strings.Split(rule.GoodExample, "\n") {
			r.Println(styles.Success.Render("  " + line))
		}
		r.Println("")
	}

	if rule.Fix != "" {
		r.Println(styles.Bold.Render("How to Fix"))
		r.Println("  " + rule.Fix)
		r.Println("")
	}

	if len(rule.ConfigKeys) > 0 {
		r.Println(styles.Bold.Render("Configuration"))
		r.Printf("  Options: %s\n", strings.Join(rule.ConfigKeys, ", "))
		r.Println("")
	}

	if len(rule.Dialects) > 0 {
		r.Printf("  %s: %s\n", styles.Bold.Render("Dialects"), strings.Join(rule.Dialects, ", "))
	}

	return nil
}

// showRuleMarkdown displays detailed rule info in markdown format.
func showRuleMarkdown(r *output.Renderer, rule *core.RuleInfo) error {
	r.Printf("# %s - %s\n\n", rule.ID, rule.Name)
	r.Printf("**Type:** %s | **Group:** %s | **Severity:** `%s`\n\n", rule.Type, rule.Group, rule.DefaultSeverity.String())
	r.Println(rule.Description)
	r.Println("")

	if rule.Rationale != "" {
		r.Println("## Why This Matters")
		r.Println("")
		r.Println(rule.Rationale)
		r.Println("")
	}

	if rule.BadExample != "" {
		r.Println("## Bad Example")
		r.Println("")
		r.Println("```sql")
		r.Println(rule.BadExample)
		r.Println("```")
		r.Println("")
	}

	if rule.GoodExample != "" {
		r.Println("## Good Example")
		r.Println("")
		r.Println("```sql")
		r.Println(rule.GoodExample)
		r.Println("```")
		r.Println("")
	}

	if rule.Fix != "" {
		r.Println("## How to Fix")
		r.Println("")
		r.Println(rule.Fix)
		r.Println("")
	}

	if len(rule.ConfigKeys) > 0 {
		r.Println("## Configuration")
		r.Println("")
		r.Printf("Options: `%s`\n", strings.Join(rule.ConfigKeys, "`, `"))
		r.Println("")
	}

	return nil
}

// showRuleJSON displays detailed rule info in JSON format.
func showRuleJSON(r *output.Renderer, rule *core.RuleInfo) error {
	enc := json.NewEncoder(r.Writer())
	enc.SetIndent("", "  ")
	return enc.Encode(rule)
}

// Helper functions

func getSeverityStyle(styles *output.Styles, sev core.Severity) lipgloss.Style {
	switch sev {
	case core.SeverityError:
		return styles.Error
	case core.SeverityWarning:
		return styles.Warning
	case core.SeverityInfo:
		return styles.Info
	default:
		return styles.Muted
	}
}

func truncateOneLine(s string, maxLen int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
