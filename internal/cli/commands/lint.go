package commands

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/internal/cli/config"
	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	intparser "github.com/leapstack-labs/leapsql/internal/parser"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/rules" // register rules
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/spf13/cobra"
)

// LintOptions holds options for the lint command.
type LintOptions struct {
	Path     string   // File or directory path
	Format   string   // Output format: text, json
	Disable  []string // Rule IDs to disable
	Severity string   // Minimum severity: error, warning, info, hint
	Rules    []string // Run only specific rules
}

// NewLintCommand creates the lint command.
func NewLintCommand() *cobra.Command {
	opts := &LintOptions{}
	cmd := &cobra.Command{
		Use:   "lint [path]",
		Short: "Run lint rules on SQL models",
		Long: `Analyze SQL models for potential issues.

Runs SQLFluff-style lint rules against your SQL models and reports
any violations found. Rules can be configured in leapsql.yaml.

Output adapts to environment:
  - Terminal: Styled output with colors
  - Piped/Scripted: Markdown format
  - JSON: Machine-readable format`,
		Example: `  # Lint all models
  leapsql lint

  # Lint specific path
  leapsql lint ./models/staging

  # Output as JSON
  leapsql lint --format json

  # Disable specific rules
  leapsql lint --disable AM01,ST01

  # Only report errors (ignore warnings/hints)
  leapsql lint --severity error`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				opts.Path = args[0]
			}
			return runLint(cmd, opts)
		},
	}

	cmd.Flags().StringVarP(&opts.Format, "format", "f", "", "Output format: text, json")
	cmd.Flags().StringSliceVar(&opts.Disable, "disable", nil, "Rule IDs to disable")
	cmd.Flags().StringVar(&opts.Severity, "severity", "warning", "Minimum severity: error, warning, info, hint")
	cmd.Flags().StringSliceVar(&opts.Rules, "rule", nil, "Run only specific rules")

	return cmd
}

func runLint(cmd *cobra.Command, opts *LintOptions) error {
	cmdCtx, cleanup, err := NewCommandContext(cmd)
	if err != nil {
		return err
	}
	defer cleanup()

	eng := cmdCtx.Engine
	cfg := cmdCtx.Cfg
	r := cmdCtx.Renderer

	// Override renderer if format flag is set
	if opts.Format != "" {
		r = output.NewRenderer(cmd.OutOrStdout(), cmd.ErrOrStderr(), output.Mode(opts.Format))
	}

	// Discover models
	if _, err := eng.Discover(engine.DiscoveryOptions{}); err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	// Build lint config from CLI flags + project config
	lintCfg := buildLintConfig(cfg, opts)

	// Get dialect - it's set after engine initialization (before DB connection)
	d := eng.GetDialect()
	if d == nil {
		return fmt.Errorf("dialect not available")
	}

	// Create analyzer with registry
	analyzer := lint.NewAnalyzerWithRegistry(lintCfg, d.Name)

	// Filter models by path if specified
	models := filterModelsByPath(eng.GetModels(), opts.Path)

	// Analyze each model
	results := analyzeModels(models, analyzer, d, eng)

	// Filter by severity threshold
	results = filterBySeverity(results, opts.Severity)

	// Render output
	hasIssues := renderLintResults(r, results)

	// Exit with code 1 if issues found
	if hasIssues {
		return fmt.Errorf("lint issues found")
	}
	return nil
}

func buildLintConfig(cfg *config.Config, opts *LintOptions) *lint.Config {
	lintCfg := lint.NewConfig()

	// Apply project config first (lower precedence)
	if cfg != nil && cfg.Lint != nil {
		projectLint := cfg.Lint
		// Apply disabled rules from project config
		for _, id := range projectLint.Disabled {
			lintCfg.Disable(strings.TrimSpace(id))
		}
		// Apply severity overrides from project config
		for id, sev := range projectLint.Severity {
			if s, ok := lint.ParseSeverity(sev); ok {
				lintCfg.SetSeverity(id, s)
			}
		}
		// Apply rule-specific options from project config
		for id, ruleOpts := range projectLint.Rules {
			lintCfg.SetRuleOptions(id, ruleOpts)
		}
	}

	// Apply CLI overrides (higher precedence)
	for _, id := range opts.Disable {
		lintCfg.Disable(strings.TrimSpace(id))
	}

	// If --rule specified, disable all others
	if len(opts.Rules) > 0 {
		enabledSet := make(map[string]bool)
		for _, id := range opts.Rules {
			enabledSet[strings.TrimSpace(id)] = true
		}
		for _, rule := range lint.GetAll() {
			if !enabledSet[rule.ID] {
				lintCfg.Disable(rule.ID)
			}
		}
	}

	return lintCfg
}

// lintFileResult holds lint results for a single file.
type lintFileResult struct {
	Path        string
	Diagnostics []lint.Diagnostic
}

func filterModelsByPath(models map[string]*intparser.ModelConfig, pathFilter string) []*intparser.ModelConfig {
	result := make([]*intparser.ModelConfig, 0, len(models))

	if pathFilter == "" {
		// Return all models
		for _, m := range models {
			result = append(result, m)
		}
		return result
	}

	// Normalize path filter
	pathFilter = filepath.Clean(pathFilter)

	for _, m := range models {
		// Match by model path or file path
		modelPath := filepath.Clean(m.Path)
		filePath := filepath.Clean(m.FilePath)

		if strings.HasPrefix(modelPath, pathFilter) ||
			strings.HasPrefix(filePath, pathFilter) ||
			modelPath == pathFilter ||
			filePath == pathFilter {
			result = append(result, m)
		}
	}
	return result
}

func analyzeModels(models []*intparser.ModelConfig, analyzer *lint.Analyzer, d lint.DialectInfo, eng *engine.Engine) []lintFileResult {
	var results []lintFileResult

	for _, m := range models {
		// Render and parse the model
		rendered, err := eng.RenderModel(m.Path)
		if err != nil {
			continue // Skip models that fail to render
		}

		stmt, err := parser.ParseWithDialect(rendered, eng.GetDialect())
		if err != nil {
			continue // Skip models that fail to parse
		}

		// Analyze using registry rules
		diags := analyzer.AnalyzeWithRegistryRules(stmt, d)
		if len(diags) > 0 {
			results = append(results, lintFileResult{
				Path:        m.FilePath,
				Diagnostics: diags,
			})
		}
	}

	// Sort results by path for consistent output
	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})

	return results
}

func filterBySeverity(results []lintFileResult, severityThreshold string) []lintFileResult {
	threshold, ok := lint.ParseSeverity(severityThreshold)
	if !ok {
		threshold = lint.SeverityWarning
	}

	var filtered []lintFileResult
	for _, r := range results {
		var diags []lint.Diagnostic
		for _, d := range r.Diagnostics {
			if d.Severity <= threshold {
				diags = append(diags, d)
			}
		}
		if len(diags) > 0 {
			filtered = append(filtered, lintFileResult{
				Path:        r.Path,
				Diagnostics: diags,
			})
		}
	}
	return filtered
}

func renderLintResults(r *output.Renderer, results []lintFileResult) bool {
	if len(results) == 0 {
		r.Success("No lint issues found")
		return false
	}

	mode := r.EffectiveMode()

	// Calculate summary stats
	summary := output.LintSummary{
		FilesAnalyzed: len(results),
	}
	for _, res := range results {
		summary.TotalIssues += len(res.Diagnostics)
		for _, d := range res.Diagnostics {
			switch d.Severity {
			case lint.SeverityError:
				summary.Errors++
			case lint.SeverityWarning:
				summary.Warnings++
			case lint.SeverityInfo:
				summary.Info++
			case lint.SeverityHint:
				summary.Hints++
			}
		}
	}

	if mode == output.ModeJSON {
		// JSON output
		jsonOutput := output.LintOutput{
			Summary: summary,
		}
		for _, res := range results {
			fileResult := output.LintFileResult{
				Path: res.Path,
			}
			for _, d := range res.Diagnostics {
				fileResult.Diagnostics = append(fileResult.Diagnostics, output.LintDiagnostic{
					RuleID:   d.RuleID,
					Severity: d.Severity.String(),
					Message:  d.Message,
					Line:     d.Pos.Line,
					Column:   d.Pos.Column,
				})
			}
			jsonOutput.Files = append(jsonOutput.Files, fileResult)
		}
		_ = r.JSON(jsonOutput)
		return true
	}

	// Text/Markdown output
	for _, res := range results {
		r.Println(r.Styles().ModelPath.Render(res.Path))
		for _, d := range res.Diagnostics {
			loc := fmt.Sprintf("%d:%d", d.Pos.Line, d.Pos.Column)
			if d.Pos.Line == 0 {
				loc = "-"
			}
			sevStyle := severityStyle(r, d.Severity)
			r.Printf("  %s  %s  %s  %s\n",
				r.Styles().Muted.Render(fmt.Sprintf("%-5s", loc)),
				sevStyle,
				r.Styles().Bold.Render(d.RuleID),
				d.Message,
			)
		}
		r.Println("")
	}

	// Print summary
	summaryParts := []string{fmt.Sprintf("%d issues", summary.TotalIssues)}
	if summary.Errors > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("%d errors", summary.Errors))
	}
	if summary.Warnings > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("%d warnings", summary.Warnings))
	}
	if summary.Info > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("%d info", summary.Info))
	}
	if summary.Hints > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("%d hints", summary.Hints))
	}
	r.Printf("Summary: %s in %d files\n", strings.Join(summaryParts, ", "), summary.FilesAnalyzed)

	return true
}

func severityStyle(r *output.Renderer, sev lint.Severity) string {
	switch sev {
	case lint.SeverityError:
		return r.Styles().Error.Render("error  ")
	case lint.SeverityWarning:
		return r.Styles().Warning.Render("warning")
	case lint.SeverityInfo:
		return r.Styles().Info.Render("info   ")
	case lint.SeverityHint:
		return r.Styles().Muted.Render("hint   ")
	default:
		return r.Styles().Muted.Render("unknown")
	}
}
