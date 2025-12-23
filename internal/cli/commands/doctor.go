package commands

import (
	"fmt"
	"sort"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/leapstack-labs/leapsql/internal/cli/output"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/project/rules" // register project rules
	"github.com/spf13/cobra"
)

// DoctorOptions holds options for the doctor command.
type DoctorOptions struct {
	Format string // Output format: text, json
}

// NewDoctorCommand creates the doctor command.
func NewDoctorCommand() *cobra.Command {
	opts := &DoctorOptions{}
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run a comprehensive project health check",
		Long: `Analyze your LeapSQL project for potential issues and best practices.

The doctor command runs all project health rules and provides a comprehensive
report including:
- Project summary (models, macros, seeds, DAG structure)
- Health checks grouped by category (Modeling, Structure, Lineage)
- Health score (0-100)
- Actionable recommendations

Output adapts to environment:
  - Terminal: Styled output with colors
  - Piped/Scripted: Markdown format
  - JSON: Machine-readable format`,
		Example: `  # Run health check
  leapsql doctor

  # Output as JSON
  leapsql doctor --format json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDoctor(cmd, opts)
		},
	}

	cmd.Flags().StringVarP(&opts.Format, "format", "f", "", "Output format: text, json")

	return cmd
}

// DoctorOutput is the JSON output for the doctor command.
type DoctorOutput struct {
	Summary         ProjectSummary `json:"summary"`
	HealthChecks    []HealthCheck  `json:"health_checks"`
	Score           int            `json:"score"`
	Recommendations []string       `json:"recommendations"`
	IssueCount      int            `json:"issue_count"`
}

// ProjectSummary contains project-level statistics.
type ProjectSummary struct {
	Models    int `json:"models"`
	Macros    int `json:"macros"`
	Seeds     int `json:"seeds"`
	DAGDepth  int `json:"dag_depth"`
	RootCount int `json:"root_count"`
	LeafCount int `json:"leaf_count"`
	EdgeCount int `json:"edge_count"`
}

// HealthCheck represents a single health check result.
type HealthCheck struct {
	RuleID     string   `json:"rule_id"`
	Name       string   `json:"name"`
	Group      string   `json:"group"`
	Status     string   `json:"status"` // "pass", "warn", "error"
	IssueCount int      `json:"issue_count"`
	Details    []string `json:"details,omitempty"`
}

func runDoctor(cmd *cobra.Command, opts *DoctorOptions) error {
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

	// Build project context
	projectCtx := buildProjectContext(eng, cfg)
	if projectCtx == nil {
		r.Warning("No models found in project")
		return nil
	}

	// Run project health analysis
	analyzerCfg := project.NewAnalyzerConfig()
	analyzer := project.NewAnalyzer(analyzerCfg)
	diags := analyzer.Analyze(projectCtx)

	// Build output
	doctorOutput := buildDoctorOutput(eng, projectCtx, diags)

	// Render based on mode
	effectiveMode := r.EffectiveMode()
	switch effectiveMode {
	case output.ModeJSON:
		return r.JSON(doctorOutput)
	case output.ModeMarkdown:
		return renderDoctorMarkdown(r, doctorOutput)
	default:
		return renderDoctorText(r, doctorOutput)
	}
}

func buildDoctorOutput(eng *engine.Engine, _ *project.Context, diags []project.Diagnostic) *DoctorOutput {
	// Build summary
	summary := buildProjectSummary(eng)

	// Group diagnostics by rule
	diagsByRule := make(map[string][]project.Diagnostic)
	for _, d := range diags {
		diagsByRule[d.RuleID] = append(diagsByRule[d.RuleID], d)
	}

	// Build health checks from all registered rules
	rules := project.GetAll()
	healthChecks := make([]HealthCheck, 0, len(rules))

	for _, rule := range rules {
		ruleDiags := diagsByRule[rule.ID]
		status := "pass"
		if len(ruleDiags) > 0 {
			if rule.Severity == lint.SeverityError {
				status = "error"
			} else {
				status = "warn"
			}
		}

		details := make([]string, 0, len(ruleDiags))
		for _, d := range ruleDiags {
			details = append(details, d.Message)
		}

		healthChecks = append(healthChecks, HealthCheck{
			RuleID:     rule.ID,
			Name:       rule.Name,
			Group:      rule.Group,
			Status:     status,
			IssueCount: len(ruleDiags),
			Details:    details,
		})
	}

	// Sort health checks by group then by rule ID
	sort.Slice(healthChecks, func(i, j int) bool {
		if healthChecks[i].Group != healthChecks[j].Group {
			return healthChecks[i].Group < healthChecks[j].Group
		}
		return healthChecks[i].RuleID < healthChecks[j].RuleID
	})

	// Calculate score
	score := calculateHealthScore(healthChecks, summary.Models)

	// Generate recommendations
	recommendations := generateRecommendations(healthChecks, diags)

	return &DoctorOutput{
		Summary:         summary,
		HealthChecks:    healthChecks,
		Score:           score,
		Recommendations: recommendations,
		IssueCount:      len(diags),
	}
}

func buildProjectSummary(eng *engine.Engine) ProjectSummary {
	models := eng.GetModels()
	graph := eng.GetGraph()

	summary := ProjectSummary{
		Models: len(models),
	}

	// Count macros (from engine if available)
	macroCount := 0
	seeds := 0

	// Count roots and leaves from graph
	if graph != nil {
		summary.EdgeCount = graph.EdgeCount()

		levels, err := graph.GetExecutionLevels()
		if err == nil {
			summary.DAGDepth = len(levels)
			if len(levels) > 0 {
				summary.RootCount = len(levels[0])
			}
			if len(levels) > 0 {
				summary.LeafCount = len(levels[len(levels)-1])
			}
		}
	}

	summary.Macros = macroCount
	summary.Seeds = seeds

	return summary
}

// calculateHealthScore computes a health score from 0-100.
// The scoring weights:
// - Each passing rule adds points
// - Each issue reduces points
// - More models means issues have less individual impact
func calculateHealthScore(checks []HealthCheck, modelCount int) int {
	if len(checks) == 0 {
		return 100
	}

	// Base score starts at 100
	score := 100.0

	// Calculate penalty per issue
	// With more models, each individual issue has less impact
	basePenalty := 5.0
	if modelCount > 10 {
		basePenalty = 3.0
	}
	if modelCount > 50 {
		basePenalty = 2.0
	}
	if modelCount > 100 {
		basePenalty = 1.0
	}

	for _, check := range checks {
		switch check.Status {
		case "error":
			score -= float64(check.IssueCount) * basePenalty * 2 // Errors count double
		case "warn":
			score -= float64(check.IssueCount) * basePenalty
		}
	}

	// Clamp to 0-100
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	return int(score)
}

// generateRecommendations creates actionable recommendations based on findings.
func generateRecommendations(checks []HealthCheck, _ []project.Diagnostic) []string {
	var recommendations []string
	seen := make(map[string]bool)

	for _, check := range checks {
		if check.IssueCount == 0 {
			continue
		}

		rec := getRecommendation(check.RuleID, check.IssueCount)
		if rec != "" && !seen[rec] {
			recommendations = append(recommendations, rec)
			seen[rec] = true
		}
	}

	// Limit to top 5 recommendations
	if len(recommendations) > 5 {
		recommendations = recommendations[:5]
	}

	return recommendations
}

// getRecommendation returns a recommendation for a specific rule.
func getRecommendation(ruleID string, _ int) string {
	switch ruleID {
	case "PM01":
		return "Fix root models by adding proper source dependencies"
	case "PM02":
		return "Create staging models to abstract sources with high fanout"
	case "PM03":
		return "Refactor staging models to not depend on other staging models"
	case "PM04":
		return "Consider creating intermediate models to reduce model fanout"
	case "PM05":
		return "Split models with too many joins into smaller, focused models"
	case "PM06":
		return "Add staging layers between raw sources and downstream models"
	case "PM07":
		return "Remove unnecessary intermediate models that only pass through data"
	case "PS01":
		return "Rename models to follow naming conventions (stg_, int_, fct_, dim_)"
	case "PS02":
		return "Move models to appropriate directories matching their type"
	case "PL01":
		return "Replace SELECT * with explicit column selection"
	case "PL02":
		return "Remove orphaned columns that are never used downstream"
	case "PL04":
		return "Add explicit join conditions to prevent Cartesian products"
	default:
		return ""
	}
}

func renderDoctorText(r *output.Renderer, out *DoctorOutput) error {
	styles := r.Styles()

	// Header
	r.Println("")
	r.Println(styles.Header1.Render("LeapSQL Project Health Report"))
	r.Println(styles.Muted.Render(strings.Repeat("=", 55)))
	r.Println("")

	// Project Summary
	r.Println(styles.Header2.Render("Project Summary"))
	r.Printf("   Models: %d | Macros: %d | Seeds: %d\n", out.Summary.Models, out.Summary.Macros, out.Summary.Seeds)
	r.Printf("   DAG Depth: %d levels | Roots: %d | Leaves: %d\n", out.Summary.DAGDepth, out.Summary.RootCount, out.Summary.LeafCount)
	r.Println("")

	// Health Checks grouped by category
	r.Println(styles.Header2.Render("Health Checks"))
	r.Println("")

	currentGroup := ""
	titleCaser := cases.Title(language.English)
	for _, check := range out.HealthChecks {
		if check.Group != currentGroup {
			currentGroup = check.Group
			r.Println(styles.Bold.Render("   " + titleCaser.String(currentGroup)))
			r.Println(styles.Muted.Render("   " + strings.Repeat("-", 40)))
		}

		icon := styles.StatusSuccess.String()
		switch check.Status {
		case "warn":
			icon = styles.Warning.Render("!")
		case "error":
			icon = styles.StatusFailed.String()
		}

		status := fmt.Sprintf("%s %s: %s", icon, check.RuleID, check.Name)
		if check.IssueCount > 0 {
			status += fmt.Sprintf(" (%d issues)", check.IssueCount)
		}
		r.Println("   " + status)

		// Show first 3 details for issues
		for i, detail := range check.Details {
			if i >= 3 {
				r.Println(styles.Muted.Render(fmt.Sprintf("       ... and %d more", len(check.Details)-3)))
				break
			}
			r.Println(styles.Muted.Render("       - " + detail))
		}
	}
	r.Println("")

	// Health Score
	r.Println(styles.Muted.Render(strings.Repeat("=", 55)))
	scoreStyle := styles.Success
	if out.Score < 70 {
		scoreStyle = styles.Warning
	}
	if out.Score < 50 {
		scoreStyle = styles.Error
	}
	r.Printf("   Health Score: %s\n", scoreStyle.Render(fmt.Sprintf("%d/100", out.Score)))
	r.Println("")

	// Recommendations
	if len(out.Recommendations) > 0 {
		r.Println(styles.Header2.Render("Recommendations"))
		for i, rec := range out.Recommendations {
			r.Printf("   %d. %s\n", i+1, rec)
		}
		r.Println("")
	}

	return nil
}

func renderDoctorMarkdown(r *output.Renderer, out *DoctorOutput) error {
	r.Println("# LeapSQL Project Health Report")
	r.Println("")

	// Project Summary
	r.Println("## Project Summary")
	r.Println("")
	r.Printf("- **Models**: %d\n", out.Summary.Models)
	r.Printf("- **Macros**: %d\n", out.Summary.Macros)
	r.Printf("- **Seeds**: %d\n", out.Summary.Seeds)
	r.Printf("- **DAG Depth**: %d levels\n", out.Summary.DAGDepth)
	r.Printf("- **Root Models**: %d\n", out.Summary.RootCount)
	r.Printf("- **Leaf Models**: %d\n", out.Summary.LeafCount)
	r.Println("")

	// Health Checks
	r.Println("## Health Checks")
	r.Println("")

	currentGroup := ""
	titleCaser := cases.Title(language.English)
	for _, check := range out.HealthChecks {
		if check.Group != currentGroup {
			currentGroup = check.Group
			r.Println("### " + titleCaser.String(currentGroup))
			r.Println("")
		}

		status := "PASS"
		switch check.Status {
		case "warn":
			status = "WARN"
		case "error":
			status = "ERROR"
		}

		r.Printf("- **[%s]** %s: %s", status, check.RuleID, check.Name)
		if check.IssueCount > 0 {
			r.Printf(" (%d issues)", check.IssueCount)
		}
		r.Println("")

		for _, detail := range check.Details {
			r.Printf("  - %s\n", detail)
		}
	}
	r.Println("")

	// Health Score
	r.Println("## Health Score")
	r.Println("")
	r.Printf("**%d/100**\n", out.Score)
	r.Println("")

	// Recommendations
	if len(out.Recommendations) > 0 {
		r.Println("## Recommendations")
		r.Println("")
		for i, rec := range out.Recommendations {
			r.Printf("%d. %s\n", i+1, rec)
		}
		r.Println("")
	}

	return nil
}
